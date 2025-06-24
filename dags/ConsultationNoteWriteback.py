from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
import requests
import json

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

#host = "192.168.1.59"
host="192.168.1.94"

cdrFHIRUrl = "http://"+host+":8180/CDR/FHIR/R4"
emisFHIRUrl = "http://"+host+":8180/EMIS/FHIR/R4"
esbFHIRUrl = "http://"+host+":8181/ESB/R4"

with DAG(
        'Consultation_Note_Trigger_Tasks',
        schedule=None, catchup=False,
        description='Consultation Note Writeback',
        start_date=datetime(2022, 1, 1)
) as dag1:



    def get_tasks():
        headersCDR = { "Accept": "application/fhir+json"}
        parameters = {'_sort' : '-authored-on',
                      'authored-on': 'gt2023-01-01',
                      'status': 'failed'}

        tasks = []
        response = requests.get(cdrFHIRUrl + '/Task',parameters,headers=headersCDR)
        if response.status_code == 200:
            tasksJSON = json.loads(response.text)
            if 'entry' in tasksJSON:
                for entry in tasksJSON['entry']:
                    if 'resource' in entry:
                        tasks.append(entry['resource'])
        return tasks

    tasks = get_tasks()
    id = 0
    for _task in tasks:
        id += 1
        print("Trigger task id = "+_task['id'])
        trigger_send_task_dag = TriggerDagRunOperator(
          #  task_id=f"trigger_consultation_task",
            task_id=f"trigger_consultation_task_{id}",
            trigger_dag_id="Consultation_Note_Task",
            conf={"_task": _task},
            dag=dag1
        )


with (DAG(
        'Consultation_Note_Task',
        schedule=None,
        description='Consultation Note Writeback',
        start_date=datetime(2022, 1, 1)
) as dag2):

    options = ["EMIS", "TPP", "GPConnect"]

    @task(task_id="start")
    def start(**context):
        print(context["params"]["_task"])
        task = context["params"]["_task"]
        print("Started Task id = "+task['id'])
        return task

    @task(task_id="sucess")
    def sucess(_task):
        return "end"

    @task(task_id="error")
    def error(_task):
        return "error"

    @task(task_id="get_consultation")
    def get_consultation(_task):
        headersCDR = { "Accept": "application/fhir+json"}
        encounter = _task['focus']['identifier']
        parameters = {'identifier' : encounter['system'] + '|' + encounter['value']}

        responseCDR = requests.get(cdrFHIRUrl + '/Encounter/$extract-collection',parameters,headers=headersCDR)
        if responseCDR.status_code == 200:
            print("======= Response from extract collection ========")
            print(responseCDR.text)
        return {
            "response": responseCDR.text,
            "task": _task
        }

    @task.branch(task_id="get_destination_endpoint",retries=0)
    def get_destination_endpoint(record):
        return "EMIS"

    @task(task_id="check_consultation_not_already_present_in_EMIS")
    def check_consultation_not_already_present_in_EMIS(record):
        headersEMIS = {"Accept": "application/fhir+json",
                       "ODS_CODE": "F83004"}
        resource = json.loads(record['response'])
        id = 0
        patientId = ''
        encounterId = ''
        for entry in resource.get('entry', []):
            id += 1
            if 'resource' in entry:
                if 'resourceType' in entry['resource']:
                    resourceType = entry['resource']['resourceType']
                    if resourceType == 'Patient':
                        if 'identifier' in entry['resource']:
                            for identifier in entry['resource'].get('identifier', []):
                                if 'system' in identifier and identifier['system'] == 'https://emis.com/Id/Patient/DBID':
                                    patientId = identifier['value']
                        print(patientId)
                    if resourceType == 'Encounter':
                        if 'identifier' in entry['resource']:
                            for identifier in entry['resource'].get('identifier', []):
                                if 'system' in identifier and identifier['system'] == 'https://fhir.virtually.healthcare/Id/Encounter':
                                    encounterId = identifier['value']
                        print(encounterId)
        if (patientId != '' and encounterId != ''):
            url = emisFHIRUrl + f'/Composition?patient={patientId}'
            responseComposition = requests.get(url, headers=headersEMIS)
            print(responseComposition.text)
            resource = json.loads(responseComposition.text)
            for entry in resource.get('entry', []):
                if 'resourceType' in entry['resource'] and entry['resource']['resourceType'] == 'Composition':
                    print('Have composition')
                    if 'encounter' in entry['resource'] and 'identifier' in entry['resource']['encounter'] and entry['resource']['encounter']['identifier']['value'] == encounterId :
                        print('DUPLICATE SHOULD GRACEFULLY FINISH')
                        raise ValueError('Consultation Note already present for this encounter.')
        return record

    @task.branch(task_id="perform_FHIR_Validation",retries=3)
    def perform_FHIR_Validation(record):
        headersESB = {"Content-Type": "application/fhir+json",
                      "ODS_CODE": "F83004"}
        # begin fix the bundle
        resource = json.loads(record['response'])
        id = 0
        for entry in resource.get('entry', []):
            id += 1
            if 'resource' in entry:
                if 'resourceType' in entry['resource']:
                    resourceType = entry['resource']['resourceType']
                    if resourceType == 'QuestionnaireResponse':
                        # add in the questionnaire
                        if 'questionnaire' not in entry['resource']:
                            entry['resource']['questionnaire'] = "https://fhir.virtually.healthcare/Questionnaire/ClinicalManagementPlan"
                        # missing linkId - assume it's the problem section
                        for item in entry['resource'].get('item', []):
                            if 'linkId' not in item:
                                item['linkId'] = 'fat-14'
                            # move the nested items to correct level
                            for subitem in item.get('item', []):
                                if 'linkId' not in subitem:
                                    subitem['linkId'] = 'problem'
                                for nesteditem in subitem.get('item', []):
                                    item['item'].append(nesteditem)
                                subitem['item'] = []
                        #print(json.dumps(entry['resource']))
        # end of fix

        responseValidate = requests.post(esbFHIRUrl + '/$validate', json.dumps(resource), headers=headersESB)
        print("======= Response from FHIR Validation ========")
        #print(responseValidate.text)
        operationOutcome = json.loads(responseValidate.text)
        failed = False

        if 'issue' in operationOutcome:
            print("======= Error ========")
            for issue in operationOutcome['issue']:
                if issue['severity'] == 'error':
                    ignore = False
                    if 'details' in issue:
                        if '307321000000107' in issue['details']['text']:
                            ignore = True
                    if not ignore:
                        failed = True
                        if 'details' in issue:
                            print("Issue: " + issue['details']['text'] + ' [' + issue['severity'] + ']')
                        if 'diagnostics' in issue:
                            print("Issue: " + issue['diagnostics'] + ' [' + issue['severity'] + ']')
                        if 'expression' in issue:
                            print(issue['expression'])
                        print('')
            print("======= Warning ========")
            for issue in operationOutcome['issue']:
                if issue['severity'] == 'warning':
                    if 'details' in issue:
                        print("Issue: " + issue['details']['text'] + ' [' + issue['severity'] + ']')
                    if 'diagnostics' in issue:
                        print("Issue: " + issue['diagnostics'] + ' [' + issue['severity'] + ']')
                    if 'expression' in issue:
                        print(issue['expression'])
                    print('')

        if failed:
            print("FAILED Validation")
            return "FAIL"
            #record['task']['status'] = 'failed'
            #response = requests.put(cdrFHIRUrl + '/Task/' + task['id'],json.dumps(task),headers=headersCDR)
            #print(response.text)
            #break
        return "PASS"

    @task(task_id="convert_to_emisopen",retries=3)
    def convert_to_emisopen(record):
        headersEMIS = {"Content-Type": "application/fhir+json",
                       "ODS_CODE": "F83004"}
        responseEMISTransform = requests.post(emisFHIRUrl + '/Bundle/$transform-EMISOpen', record['response'], headers=headersEMIS )
        print("======= Response from transform to EMIS Open ========")
        print(responseEMISTransform.text)
        EMISOpenRecords = {
            "response" : responseEMISTransform.text,
            "task": record['task']
        }
        return EMISOpenRecords

    @task(task_id="send_to_emis")
    def send_to_emis(EMISOpen):
        headersEMIS = {"Content-Type": "application/fhir+json",
                       "ODS_CODE": "F83004"}
        body = {
            "resourceType": "Parameters",
            "parameter" : [ {
                "name" : "EMISOpen",
                "valueString": EMISOpen['response']  }]
        }

        responseEMISSend = requests.post(emisFHIRUrl + '/$send-EMISOpen', json.dumps(body), headers=headersEMIS )
        print("======= Send to EMIS Open ========")
        print(responseEMISSend.text)
        sendResponse = {
            "response" : responseEMISSend.text,
            "task": EMISOpen['task']
        }
        return sendResponse

    _task = start()
    _sucess= sucess(_task)
    _error = error(_task)
    _collection = get_consultation(_task)
    _duplicate = check_consultation_not_already_present_in_EMIS(_collection)
    _valid = perform_FHIR_Validation(_collection)
    _EMISOpen = convert_to_emisopen(_collection)
    _sendResponse = send_to_emis(_EMISOpen)
    _endpoint = get_destination_endpoint(_collection)

    EMIS_op = EmptyOperator(task_id="EMIS", dag=dag2)
    TPP_op = EmptyOperator(task_id="TPP", dag=dag2)
    GPConnect_op = EmptyOperator(task_id="GPConnect", dag=dag2)
    PASS_op = EmptyOperator(task_id="PASS", dag=dag2)
    FAIL_op = EmptyOperator(task_id="FAIL", dag=dag2)

    _task >> _collection >> _valid >> [PASS_op, FAIL_op]

    PASS_op >> _endpoint >> [TPP_op, GPConnect_op, EMIS_op]
    FAIL_op >> _error

    EMIS_op >> _duplicate >>  _EMISOpen >> _sendResponse >> _sucess


