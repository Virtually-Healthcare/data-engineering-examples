from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
import requests
import json
import uuid
import copy

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

host = "192.168.1.59"

#host="192.168.1.94"

cdrFHIRUrl = "http://"+host+":8180/CDR/FHIR/R4"
emisFHIRUrl = "http://"+host+":8180/EMIS/FHIR/R4"
esbFHIRUrl = "http://"+host+":8181/ESB/R4"

with DAG(
        'Consultation_Note_Trigger_Tasks',
        schedule=timedelta(minutes=1),
        catchup=False,
        description='Consultation Note Writeback',
        start_date=datetime(2022, 1, 1)
) as Parent_dag:

    @task
    def done():
        print("Done")

    def get_tasks():
        headersCDR = { "Accept": "application/fhir+json"}
        parameters = {'_sort' : '-authored-on',
                      'authored-on': 'gt2023-01-01',
                      'status': 'accepted'}

        tasks = []
        print('get tasks run')
        try:
            response = requests.get(cdrFHIRUrl + '/Task',parameters,headers=headersCDR)
            if response.status_code == 200:
                tasksJSON = json.loads(response.text)
                if 'entry' in tasksJSON:
                    for entry in tasksJSON['entry']:
                        if 'resource' in entry:
                            tasks.append(entry['resource'])
            print("Number of Tasks = {}".format(len(tasks)))
        except:
            print("CONNECTION ISSUE")
        return tasks

    _tasks = get_tasks()

    for _task in _tasks:

        id = _task['id']
        print("Trigger task id = "+ id)
        _trigger_send_task_dag = TriggerDagRunOperator(
            #  task_id=f"trigger_consultation_task",
            task_id=f"trigger_consultation_task_{id}",
            trigger_dag_id="Consultation_Note_Task",
            conf={"_task": _task},
            dag=Parent_dag
        )
    _done = done()

    #_trigger_send_task_dag >> _done

with DAG(
        'Consultation_Note_Task',
        schedule=None,
        description='Consultation Note Writeback',
        start_date=datetime(2022, 1, 1)
) as dag2:

    options = ["EMIS", "TPP", "GPConnect_SendDocument"]

    @task(task_id="Task_accepted")
    def Task_accepted(**context):
        print(context["params"]["_task"])
        task = context["params"]["_task"]
        print("Started Task id = "+task['id'])
        return task

    @task(task_id="Task_completed", trigger_rule="all_success")
    def Task_completed(_task):
        _task['status'] = 'completed'
        headersCDR = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}
        response = requests.put(cdrFHIRUrl + '/Task/'+_task['id'],json.dumps(_task),headers=headersCDR)
        print(response.text)
        return "end"

    @task(task_id="Task_in-progress")
    def Task_in_progress(_task):

        _task['status'] = 'in-progress'
        headersCDR = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}
        response = requests.put(cdrFHIRUrl + '/Task/'+_task['id'],json.dumps(_task),headers=headersCDR)
        print(response.text)
        return "end"

    @task(task_id="Task_failed")
    def Task_failed(_task):
        _task['status'] = 'failed'
        headersCDR = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}
        response = requests.put(cdrFHIRUrl + '/Task/'+_task['id'],json.dumps(_task),headers=headersCDR)
        print(response.text)
        raise ValueError('Task Failed - Data issue detected with Consultation Note')
        return "error"


    def Task_cancelled(context):
        print("Task cancelled")
        print(context)
        _task['status'] = 'cancelled'
        headersCDR = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}
        response = requests.put(cdrFHIRUrl + '/Task/'+_task['id'],json.dumps(_task),headers=headersCDR)
        print(response.text)
        raise ValueError('Task Failed - Technical Issue')
        return "Task Cancelled"

    @task(task_id="Done_Primary_Care_Send", trigger_rule="one_success")
    def Done_Primary_Care_Send():
        return "One GPSend success"

    @task(task_id="get_consultation")
    def get_consultation(_task):
        headersCDR = { "Accept": "application/fhir+json"}
        encounter = _task['focus']['identifier']
        parameters = {'identifier' : encounter['system'] + '|' + encounter['value']}

        responseCDR = requests.get(cdrFHIRUrl + '/Encounter/$extract-collection',parameters,headers=headersCDR)
        if responseCDR.status_code == 200:
            print("======= Response from extract collection ========")
            print(responseCDR.text)

        resource = json.loads(responseCDR.text)
        for entry in resource.get('entry', []):
            if 'resource' in entry:
                if 'resourceType' in entry['resource']:
                    resourceType = entry['resource']['resourceType']
                    if resourceType == 'QuestionnaireResponse':
                        entry['resource'] = LegacyQuestionnaireResponseConversion(entry['resource'])
        return {
            "response": json.dumps(resource),
            "task": _task
        }

    @task.branch(task_id="get_Primary_Care_Endpoint",retries=0)
    def get_Primary_Care_Endpoint(record):
        return "EMIS"

    @task.branch(task_id="check_consultation_not_already_present_in_EMIS", on_failure_callback = [Task_cancelled])
    def check_consultation_not_already_present_in_EMIS(record):
        headersEMIS = {"Accept": "application/fhir+json",
                       "ODS_CODE": "F83004"}
        resource = json.loads(record['response'])
        task = record['task']
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
                    print('Task '+task['id']+ ' refers to a consultation already on EMIS')
                    if 'encounter' in entry['resource'] and 'identifier' in entry['resource']['encounter'] and entry['resource']['encounter']['identifier']['value'] == encounterId :
                        return "DUPLICATE"
        return "NOT_DUPLICATE"


    def LegacyQuestionnaireResponseConversion(questionnaireResponse):
        questionnaireResponse['questionnaire'] = "https://fhir.virtually.healthcare/Questionnaire/ClinicalManagementPlan"
        newQR = copy.deepcopy(questionnaireResponse)
        newQR['item'] = [{
            "linkId": "LOINC/61149-1",
            "text": "Comments and advice",
            "item": []
        }]
        # missing linkId - assume it's the problem section
        problems = {
            "linkId": "LOINC/11450-4",
            "text": "Problem list",
            "item": []
        }
        problemsFound = False
        for item in questionnaireResponse.get('item', []):
            # exiting fat entries are plain questions and answers
            if 'answer' in item:
                newQR['item'][0]['item'].append({
                    "linkId" : "questions",
                    "item": [{
                        "linkId": "question",
                        "answer": [ {
                            "valueString": item['text']
                        }]
                    },
                        {
                            "linkId": "answer",
                            "answer": [ {
                                "valueString": item['answer'][0]["valueString"]
                            }]
                        }]
                })
            # problem management comes as a set of subitems.
            if 'item' in item:
                problem = {
                    "linkId": "problem",
                    "text": "Problem",
                    "item": []
                }
                problemFound = False
                for problemitem in item.get('item', []):
                    if 'linkId' not in problemitem:
                        newitem = {
                            'linkId' : 'problemCode',
                            'text' : "Problem Code",
                            'answer' : problemitem['answer']
                        }
                        problem['item'].append(newitem)
                    if 'item' in problemitem:
                        for subitem in problemitem.get('item', []):
                            if subitem['linkId'] == 'problemStatus' or subitem['linkId'] == 'problemSignificance' or subitem['linkId'] == 'problemType' or subitem['linkId'] == 'problemExpectedDuration':
                                problem['item'].append(subitem)
                                problemFound = True
                if problemFound:
                    problems['item'].append(problem)
                    problemsFound = True

        if problemsFound:
            newQR['item'].append(problems)
        #print(json.dumps(newQR))
        return newQR

    @task.branch(task_id="perform_FHIR_Validation",
                 retries=3)
    def perform_FHIR_Validation(record):
        headersESB = {"Content-Type": "application/fhir+json",
                      "ODS_CODE": "F83004"}
        # begin fix the bundle
        resource = json.loads(record['response'])
        id = 0

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
        if responseValidate.status_code != 200:
            raise ValueError('Task Failed - FHIR Validation Fatal Issue')
        if failed:
            print("FAILED Validation")
            return "FAIL"
        return "PASS"

    @task(task_id="convert_to_EMISOpen",retries=3, on_failure_callback = [Task_cancelled])
    def convert_to_EMISOpen(record):
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

    @task(task_id="send_to_EMIS", retries = 3, on_failure_callback = [Task_cancelled])
    def send_to_EMIS(EMISOpen):
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

    @task(task_id="convert_to_FHIR_Document",retries=3)
    def convert_to_FHIR_Document(_collection):
        return "TODO: convert to FHIR Document"

    @task(task_id="transform_to_PDF",retries=3)
    def transform_to_PDF(_FHIRDocument):
        return "TODO: convert to PDF"

    @task(task_id="send_to_MESH",retries=3)
    def send_to_MESH(_PDF):
        return "TODO: send to MESH"

    @task(task_id="get_PDS_Patient",retries=3)
    def get_PDS_Patient(_task):
        return "TODO: Get PDS Patient"

    @task(task_id="NHS_Trust_FUTURE_TODO",retries=3)
    def NHS_Trust_FUTURE():
        return "TODO: NHS Trust - Future"

    @task(task_id="convert_to_HL7_v2_ADT_A04_FUTURE_TODO",retries=3, on_failure_callback = [Task_cancelled])
    def convert_to_HL7_v2_ADT_A04(record):
        return "TODO: Convert to HL7 ADT A04"


    @task(task_id="convert_to_HL7_FHIR_Message_A04",retries=3, on_failure_callback = [Task_cancelled])
    def convert_to_HL7_FHIR_Message_A04(record):
        resource = json.loads(record['response'])
        resource["type"] = "message"
        myuuid = uuid.uuid4()
        resource["identifier"] = {
            "system": "urn:ietf:rfc:3986",
            "value": 'urn:uuid:' + str(myuuid)
        }
        messageHeader = {
            "resourceType" : "MessageHeader",
            "eventCoding" : {
                "system" : "http://terminology.hl7.org/CodeSystem/v2-0003",
                "code" : "A04"
            },
            "destination" : [
                {
                    "endpoint" : "http://ec2-18-130-139-120.eu-west-2.compute.amazonaws.com/emis",
                    "receiver" : {
                        "identifier" : {
                            "system" : "https://fhir.nhs.uk/Id/ods-organization-code",
                            "value" : "F83004"
                        }
                    }
                }
            ],
            "sender" : {
                "identifier" : {
                    "system" : "https://fhir.nhs.uk/Id/ods-organization-code",
                    "value" : "F83004"
                }
            },
            "source" : {
                "endpoint" : "http://ec2-18-130-139-120.eu-west-2.compute.amazonaws.com/emis"
            },
            "focus" : [
            ]
        }
        for entry in resource.get('entry', []):
            if 'resource' in entry:
                if 'resourceType' in entry['resource']:
                    resourceType = entry['resource']['resourceType']
                    if resourceType == 'Encounter':
                        print("Encounter")
                        messageHeader['focus'].append({
                            "reference" : entry['fullUrl'],
                            "type": "Encounter"
                        })
        resource["entry"].insert(0,{
            "fullUrl": "urn:uuid:" + str(myuuid),
            "resource": messageHeader
        })
        print(json.dumps(resource))
        return {
            "response" : json.dumps(resource),
            "task": record['task']
        }

    @task(task_id="send_to_Trust_Integration_Engine_FUTURE_TODO",retries=3)
    def send_to_Trust_Integration_Engine(_message):
        return "TODO: Send to Trust Integration Engine"



    _task = Task_accepted()
    _inprogress = Task_in_progress(_task)
    _success= Task_completed(_task)
    _error = Task_failed(_task)
    _FHIRcollection = get_consultation(_task)
    _duplicate = check_consultation_not_already_present_in_EMIS(_FHIRcollection)
    _FHIRmessage = convert_to_HL7_FHIR_Message_A04(_FHIRcollection)
    _valid = perform_FHIR_Validation(_FHIRmessage)
    _EMISOpen = convert_to_EMISOpen(_FHIRcollection)
    _sendResponse = send_to_EMIS(_EMISOpen)
    _endpoint = get_Primary_Care_Endpoint(_FHIRcollection)
    _pdsPatient = get_PDS_Patient(_task)
    _FHIRDocument = convert_to_FHIR_Document(_FHIRcollection)
    _pdf = transform_to_PDF(_FHIRDocument)
    _sendMESH = send_to_MESH(_pdf)

    _NHSTrust = NHS_Trust_FUTURE()
    _v2message = convert_to_HL7_v2_ADT_A04(_FHIRcollection)

    _sendTrust = send_to_Trust_Integration_Engine(_v2message)
    _doneGPSend = Done_Primary_Care_Send()
    #_cancelled = Task_cancelled(_task)

    EMIS_op = EmptyOperator(task_id="EMIS", dag=dag2)
    TPP_op = EmptyOperator(task_id="TPP", dag=dag2)
    GPConnect_op = EmptyOperator(task_id="GPConnect_SendDocument", dag=dag2)
    PASS_op = EmptyOperator(task_id="PASS", dag=dag2)
    FAIL_op = EmptyOperator(task_id="FAIL", dag=dag2)
    DUPLICATE_op = EmptyOperator(task_id="DUPLICATE", dag=dag2)
    NOT_DUPLICATE_op = EmptyOperator(task_id="NOT_DUPLICATE", dag=dag2)


    _task >> _inprogress >> _FHIRcollection >> _FHIRmessage >> _valid >> [PASS_op, FAIL_op]

    PASS_op >> _pdsPatient >> [_endpoint, _NHSTrust]
    FAIL_op >> _error

    _endpoint >>  [TPP_op, GPConnect_op, EMIS_op]
    EMIS_op >> _duplicate >> [DUPLICATE_op, NOT_DUPLICATE_op]
    NOT_DUPLICATE_op >> _EMISOpen >> _sendResponse

    GPConnect_op >> _FHIRDocument >> _pdf >> _sendMESH

    _NHSTrust >> _v2message >> _sendTrust

    [_sendResponse, _sendMESH, TPP_op, DUPLICATE_op] >> _doneGPSend >> _success
