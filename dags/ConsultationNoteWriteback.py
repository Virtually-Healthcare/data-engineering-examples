from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
import requests
import json

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



with DAG(
        'Consultation_Note_Generate_Tasks',
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
            task_id=f"trigger_consultation_task_{id}",
            trigger_dag_id="Consultation_Note_Task",
            conf={"_task": _task},
            dag=dag1
        )


with DAG(
        'Consultation_Note_Task',
        schedule=None,
        description='Consultation Note Writeback',
        start_date=datetime(2022, 1, 1)
) as dag2:


    @task(task_id="dag_start")
    def dag_start(**context):
        print(context["params"]["_task"])
        task = json.loads(context["params"]["_task"])
        print("Started Task id = "+task['id'])
        return task

    @task(task_id="dag_end")
    def dag_end():
        return "end"

    @task(task_id="get_collection")
    def get_collection(fhirtask):
        headersCDR = { "Accept": "application/fhir+json"}
        encounter = task['focus']['identifier']
        parameters = {'identifier' : encounter['system'] + '|' + encounter['value']}

        responseCDR = requests.get(cdrFHIRUrl + '/Encounter/$extract-collection',parameters,headers=headersCDR)
        if responseCDR.status_code == 200:
            print("======= Response from extract collection ========")
            print(responseCDR.text)
        return {
            "response": responseCDR.text,
            "task": task
        }

    _fhirtask = dag_start()
    #_dag_end = dag_end()
    _collection = get_collection(_fhirtask)

    _fhirtask >> _collection


