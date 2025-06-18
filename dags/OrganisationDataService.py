from __future__ import annotations

from pprint import pprint
from textwrap import dedent
from datetime import datetime, timedelta

import pendulum
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator
#from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task, dag

import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import iris


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


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)

def load_practitioner():


    @task(task_id="Load_ODS_Practioners")
    def load_ods_practitioners(ds=None, **kwargs):
        maxentries = 20000

        headers = {'User-Agent': 'Mozilla/5.0 (X11; Windows; Windows x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36'}
        odsurl = 'https://files.digital.nhs.uk/assets/ods/current/epraccur.zip'
        response = requests.get(odsurl, headers=headers, timeout=120)
        response.raise_for_status()  # Raise an exception for bad status codes

        myzip = ZipFile(BytesIO(response.content))

        with myzip.open('epraccur.csv') as csv_file:
            epraccur = pd.read_csv(
                csv_file,
                header=None,
                index_col=False,
                names=[
                    "ODS", "Organisation_Name", "NationalGrouping", 4,
                    "AddressLine_1", "AddressLine_2", "AddressLine_3",
                    "AddressLine_4", "AddressLine_5", "PostCode",
                    "Opened", "Closed", 13, 14, "PRAC_ODS",
                    16, 17, "PhoneNumber", 19, 20, 21, 22, 23, 24, 25, 26
                ]
            )
        epraccur = epraccur.set_index(['ODS'])

        return epraccur

    @task(task_id="Load_FHIR_Practioners")
    def load_FHIR_practitioners(ds=None, **kwargs):
        host = "localhost"
        # this is the superserver port
        port = 32782
        namespace = "FHIRSERVER"
        user = "_SYSTEM"
        password = "SYS"

        conn = iris.connect(
            hostname=host,
            port=port,
            namespace=namespace,
            username=user,
            password=password
        )

        # create a cursor
        cursor = conn.cursor()

        sql = """
              select org._id, org.Key, org.Identifier, org._lastUpdated, resource.ResourceString, null as ODS from HSFHIR_X0001_S.Organization org
                                                                                                                       join HSFHIR_X0001_R.Rsrc resource on resource.ID = org._id
              where IsNull(org.addressCountry,'') <> 'US' and org.type [ 'https://fhir.nhs.uk/CodeSystem/organisation-role|76'
              """

        cursor.execute(sql)
        data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=column_names)
        pd.set_option('future.no_silent_downcasting', True)
        return df

    @task(task_id="Merge_Practioners")
    def merge_practitioners(ds=None, **kwargs):
        return

    load_ods_practitioners = load_ods_practitioners()
    load_FHIR_practitioners = load_FHIR_practitioners()

    [load_ods_practitioners,load_FHIR_practitioners] >> merge_practitioners

# [END tutorial]
