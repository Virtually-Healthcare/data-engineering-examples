from __future__ import annotations

from datetime import datetime, timedelta

import pendulum
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!

from airflow.sdk import task, dag

import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import intersystems_iris.dbapi._DBAPI as dbapi
import re
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.practitioner import Practitioner
from fhir.resources.R4B.practitionerrole import PractitionerRole
import json

maxentries = 5000
host = "192.168.1.59"
# this is the superserver port
port = 32782
namespace = "FHIRSERVER"
user = "_SYSTEM"
password = "SYS"

headers = {"Content-Type": "application/fhir+json"}
fhirurl = "http://"+host+":32783/fhir/r4"

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



with DAG(
        'Organisation_Data_Service',
        default_args=default_args,
        description='A simple tutorial DAG',
        start_date=datetime(2022, 1, 1),
        tags=['FHIR','ODS'],
) as dag:

    @task(task_id="Extract_ODS_Organisations_GPPractice")
    def extract_ods_organisations(ds=None, **kwargs):


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



    @task(task_id="Extract_FHIR_Organisations_GPPractice")
    def extract_FHIR_organisations(ds=None, **kwargs):


        conn = dbapi.connect(
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
        for orgId in range(0, len(df)):
            identifier = df.loc[orgId,'identifier']
            identifiers = identifier.split(',')
            for id in identifiers:
                if (re.match('^https:.*ods-organization-code[|][A-Za-z0-9].*$',id)):
                    df.loc[orgId,'ODS'] = id.split('|')[1]

        return df



    @task(task_id="Merge_Organisations_GPPractice")
    def merge_organisations(epraccur, df):
        for orgId in range(0, len(df)):
            identifier = df.loc[orgId,'identifier']
            identifiers = identifier.split(',')
            for id in identifiers:
                if (re.match('^https:.*ods-organization-code[|][A-Za-z0-9].*$',id)):
                    df.loc[orgId,'ODS'] = id.split('|')[1]


        organisations = pd.merge(epraccur, df, how="left", on=["ODS"])
        organisations = organisations.set_index(['ODS'])

        organisations['_id'] = organisations['_id'].fillna(-1).astype(int)

        return organisations


    @task(task_id="Load_Organisations_GPPractice")
    def load_organisations(organisations):
        def convertOrganisationFHIR(org):
            organisationJSON = {
                "resourceType": "Organization",
                "identifier": [
                    {
                        "system": "https://fhir.nhs.uk/Id/ods-organization-code",
                        "value": org
                    }
                ],
                "active": True,
                "type": [
                    {
                        "coding": [
                            {
                                "system": "https://fhir.nhs.uk/CodeSystem/organisation-role",
                                "code": "76",
                                "display": "GP PRACTICE"
                            }
                        ]
                    }
                ],
                "name": organisations.loc[org,'Organisation_Name']
            }
            # if org is closed update active field
            #
            #
            if (organisations.loc[org,'PostCode'] != '' and not pd.isnull(organisations.loc[org,'PostCode'])):
                organisationJSON["address"]: [
                    {
                        "use": "work",
                        "postalCode": organisations.loc[org,'PostCode']
                    }
                ]
            if (organisations.loc[org,'NationalGrouping'] != '' and not pd.isnull(organisations.loc[org,'NationalGrouping'])):
                organisationJSON["partOf"] = {
                    "identifier": {
                        "system": "https://fhir.nhs.uk/Id/ods-organization-code",
                        "value": organisations.loc[org,'NationalGrouping']
                    }
                }
            if (organisations.loc[org,'PhoneNumber'] != '' and not pd.isnull(organisations.loc[org,'PhoneNumber'])):
                # print('-',organisations.loc[org,'PhoneNumber'].strip(),'-',"1")
                organisationJSON['telecom'] = [
                    {
                        "system": "phone",
                        "value": organisations.loc[org,'PhoneNumber'].strip(),
                        "use": "work"
                    }]
            if (organisations.loc[org,'PostCode'] != '' and not pd.isnull(organisations.loc[org,'PostCode'])):
                # print('-',organisations.loc[org,'PhoneNumber'].strip(),'-',"1")
                organisationJSON['address'] = [
                    {
                        "postalCode" : organisations.loc[org,'PostCode']
                    }]
                if (organisations.loc[org,'AddressLine_1'] != '' and not pd.isnull(organisations.loc[org,'AddressLine_1'])):
                    organisationJSON['address'][0]['line'] = []
                    organisationJSON['address'][0]['line'].append(organisations.loc[org,'AddressLine_1'])
                if (organisations.loc[org,'AddressLine_2'] != '' and not pd.isnull(organisations.loc[org,'AddressLine_2'])):
                    organisationJSON['address'][0]['line'].append(organisations.loc[org,'AddressLine_2'])
                if (organisations.loc[org,'AddressLine_3'] != '' and not pd.isnull(organisations.loc[org,'AddressLine_3'])):
                    organisationJSON['address'][0]['city'] = organisations.loc[org,'AddressLine_3']
                if (organisations.loc[org,'AddressLine_4'] != '' and not pd.isnull(organisations.loc[org,'AddressLine_4'])):
                    organisationJSON['address'][0]['district'] = organisations.loc[org,'AddressLine_4']
            if organisations.loc[org,'Closed'] != '' and not pd.isnull(organisations.loc[org,'Closed']) :
                organisationJSON['active'] = False
            if organisations.loc[org,'_id'] != -1:
                organisationJSON['id'] = str(organisations.loc[org,'_id'])
            #print(json.dumps(organisationJSON, indent=2, ensure_ascii=False))
            # validate organisation against schema
            Organization(**organisationJSON)
            return organisationJSON

        new = organisations[['_id']].head(maxentries).copy()
        for org in new.index:

            organisationJSON = convertOrganisationFHIR(org)
            #print(json.dumps(organisationJSON, indent=2, ensure_ascii=False))

            if (organisations.loc[org,'_id'] == -1):

                # Create

                r = requests.post(fhirurl+'/Organization', data=json.dumps(organisationJSON), headers=headers)

                if 'Location' in r.headers:
                    location = r.headers['Location'].split('Organization/')[1].split('/')[0]
                    organisations.loc[org,'_id'] = str(location)
                    #print('Created ' + org + ' id - '+location)
                else:
                    print("No Location header in response: ",r.status_code)
                    print("Response headers:", r.headers)
                    print(json.dumps(organisationJSON, indent=2, ensure_ascii=False))
                    print(r.text)
            else:
                # Update

                isUpdate = False
                if organisations.loc[org,'ResourceString'] != '':
                    organisationMDM = json.loads(organisations.loc[org,'ResourceString'])
                    #organisationMDM['name'] = organisations.loc[org,'Organisation_Name']
                    if ('telecom' in organisationMDM and
                            isinstance(organisationMDM['telecom'], list) and
                            len(organisationMDM['telecom']) > 0 and
                            organisationMDM['telecom'][0].get('value', '') == ''):
                        isUpdate = True
                        organisationMDM['telecom'] = organisationJSON['telecom']
                        print('telephone')
                    if ('address' in organisationMDM and
                            isinstance(organisationMDM['address'], list) and
                            len(organisationMDM['address']) > 0 and
                            organisationMDM['address'][0].get('postalCode', '') != organisations.loc[org,'PostCode']):
                        isUpdate = True
                        print('postcode')
                        organisationMDM['address'] = organisationJSON['address']
                    if ('address' not in organisationMDM) and not pd.isnull(organisations.loc[org,'PostCode']):
                        isUpdate = True
                        print('postcode ',organisations.loc[org,'PostCode'])
                        organisationMDM['address'] = organisationJSON['address']
                    if organisationMDM.get('partOf', {}).get('identifier', {}).get('value', '') != organisations.loc[org,'NationalGrouping'] and not pd.isnull(organisations.loc[org,'NationalGrouping']):
                        isUpdate = True
                        organisationMDM['partOf'] = organisationJSON['partOf']
                        print('partOf - ',organisations.loc[org,'NationalGrouping'])

                    if organisationMDM['active'] != organisationJSON['active']:
                        isUpdate = True
                        organisationMDM['active'] = organisationJSON['active']
                        print('active')

                    if isUpdate:
                        #print(json.dumps(organisationMDM, indent=2, ensure_ascii=False))
                        r = requests.put(fhirurl+'/Organization/'+organisationMDM['id'], data=json.dumps(organisationMDM), headers=headers)

                        if 'Location' in r.headers:
                            location = r.headers['Location'].split('Organization/')[1].split('/')[0]
                            organisations.loc[org,'ResourceString'] = json.dumps(organisationMDM)
                            #print('Created ' + org + ' id - '+location)
                        else:
                            print("No Location header in response: ",r.status_code)
                            print("Response headers:", r.headers)
                            print(json.dumps(organisationJSON, indent=2, ensure_ascii=False))
                            print(r.text)
                return organisations


    @task(task_id="Extract_ODS_Practitioners_GPPractice")
    def extract_ods_practitioners(ds=None, **kwargs):
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Windows; Windows x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36'}

        zipurl = 'https://files.digital.nhs.uk/assets/ods/current/egpcur.zip'
        response = requests.get(zipurl, headers=headers, timeout=120)
        response.raise_for_status()  # Raise an exception for bad status codes

        myzip = ZipFile(BytesIO(response.content))


        with myzip.open('egpcur.csv') as csv_file:
            egpcur = pd.read_csv(
                csv_file,
                header=None,
                index_col=False,
                names=[
                    "GMP","Practitioner_Name",3,4,"AddressLine_1","AddressLine_2","AddressLine_3","AddressLine_4","AddressLine_5","PostCode",10,11,"Status",13,"ODS","Started","Ended","PhoneNumber",18,19,20,21,22,23,24,25,26
                ]
            )
        def given(name):
            if isinstance(name, str):
                names = name.strip().split(' ')
                if len(names) <= 1:  # Handle single-word names
                    return ''
                return ' '.join(names[1:]).strip()
            return ''

        egpcur['Practitioner_Surname'] = egpcur['Practitioner_Name'].str.strip().str.split(' ', expand=True)[0]
        egpcur['Practitioner_Initials'] = egpcur['Practitioner_Name'].apply(given)

        egpcur = egpcur.set_index(['GMP'])
        return egpcur

    @task(task_id="Extract_FHIR_Practitioner_GPPractice")
    def extract_FHIR_practitioners(ds=None, **kwargs):

        conn = dbapi.connect(
            hostname=host,
            port=port,
            namespace=namespace,
            username=user,
            password=password
        )

        # create a cursor
        cursor = conn.cursor()

        sql = """
              select prac._id, prac.Key, prac.Identifier, prac._lastUpdated, resource.ResourceString, null as GMP from HSFHIR_X0001_S.Practitioner prac
                                                                                                                           join HSFHIR_X0001_R.Rsrc resource on  resource.ID = prac._id
              where IsNull(prac.addressCountry,'') <> 'US' \
              """

        cursor.execute(sql)
        data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        dfgp = pd.DataFrame(data, columns=column_names)

        return dfgp

    @task(task_id="Merge_Practitioners_GPPractice")
    def merge_practitioners(egpcur, dfgp):
        for pracId in range(0, len(dfgp)):
            identifier = dfgp.loc[pracId,'identifier']
            identifiers = identifier.split(',')
            for id in identifiers:
                if (re.match('^https:.*gmp-number[|]G[0-9].*$',id)):
                    #print(id)
                    dfgp.loc[pracId,'GMP'] = id.split('|')[1]


        practitioners = pd.merge(egpcur, dfgp, how="left", on=["GMP"])
        practitioners = practitioners.set_index(['GMP'])
        practitioners['_id'] = practitioners['_id'].fillna(-1).astype(int)
        return practitioners

    @task(task_id="Load_Practitioners_GPPractice")
    def load_practitioners(practitioners):
        def convertPractitionerFHIR(GMP):
            practitionerJSON = {
                "resourceType": "Practitioner",
                "identifier": [
                    {
                        "system": "https://fhir.hl7.org.uk/Id/gmp-number",
                        "value": GMP
                    }
                ],
                "active": True,
                "name": [
                    {
                        "family": practitioners.loc[GMP,'Practitioner_Surname'],
                        "prefix": [
                            "Dr"
                        ]
                    }
                ],
                "qualification": [
                    {
                        "code": {
                            "coding": [ {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0360",
                                "code": "MD",
                                "display": "Medical Doctor"
                            }
                            ]
                        }
                    }
                ]
            }
            if practitioners.loc[GMP,'Practitioner_Initials'] != '' :
                given = practitioners.loc[GMP,'Practitioner_Initials'].split(' ')
                practitionerJSON['name'][0]['given'] = []
                for id in range(0, len(given)):
                    practitionerJSON['name'][0]['given'].append(given[id])

            if practitioners.loc[GMP,'Status'] != 'C' and not pd.isnull(practitioners.loc[GMP,'Status']) :
                practitionerJSON['active'] = False

            Practitioner(**practitionerJSON)


            return practitionerJSON


        new = practitioners[['_id']].head(maxentries).copy()
        for GMP in new.index:

            practitionerJSON = convertPractitionerFHIR(GMP)
            if (practitioners.loc[GMP,'_id'] == -1):

                # Create

                r = requests.post(fhirurl+'/Practitioner', data=json.dumps(practitionerJSON), headers=headers)

                if 'Location' in r.headers:
                    location = r.headers['Location'].split('Practitioner/')[1].split('/')[0]
                    practitioners.loc[GMP,'_id'] = str(location)
                # print('Created ' + GMP + ' id - '+location)
                else:
                    print("No Location header in response: ",r.status_code)
                    print("Response headers:", r.headers)
                    print(json.dumps(practitionerJSON, indent=2, ensure_ascii=False))
                    print(r.text)
        return practitioners

    @task(task_id="Transform_To_PractitionerRole")
    def transform_pratitionerroles(updatedpractitioners, updatedorganisations):
        updatedpractitioners['GMP'] = updatedpractitioners.index.astype(str)
        practitionerroles = pd.merge(updatedpractitioners[["GMP","_id","Practitioner_Name","ODS","Started","Ended"]], updatedorganisations[["_id","Organisation_Name"]], how="inner", on=["ODS"]).set_index('GMP')

        # Remove entries without ID's in the database

        practitionerroles = practitionerroles.drop(practitionerroles[practitionerroles._id_x == -1].index)
        practitionerroles = practitionerroles.drop(practitionerroles[practitionerroles._id_y == -1].index)


        return practitionerroles

    @task(task_id="Extract_FHIR_PractitionerRole_GPPractice")
    def extract_FHIR_practitionerroless(ds=None, **kwargs):

        conn = dbapi.connect(
            hostname=host,
            port=port,
            namespace=namespace,
            username=user,
            password=password
        )

        # create a cursor
        cursor = conn.cursor()

        sql = """
              select prac._id, prac.Key, prac._lastUpdated,
                     resource.ResourceString, null as ODS, null as GMP
              from HSFHIR_X0001_S.PractitionerRole prac
                       inner join HSFHIR_X0001_R.Rsrc resource on resource.ID = prac._id \
              """

        cursor.execute(sql)
        data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=column_names)

        return df

    @task(task_id="Merge_PractitionerRoless_GPPractice")
    def merge_practitionerroles(practitionerroles, dfroles):
        for role in range(0, len(dfroles)):
            resourceStr = dfroles.iloc[role]['ResourceString']
            roleJSON = json.loads(resourceStr)
            key = roleJSON['practitioner']['identifier']
            #print("The key is ({})".format(key))
            system = key['system']
            value = key['value']
            if (system == 'https://fhir.hl7.org.uk/Id/gmp-number'):
                dfroles.loc[role,'GMP'] = value
            key = roleJSON['organization']['identifier']
            #print("The key is ({})".format(key))
            system = key['system']
            value = key['value']
            if (system == 'https://fhir.nhs.uk/Id/ods-organization-code'):
                dfroles.loc[role,'ODS'] = value

        practitionerroles = pd.merge(practitionerroles, dfroles, how="left", on=["ODS","GMP"]).set_index('GMP')
        ## Make a copy of the index as a column
        practitionerroles['GMP'] = practitionerroles.index.astype(str)
        practitionerroles['_id'] = practitionerroles['_id'].fillna(-1).astype(int)
        return practitionerroles

    @task(task_id="Load_PractitionerRoles_GPPractice")
    def load_practitionerroles(mergedroles):

        def convertPractitionerRoleFHIR(GMP):
            practitionerRoleJSON = {
                "resourceType": "PractitionerRole",
                "active": True,
                "practitioner": {
                    "identifier": {
                        "system": "https://fhir.hl7.org.uk/Id/gmp-number",
                        "value": GMP
                    },
                    "display": mergedroles.loc[GMP,'Practitioner_Name']
                },
                "organization": {
                    "identifier": {
                        "system": "https://fhir.nhs.uk/Id/ods-organization-code",
                        "value": mergedroles.loc[GMP,'ODS']
                    },
                    "display": mergedroles.loc[GMP,'Organisation_Name']
                },
                "code": [
                    {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "62247001",
                                "display": "General practitioner"
                            }
                        ]
                    }
                ],
                "specialty": [
                    {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "394814009",
                                "display": "General practice (specialty) (qualifier value)"
                            }
                        ]
                    }
                ],
                "period": {}
            }
            if mergedroles.loc[GMP,'_id_x'] != '' and not pd.isnull(mergedroles.loc[GMP,'_id_x']) :
                practitionerRoleJSON['practitioner']['reference'] = 'Practitioner/'+str(mergedroles.loc[GMP,'_id_x'])
            if mergedroles.loc[GMP,'_id_y'] != '' and not pd.isnull(mergedroles.loc[GMP,'_id_y']) :
                practitionerRoleJSON['organization']['reference'] = 'Organization/'+str(mergedroles.loc[GMP,'_id_y'])
            if mergedroles.loc[GMP,'Started'] != '' and not pd.isnull(mergedroles.loc[GMP,'Started']) :
                startDate = str(mergedroles.loc[GMP,'Started'])
                startDate = startDate[:4]+"-"+startDate[4:6]+"-"+startDate[6:8]
                practitionerRoleJSON['period']['start'] = startDate
            if mergedroles.loc[GMP,'Ended'] != '' and not pd.isnull(mergedroles.loc[GMP,'Ended']) :
                practitionerRoleJSON['active'] = False
                endDate = str(mergedroles.loc[GMP,'Ended'])
                endDate = endDate[:4]+"-"+endDate[4:6]+"-"+endDate[6:8]
                practitionerRoleJSON['period']['end'] = endDate
            # validate JSON
            practitionerRole = PractitionerRole(**practitionerRoleJSON)
            return practitionerRoleJSON


        new = mergedroles[['GMP']].head(maxentries).copy()
        for GMP in new.index:

            practitionerRoleJSON = convertPractitionerRoleFHIR(GMP)
            if (mergedroles.loc[GMP,'_id'] == -1):

                # Create

                r = requests.post(fhirurl+'/PractitionerRole', data=json.dumps(practitionerRoleJSON), headers=headers)

                if 'Location' in r.headers:
                    location = r.headers['Location'].split('PractitionerRole/')[1].split('/')[0]
                    practitionerRoleJSON['id']=location
                    mergedroles.loc[GMP,'ResourceString']=json.dumps(practitionerRoleJSON)
                # print('Created Role ' + GMP + ' id - '+location)
                else:
                    print("No Location header in response: ",r.status_code)
                    print("Response headers:", r.headers)
                    print(json.dumps(practitionerRoleJSON, indent=2, ensure_ascii=False))
                    print(r.text)
            else:
                # Update

                isUpdate = False
                if mergedroles.loc[GMP,'ResourceString'] != '':
                    practitionerroleMDM = json.loads(mergedroles.loc[GMP,'ResourceString'])
                    if practitionerroleMDM['active'] != practitionerRoleJSON['active']:
                        isUpdate = True
                        practitionerroleMDM['active'] = practitionerRoleJSON['active']
                        print('active')
                    try:
                        if 'period' in practitionerRoleJSON and 'start' in practitionerRoleJSON['period']:
                            if 'period' in practitionerroleMDM and 'start' in practitionerroleMDM['period']:
                                if (str(practitionerroleMDM['period']['start']) != str(practitionerRoleJSON['period']['start'])):
                                    isUpdate = True
                                    practitionerroleMDM['period'] = practitionerRoleJSON['period']
                                    print('period.start')
                            else:
                                isUpdate = True
                                #print(json.dumps(practitionerroleMDM, indent=2, ensure_ascii=False))
                                practitionerroleMDM['period'] = practitionerRoleJSON['period']
                                print('period start - not in MDM')
                        if 'period' in practitionerRoleJSON and 'end' in practitionerRoleJSON['period']:
                            if 'period' in practitionerroleMDM and 'end' in practitionerroleMDM['period']:
                                if (str(practitionerroleMDM['period']['end']) != str(practitionerRoleJSON['period']['end'])):
                                    isUpdate = True
                                    practitionerroleMDM['period'] = practitionerRoleJSON['period']
                                    print('period.end')
                            else:
                                isUpdate = True
                                practitionerroleMDM['period'] = practitionerRoleJSON['period']
                                print('period end - not in MDM')
                    except KeyError:
                        # Handle missing period key
                        isUpdate = True
                        print('period (exception)')
                        practitionerroleMDM['period'] = practitionerRoleJSON.get('period')
                    if isUpdate:
                        #print(json.dumps(organisationMDM, indent=2, ensure_ascii=False))
                        r = requests.put(fhirurl+'/PractitionerRole/'+practitionerroleMDM['id'], data=json.dumps(practitionerroleMDM), headers=headers)

                        if 'Location' in r.headers:
                            location = r.headers['Location'].split('PractitionerRole/')[1].split('/')[0]
                            # update dateframe with updated JSON
                            mergedroles.loc[GMP,'ResourceString']=json.dumps(practitionerroleMDM)
                            print('Updated Role ' + GMP + ' id - '+location)
                        else:
                            print("No Location header in response: ",r.status_code)
                            print("Response headers:", r.headers)
                            print(json.dumps(practitionerroleMDM, indent=2, ensure_ascii=False))
                            print(r.text)
        return mergedroles


    epraccur = extract_ods_organisations()
    df = extract_FHIR_organisations()
    organisations = merge_organisations(epraccur, df)
    updatedorganisations = load_organisations(organisations)

    egpcur = extract_ods_practitioners()
    dfgp = extract_FHIR_practitioners()
    practitioners = merge_practitioners(egpcur, dfgp)
    updatedpractitioners = load_practitioners(practitioners)

    dfroles = extract_FHIR_practitionerroless()
    practitionerroles = transform_pratitionerroles(updatedpractitioners, updatedorganisations)
    mergedroles = merge_practitionerroles(practitionerroles, dfroles)
    loadedroles = load_practitionerroles(mergedroles)

    [epraccur,df] >> organisations >> updatedorganisations
    [egpcur,dfgp] >> practitioners >> updatedpractitioners
    [updatedorganisations, updatedpractitioners] >> practitionerroles
    [dfroles, practitionerroles] >> mergedroles >> loadedroles


# [END tutorial]
