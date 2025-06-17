# Install

## Apache Airflow Install

https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

Steps

### Initialise airflow.cfg (Optional)

> docker compose run airflow-cli airflow config list

### Initialise Database

> docker compose up airflow-init

### Running Airflow

> docker compose up



## FHIR Repository (Intersystems)

Examples work with this FHIR Server [Intersystems Open Exchange - iris-fhir-template](https://openexchange.intersystems.com/package/iris-fhir-template)

Management Portal 

[IRIS Managment Portal](http://localhost:32783/csp/sys/UtilHome.csp)

Intersystems python model 

`pip install intersystems-irispython`

`pip install sqlalchemy-iris`

`pip install fhir.resources`

### Patient Data Fix

[iris-fhir-template issue 32](https://github.com/intersystems-community/iris-fhir-template/issues/32)

Follow instructions up to this command

`FHIRSERVER>d ##class(fhirtemplate.Setup).LoadPatientData("/irisdev/app/output/fhir","FHIRSERVER","/fhir/r4")`

Instead 

Using [IRIS Package Manager](http://localhost:32783/csp/sys/mgr/%25CSP.UI.Portal.Mappings.zen?MapType=Prj&PID=FHIRSERVER) map package `fhirtemplate` from USER namespace.

Copy contents of

> output\fhir

These need to be copied to

> data\fhir

Which is a mapped folder in the docker-compose. Then the load command is (presuming the class has been mapped to FHIRSERVER namespace)

> d ##class(fhirtemplate.Setup).LoadPatientData("/data/fhir","FHIRSERVER","/fhir/r4")


### SQL Explorer 

[IRIS SQL Explorer](http://localhost:32783/csp/sys/exp/%25CSP.UI.Portal.SQL.Home.zen?$NAMESPACE=FHIRSERVER)

```sql
SELECT 
*
FROM HSFHIR_X0001_S.Observation
where patient = 'Patient/6' and code [ '38483-4';
````

### Useful resources

Intersystems (France) Engineer
https://github.com/SylvainGuilbaud?tab=repositories


