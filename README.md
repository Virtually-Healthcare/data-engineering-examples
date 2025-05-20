Examples work with this FHIR Server [Intersystems Open Exchange - iris-fhir-template](https://openexchange.intersystems.com/package/iris-fhir-template)

Management Portal 

http://localhost:32783/csp/sys/UtilHome.csp

Intersystems python model 

`pip install intersystems-irispython`

# SQL Explorer 

http://localhost:32783/csp/sys/exp/%25CSP.UI.Portal.SQL.Home.zen?$NAMESPACE=FHIRSERVER

```sql
SELECT 
*
FROM HSFHIR_X0001_S.Observation
where patient = 'Patient/6' and code [ '38483-4';
````

# Useful resources

Intersystems Engineer
https://github.com/SylvainGuilbaud?tab=repositories
