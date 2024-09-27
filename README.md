Databricks App - Geospatial points

# Purpose
Demo app showing plotting properties as points on a map. Designed to be used as a Databricks app, directly querying data from Databricks tables.


# Development
To sync the changes from local to databricks:
databricks workspace import-dir ./src /Workspace/Users/james.graham@databricks.com/databricks-apps/geo --overwrite --profile databricks-e2-demo-field-eng


Running locally:
- Set up new virtual python env, using venv and python 3.11
- [optional] To get the image of the property to display within the pop-up, add your Google Maps API key 
- pip3 install -r requirements.txt
- Set env var for SQL Warehouse: export HTTP_PATH=/sql/1.0/warehouses/475b94ddc7cd5211
- Make sure SQL warehouse and catalog has granted permissions for the app service principle. Get this from the Databricks Apps UI page.
- Run app locally: gunicorn --reload -w 4 -b 127.0.0.1:8000 app:app


Deploying on Databricks:
Syntax: databricks apps deploy APP_NAME SOURCE_CODE_PATH [flags]
Example: databricks apps deploy jg-test-geo /Workspace/Users/james.graham@databricks.com/databricks-apps/geo -p e2-demo-field-eng