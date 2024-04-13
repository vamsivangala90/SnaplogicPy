import os

import pandas as pd
from google.cloud import bigquery
import snowflake

def get_bigquery_data(query,credentials):
    bigqueryauth = bigquery.Client.from_service_account_json(credentials)
    df_bigquery = bigqueryauth.query(query).to_dataframe()
    return df_bigquery

def get_snowflake_data(query,snowflake_connection_params):
    snowflakeauth = snowflake.connector.connect(        user=snowflake_connection_params['user'],
        password=snowflake_connection_params['password'],
        account=snowflake_connection_params['account'],
        warehouse=snowflake_connection_params['warehouse'],
        database=snowflake_connection_params['database'])

cursorobject = snowflakeauth.cursor()
cursorobject.execute(query)
df_snowflake = pd.DataFrame(cursorobject.fetchall(), columns= [desc[0] for desc in cursorobject.description])
snowflakeauth.close()
return df_snowflake

def compare_dataframe(df_bigquery, df_snowflake)


