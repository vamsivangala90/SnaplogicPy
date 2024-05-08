import pandas as pd
from google.cloud import bigquery
import snowflake.connector
import s3fs
def get_bigquery_data(query, credentials_path):
    client = bigquery.Client.from_service_account_json(credentials_path)
    df_bigquery = client.query(query).to_dataframe()
    return df_bigquery
def get_snowflake_data(query, snowflake_connection_params):
    conn = snowflake.connector.connect(
        user=snowflake_connection_params['user'],
        password=snowflake_connection_params['password'],
        account=snowflake_connection_params['account'],
        warehouse=snowflake_connection_params['warehouse'],
        database=snowflake_connection_params['database'],
        schema=snowflake_connection_params['schema']
    )
    cursor = conn.cursor()
    cursor.execute(query)
    df_snowflake = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    cursor.close()
    conn.close()
    return df_snowflake
def compare_dataframes(df_bigquery, df_snowflake):
    bigquery_columns = set(df_bigquery.columns)
    snowflake_columns = set(df_snowflake.columns)
    extra_columns_in_snowflake = snowflake_columns - bigquery_columns

    # Check if there are extra columns in Snowflake
    if extra_columns_in_snowflake:
        print("Extra columns found in Snowflake:", extra_columns_in_snowflake)
        for col in extra_columns_in_snowflake:
            print(f"\nData from extra column '{col}':")
            print(df_snowflake[col])
    else:
        print("No extra columns found in Snowflake compared to BigQuery.")
        diff_df = pd.concat([df_bigquery, df_snowflake]).drop_duplicates(keep=False)
    return diff_df
def main():
    bigquery_query = "SELECT * FROM `case16370.babynames.DATAPROD729_FEW_NEW`"
    bigquery_credentials_path = "/home/gaian/Downloads/case16370-5b90ae0ebce9.json"
    snowflake_query = "SELECT * FROM GOOGLEBIGQUERY.DATAPROD729_FEW;"
    snowflake_connection_params = {
        'user': 'Bigdatasnaplogic',
        'password': 'Snaplogic2018',
        'account': 'snaplogic',
        'warehouse': 'ELT_XS_WH',
        'database': 'FDLDB',
        'schema': 'GOOGLEBIGQUERY'
    }
    df_bigquery = get_bigquery_data(bigquery_query, bigquery_credentials_path)
    df_snowflake = get_snowflake_data(snowflake_query, snowflake_connection_params)
    differences_df = compare_dataframes(df_bigquery, df_snowflake)
    if not differences_df.empty:
        print("Differences found between BigQuery and Snowflake data:")
        print(differences_df)
    else:
        print("No differences found. Data is consistent between BigQuery and Snowflake.")


if __name__ == "__main__":
    main()
