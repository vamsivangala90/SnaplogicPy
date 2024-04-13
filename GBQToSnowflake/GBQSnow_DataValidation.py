import os
import pandas as pd
from google.cloud import bigquery
import snowflake.connector


def get_bigquery_data(query, credentials_path):
    # Authenticate with BigQuery using service account key
    client = bigquery.Client.from_service_account_json(credentials_path)

    # Run the query and convert the result to a pandas DataFrame
    df_bigquery = client.query(query).to_dataframe()
    return df_bigquery


def get_snowflake_data(query, snowflake_connection_params):
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=snowflake_connection_params['user'],
        password=snowflake_connection_params['password'],
        account=snowflake_connection_params['account'],
        warehouse=snowflake_connection_params['warehouse'],
        database=snowflake_connection_params['database']
    )

    # Run the query and convert the result to a pandas DataFrame
    cursor = conn.cursor()
    cursor.execute(query)
    df_snowflake = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    conn.close()
    return df_snowflake


def compare_dataframes(df_bigquery, df_snowflake):
    # Compare the dataframes and return differences
    diff = pd.concat([df_bigquery, df_snowflake]).drop_duplicates(keep=False)
    return diff


def main():
    # BigQuery query and credentials path
    bigquery_query = "SELECT * FROM `case16370.babynames.DATAPROD729_FEW`"
    bigquery_credentials_path = "/home/gaian/Downloads/case16370-5b90ae0ebce9.json"

    # Snowflake query and connection parameters
    snowflake_query = "SELECT * FROM GOOGLEBIGQUERY.DATAPROD729_FEW;"
    snowflake_connection_params = {
        'user': 'Bigdatasnaplogic',
        'password': 'Snaplogic2018',
        'account': 'snaplogic',
        'warehouse': 'ELT_XS_WH',
        'database': 'FDLDB'
    }

    # Get data from BigQuery
    df_bigquery = get_bigquery_data(bigquery_query, bigquery_credentials_path)

    # Get data from Snowflake
    df_snowflake = get_snowflake_data(snowflake_query, snowflake_connection_params)

    # Compare dataframes
    diff = compare_dataframes(df_bigquery, df_snowflake)

    # Print the differences, if any
    if not diff.empty:
        print("Differences found between BigQuery and Snowflake data:")
        print(diff)
    else:
        print("No differences found. Data is consistent between BigQuery and Snowflake.")


if __name__ == "__main__":
    main()
