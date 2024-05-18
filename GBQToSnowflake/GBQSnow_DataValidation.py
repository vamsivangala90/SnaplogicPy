import os
import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
from google.cloud import bigquery
from datetime import datetime
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
    # Add a column indicating the source
    df_bigquery['source'] = 'bigquery'
    df_snowflake['source'] = 'snowflake'

    # Combine the dataframes and find the differences
    combined_df = pd.concat([df_bigquery, df_snowflake])
    diff = combined_df.drop_duplicates(keep=False)
    return diff



def main():
    bigquery_query = "SELECT * FROM `case16370.babynames.DATAPROD729_FEW_NEW`"
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
#
    # Print the differences, if any
    if not diff.empty:
        print(df_bigquery)
        print(df_snowflake)
        print("Differences found between BigQuery and Snowflake data:")
        current_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'differences_{current_timestamp}.csv'
        diff.to_csv(filename, index=False)
        print(f"Differences have been saved to '{filename}'.")
        print(diff)
    else:
        print("No differences found. Data is consistent between BigQuery and Snowflake.")

#qwerty

if __name__ == "__main__":
    main()
