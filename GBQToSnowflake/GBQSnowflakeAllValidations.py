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
    # Validation 1: Row and column count comparison
    if df_bigquery.shape != df_snowflake.shape:
        print("Validation failed: Row and column counts differ between BigQuery and Snowflake.")
        print(f"BigQuery rows: {df_bigquery.shape[0]}, columns: {df_bigquery.shape[1]}")
        print(f"Snowflake rows: {df_snowflake.shape[0]}, columns: {df_snowflake.shape[1]}")
        return None  # Exit if row/column counts differ

    # Validation 2: Duplicate rows in BigQuery
    if df_bigquery.duplicated().sum() > 0:
        print("Validation failed: BigQuery has duplicate rows, which are not allowed in Snowflake.")
        return None  # Exit if duplicates exist in BigQuery

    # Validation 3: keys in BigQuery present in Snowflake
    key_columns = ['col1','col2']  # Replace with your actual key columns
    df_bigquery_keys = df_bigquery[key_columns]
    df_snowflake_keys = df_snowflake[key_columns]
    missing_keys = df_bigquery_keys.merge(df_snowflake_keys, how='left', indicator=True)
    missing_keys = missing_keys[missing_keys['_merge'] == 'left_only']
    if not missing_keys.empty:
        print("Validation failed: Keys present in BigQuery are missing from Snowflake:")
        print(missing_keys)
        return None  # Exit if key columns are missing in Snowflake

    # Validation 4: Special characters in Snowflake
    snowflake_cols = list(df_snowflake.columns)
    for col in snowflake_cols:
        if any(char in col for char in set(r"!@#$%^&*()_+-=[]{};':|\,.<>/? ")):
            print(f"Validation failed: Column '{col}' in Snowflake contains special characters.")
            return None  # Exit if special characters found in Snowflake column names

    # Data is consistent, identify differences (optional)
    diff = df_bigquery.merge(df_snowflake, how='outer', indicator=True)
    diff = diff[diff['_merge'] != 'both']
    if not diff.empty:
        print("Differences found between BigQuery and Snowflake data:")
        return diff
    else:
        print("Data is consistent! No discrepancies found.")
        return None

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

    #print(df_bigquery.head(2))
    #df_bigquery.tail(2)
    #print(df_bigquery.shape)
    df_bigquery.info()
    print(df_bigquery.describe())
    """df_bigquery.describe()"""


    # Compare dataframes
    diff = compare_dataframes(df_bigquery, df_snowflake)

    # Handle potential None return value from compare_dataframes
    if diff is None:
        # Validation failures occurred, messages likely printed in compare_dataframes
        print("Data validation failed. Please refer to previous messages for details.")
    else:
        # Proceed with handling differences or indicating consistency
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

if __name__ == "__main__":
    main()

