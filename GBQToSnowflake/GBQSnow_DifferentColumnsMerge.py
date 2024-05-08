import pandas as pd
from google.cloud import bigquery
import snowflake.connector

def get_bigquery_data(query, credentials_path):

        client = bigquery.Client.from_service_account_json(credentials_path)
        df_bigquery = client.query(query).to_dataframe()
        return df_bigquery
def get_snowflake_data(query, snowflake_connection_params):
        conn = snowflake.connector.connect(**snowflake_connection_params)
        cursor = conn.cursor()
        cursor.execute(query)
        df_snowflake = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        conn.close()
        return df_snowflake

def compare_dataframes(df_bigquery, df_snowflake, join_column):
    if join_column not in df_bigquery.columns or join_column not in df_snowflake.columns:
        print(f"Join column '{join_column}' not found in both DataFrames. Skipping comparison.")
        return pd.DataFrame()
        merged_df = pd.merge(df_bigquery, df_snowflake, how='left', on=join_column, indicator=True)
        differences = merged_df[merged_df['_merge'] != 'both']
        return differences

def main():
    """Main function to execute data retrieval, comparison, and error handling."""

    bigquery_query = "SELECT * FROM `case16370.babynames.DATAPROD729_FEW_NEW`"
    bigquery_credentials_path = "/home/gaian/Downloads/case16370-5b90ae0ebce9.json"
    snowflake_query = "SELECT * FROM GOOGLEBIGQUERY.DATAPROD729_FEW;"
    snowflake_connection_params = {
        'user': 'Bigdatasnaplogic',
        'password': 'Snaplogic2018',
        'account': 'snaplogic',
        'warehouse': 'ELT_XS_WH',
        'database': 'FDLDB'
    }
    join_column = "ID"

    try:
        df_bigquery = get_bigquery_data(bigquery_query, bigquery_credentials_path)
        df_snowflake = get_snowflake_data(snowflake_query, snowflake_connection_params)

        differences = compare_dataframes(df_bigquery, df_snowflake, join_column)

        if not differences.empty:
            print("Differences found between BigQuery and Snowflake data:")
            print(differences)
        else:
            print("No differences found. Data is consistent between BigQuery and Snowflake.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
