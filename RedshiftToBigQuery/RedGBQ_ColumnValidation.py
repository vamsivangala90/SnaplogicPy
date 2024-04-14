import pandas as pd
import psycopg2
from google.cloud import bigquery

REDSHIFT_CONNECTION_DETAILS = {
    "host": "your-redshift-host",
    "port": 5439,
    "dbname": "your-redshift-database",
    "user": "your-redshift-user",
    "password": "your-redshift-password",
}

BIGQUERY_PROJECT_ID = "your-bigquery-project-id"
BIGQUERY_DATASET_ID = "your-bigquery-dataset-id"
TABLE_NAME = "your_table_name"
def get_redshift_schema(table_name):
    conn = psycopg2.connect(
        host=REDSHIFT_CONNECTION_DETAILS["host"],
        port=REDSHIFT_CONNECTION_DETAILS["port"],
        dbname=REDSHIFT_CONNECTION_DETAILS["dbname"],
        user=REDSHIFT_CONNECTION_DETAILS["user"],
        password=REDSHIFT_CONNECTION_DETAILS["password"],
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return columns
def get_bigquery_schema(table_name):
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    full_table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}"
    table = client.get_table(full_table_id)
    columns = [field.name for field in table.schema]
    return columns
def main():
    redshift_columns = get_redshift_schema(TABLE_NAME)
    bigquery_columns = get_bigquery_schema(TABLE_NAME)
    redshift_columns_set = set(redshift_columns)
    bigquery_columns_set = set(bigquery_columns)
    new_columns = bigquery_columns_set - redshift_columns_set

    if new_columns:
        print(f"New columns added in BigQuery after ETL: {new_columns}")
    else:
        print("No new columns added in BigQuery after ETL.")

if __name__ == "__main__":
    main()
