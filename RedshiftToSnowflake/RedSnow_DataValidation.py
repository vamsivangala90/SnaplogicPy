import pandas as pd
import snowflake.connector
import psycopg2


def connect(conn_type, connection_params):
    """Connects to a database based on connection type."""
    try:
        if conn_type == "redshift":
            # Pass parameters as keyword arguments directly
            return psycopg2.connect(
                host=connection_params["host"],
                port=connection_params["port"],
                dbname=connection_params["dbname"],
                user=connection_params["user"],
                password=connection_params["password"]
            )
        elif conn_type == "snowflake":
            # Snowflake connection can accept the dictionary directly
            return snowflake.connector.connect(**connection_params)
        else:
            raise ValueError(f"Invalid connection type: {conn_type}")
    except Exception as e:
        print(f"Error connecting to {conn_type}: {e}")
        return None

def read_table_data(conn, table_name, limit=1000):
    """Reads table data into a Pandas DataFrame."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=[col[0] for col in cursor.description])


def compare_tables(source_conn, target_conn, table_name):
    """Compares schema and data of source and target tables using Pandas."""
    source_df = read_table_data(source_conn, table_name)
    target_df = read_table_data(target_conn, table_name)

    # Schema comparison (excluding data types)
    schema_diff = set(source_df.columns) ^ set(target_df.columns)

    # Data comparison (using set difference for row-level differences)
    data_diff = pd.concat([source_df, target_df]).drop_duplicates(keep=False)

    # Summarize results
    results = {
        "Schema Differences": list(schema_diff),
        "Data Differences (limited to the first 1000 rows)": data_diff.to_dict(orient="records")[:10],
    }
    return results


def main():
    # Replace these with your connection details
    redshift_conn_string = {
        "host": "sudesh-redshift-cluster-1.cwun0q5ce6uo.us-east-2.redshift.amazonaws.com",
        "port": 5439,
        "dbname": "dev",
        "user": "awsuser",
        "password": "Snaplogic123",
    }
    snowflake_conn_string = {
        "account": "snaplogic",
        "user": "Bigdatasnaplogic",
        "password": "Snaplogic2018",
        "warehouse": "ELT_XS_WH",
        "database": "FDLDB",
        "schema": "SALESFORCE",
    }

    source_conn = connect("redshift", redshift_conn_string)
    target_conn = connect("snowflake", snowflake_conn_string)

    if not source_conn or not target_conn:
        exit(1)

    table_name = "VKTESTPYTHON"  # Replace with the actual table name

    # Compare tables and print results
    comparison_results = compare_tables(source_conn, target_conn, table_name)
    for section, details in comparison_results.items():
        print(f"\n{section}:")
        if details:
            for item in details:
                print(item)
        else:
            print("No differences found.")


if __name__ == "__main__":
    main()
