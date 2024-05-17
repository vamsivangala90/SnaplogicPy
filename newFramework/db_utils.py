import configparser
import pandas as pd
from google.cloud import bigquery
from snowflake.connector import connect
import logging


def get_connection(database_type):
    """
  Establishes a connection to the database based on the provided type (BigQuery or Snowflake).
  """
    config = configparser.ConfigParser()
    config.read('config.ini')

    if database_type.upper() == "BIGQUERY":
        project_id = config.get('BIGQUERY', 'project_id')
        service_account_file = config.get('BIGQUERY', 'service_account_file')

        if service_account_file:
            client = bigquery.Client.from_service_account_json(service_account_file)
        else:
            client = bigquery.Client(project=project_id)
        return client

    elif database_type.upper() == "SNOWFLAKE":
        account = config.get('SNOWFLAKE', 'account')
        user = config.get('SNOWFLAKE', 'user')
        password = config.get('SNOWFLAKE', 'password')
        database = config.get('SNOWFLAKE', 'database')
        schema = config.get('SNOWFLAKE', 'schema')

        try:
            connection = connect(
                user=user,
                password=password,
                account=account,
                database=database,
                schema=schema
            )
            return connection
        except Exception as e:
            logging.error(f"Error connecting to Snowflake: {e}")
            raise ConnectionError(f"Error connecting to Snowflake: {e}")

    else:
        logging.error("Unsupported database type: {}".format(database_type))
        raise ValueError("Unsupported database type: {}".format(database_type))



def execute_query(query, connection):
  """
  Executes a provided SQL query on the established connection.
  """
  logging.info(f"Executing query: {query}")

  if isinstance(connection, bigquery.Client):
    query_job = connection.query(query)
    results = query_job.result()
    return pd.DataFrame(results)

  elif hasattr(connection, 'cursor'):
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(results, columns=columns)

  else:
    logging.error("Unsupported connection type for query execution")
    raise NotImplementedError("Unsupported connection type for query execution")


def compare_data(database_type1, query1, database_type2, query2):
    """
  Fetches data from two databases, compares them, and returns the comparison results as a DataFrame.
  """
    try:
        data1 = execute_query(query1, get_connection(database_type1))
        data2 = execute_query(query2, get_connection(database_type2))

        comparison = data1.ne(data2)
        return comparison

    except Exception as e:
        logging.error(f"Error comparing data: {e}")
        raise


def save_to_csv(data, filename):
    """
  Saves a Pandas DataFrame to a CSV file.
  """
    try:
        data.to_csv(filename)
        logging.info(f"Comparison data saved to: {filename}")
    except Exception as e:
        logging.error(f"Error saving comparison data to CSV: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
