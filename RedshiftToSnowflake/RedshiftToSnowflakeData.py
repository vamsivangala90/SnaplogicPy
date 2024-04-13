import pandas as pd
import psycopg2  # for Redshift connection
import snowflake.connector  # for Snowflake connection

# Replace these with your connection details
REDSHIFT_HOST = "your-redshift-host"
REDSHIFT_PORT = 5439
REDSHIFT_DATABASE = "your-redshift-database"
REDSHIFT_USER = "your-redshift-user"
REDSHIFT_PASSWORD = "your-redshift-password"

SNOWFLAKE_ACCOUNT = "your-snowflake-account"
SNOWFLAKE_USER = "your-snowflake-user"
SNOWFLAKE_PASSWORD = "your-snowflake-password"
SNOWFLAKE_WAREHOUSE = "your-snowflake-warehouse"
SNOWFLAKE_DATABASE = "your-snowflake-database"
SNOWFLAKE_SCHEMA = "your-snowflake-schema"

# Define the SQL queries for Redshift and Snowflake (replace with your actual queries)
REDSHIFT_QUERY = "SELECT * FROM your_redshift_table"
SNOWFLAKE_QUERY = "SELECT * FROM your_snowflake_schema.your_snowflake_table"


def compare_dataframes(df_redshift, df_snowflake):
  """
  Compares two DataFrames and returns a DataFrame containing differences.

  Args:
      df_redshift (pd.DataFrame): DataFrame from Redshift.
      df_snowflake (pd.DataFrame): DataFrame from Snowflake.

  Returns:
      pd.DataFrame: DataFrame containing rows that differ between the two sources.
  """
  # Find rows in Redshift but not in Snowflake (using left join)
  df_diff_left = pd.merge(df_redshift, df_snowflake, how='left', indicator=True)
  df_diff_left = df_diff_left[df_diff_left['_merge'] == 'left_only']
  df_diff_left.drop('_merge', axis=1, inplace=True)

  # Find rows in Snowflake but not in Redshift (using right join)
  df_diff_right = pd.merge(df_snowflake, df_redshift, how='right', indicator=True)
  df_diff_right = df_diff_right[df_diff_right['_merge'] == 'right_only']
  df_diff_right.drop('_merge', axis=1, inplace=True)

  # Combine differences from both sides
  df_diff = pd.concat([df_diff_left, df_diff_right])
  return df_diff


if __name__ == "__main__":
  # Connect to Redshift
  try:
    conn_redshift = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )
  except Exception as e:
    print(f"Error connecting to Redshift: {e}")
    exit(1)

  # Connect to Snowflake
  try:
    ctx = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cursor = ctx.cursor()
  except Exception as e:
    print(f"Error connecting to Snowflake: {e}")
    exit(1)

  # Execute queries and fetch data
  try:
    with conn_redshift.cursor() as cursor_redshift:
      cursor_redshift.execute(REDSHIFT_QUERY)
      df_redshift = pd.DataFrame(cursor_redshift.fetchall(), columns=[desc[0] for desc in cursor_redshift.description])

    cursor.execute(SNOWFLAKE_QUERY)
    df_snowflake = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
  except Exception as e:
    print(f"Error fetching data: {e}")
    exit(1)

  # Close connections
  conn_redshift.close()
  cursor.close()
  ctx.close()

  # Compare DataFrames and find differences
  df_diff = compare_dataframes(df_redshift, df_snowflake)

  # Print or handle
