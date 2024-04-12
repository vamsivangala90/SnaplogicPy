import cx_Oracle
def table_exists(db_name, schema_name, table_name):
  """
  Checks if a table exists in a schema using cx_Oracle for Oracle.

  Args:
      db_name: Name of the database (SID).
      schema_name: Name of the schema.
      table_name: Name of the table to check.

  Returns:
      True if the table exists, False otherwise.
  """
  dsn = cx_Oracle.makedsn('oratestdb2.cwztruwzzvnq.us-east-1.rds.amazonaws.com', '1521', service_name='TESTDB')  # Replace with your details
  conn = cx_Oracle.connect(user='PRASANNA$#_', password='1nt3grat10n', dsn=dsn)
  cur = conn.cursor()


  query = f"""
  SELECT EXISTS (
      SELECT 1
      FROM ALL_TABLES
      WHERE OWNER = '{schema_name}'
        AND TABLE_NAME = '{table_name}'
  );
  """

  cur.execute(query)
  exists = cur.fetchone()[0]

  cur.close()
  conn.close()
  return exists

# Example usage (replace with your credentials)
db_name = "TESTDB"
schema_name = "PRASANNA$#_"
table_name = "SRC_ALL_DATA_TYPES"

if table_exists(db_name, schema_name, table_name):
  print(f"Table {table_name} exists in schema {schema_name}.")
else:
  print(f"Table {table_name} does not exist in schema {schema_name}.")
