import os
from google.cloud import bigquery
import pandas as pd

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/gaian/Downloads/case16370-5b90ae0ebce9.json'

# Initialize BigQuery client
client = bigquery.Client()


# Define the query to execute
query = """
    SELECT *
    FROM
        `case16370.babynames.DATAPROD729_FEW`
"""

# Execute the query
query_job = client.query(query)

# Get the query results
results_df = query_job.result().to_dataframe()

# Print the results
print(results_df)
