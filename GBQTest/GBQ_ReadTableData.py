import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/gaian/Downloads/case16370-5b90ae0ebce9.json"

client = bigquery.Client()

query = """select * from case16370.babynames.DATAPROD729_FEW"""

result = client.query(query)

result_df = result.result().to_dataframe()

print(result_df)

