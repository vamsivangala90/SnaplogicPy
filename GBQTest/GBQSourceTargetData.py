import os
from google.cloud import bigquery
import pandas as pd
# Define the source and target tables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/gaian/Downloads/case16370-5b90ae0ebce9.json"

client = bigquery.Client()

Source_table = "case16370.babynames.DATAPROD729_ALLDATATYPESS"
Target_table = "case16370.DATAPROD729_US.DATAPROD729_ALLDATATYPESS"

Source_query = "select nSTRING,nBYTES,nINTEGER,NFLOAT from `case16370.babynames.DATAPROD729_ALLDATATYPESS`"
Target_query = "select nSTRING,nBYTES,nINTEGER,NFLOAT from `case16370.DATAPROD729_US.DATAPROD729_ALLDATATYPESS`"


# Execute the queries and fetch the results as dataframes
source_df = client.query(Source_query).to_dataframe()
target_df = client.query(Target_query).to_dataframe()

# Compare the dataframes using pandas
def compare_dataframes(df1, df2):
    # Compare the dataframes using `merge` with an inner join on all columns
    merged_df = df1.merge(df2, indicator=True, how='outer')

    # Filter rows that are present in one dataframe but not the other
    differences_df = merged_df[merged_df['_merge'] != 'both']

    if differences_df.empty:
        print("The source and target tables match.")
    else:
        print("Differences found between source and target tables:")
        print(differences_df)


# Compare the source and target dataframes
compare_dataframes(source_df, target_df)



