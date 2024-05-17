from datetime import datetime
from db_utils import compare_data, save_to_csv

# Get user input for database types and queries
database_type1 = input("Enter database type 1 (BigQuery or Snowflake): ").upper()
query1 = input("Enter query for database 1: ")

database_type2 = input("Enter database type 2 (BigQuery or Snowflake): ").upper()
query2 = input("Enter query for database 2: ")


# Compare data and get results
comparison_data = compare_data(database_type1, query1, database_type2, query2)

# Generate timestamp for filename

now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Format timestamp (YYYY-MM-DD_HH-MM-SS)
output_filename = f"data_comparison_{now}.csv"

# Save comparison data to CSV
save_to_csv(comparison_data, output_filename)

print(f"Data comparison saved to: {output_filename}")
