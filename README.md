# Snaplogic ETL QA

This is a test project for QA Activities on ETL operations from Different Sources to Targets.

## Project Structure

- `scripts/`: Contains the SQL and Python scripts for the project.
- `data/`: Optional directory to store any data files used in the project.

## How to Run

1. Update your Snowflake credentials in `scripts/etl_process.py`.
2. Run `scripts/etl_process.py` to execute the ETL process.

## Scripts

- ETL Queries - Various queries for Target endpoint related verifications i.e. Different scenarios
  Scenarios:
  - verify if we have duplicate rows in target 
SELECT * from table GROUP BY col1,col2,col3,col4 HAVING COUNT(*)>1
- verify if we have count of rows same between source and target.
Select count(*) from target
- verify if we have NULLS populated in the target table.
Select * from target where col1 is null or col2 is null or col3 is null;
- verify if we can have same data beween source and target.
Select 
- verify that the target has the column transformations i.e. concat or anything else
- verify that the target has some columns dropped. 											
- verify that the keys are intact wrt primary key etc
DESC 
- verify that some columns should not contain zeoes/nulls in the target.(should ideally be filtered out in ETL) - 
SELECT * from table where Column1 IS NULL;
- verify star schema if we have data is correct between fact and dimension tables (have to check the keys integrity etc)
