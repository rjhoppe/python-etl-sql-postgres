from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

# Get sensitive values from dotenv
load_dotenv()

pg_pwd = os.getenv("PGPASS")
pg_uid = os.getenv("PGUID")
pg_server = os.getenv("PGSERVER")
sql_uid = os.getenv("SQLPASS")
sql_pwd = os.getenv("SQLUID")
sql_server = os.getenv("SQLSERVER")
sql_database = os.getenv("SQLDATABASE")

# SQL db details
sql_driver = "{SQL Server Native Client 11.0}"

# Extract data from sql server
def extract():
  try:
    src_conn = pyodbc.connect('DRIVER=' + sql_driver + ';SERVER=' + sql_server + '\SQLEXPRESS' + ';DATABASE=' + sql_database + ';UID=' + sql_uid + ';PWD=' + sql_pwd)
    src_cursor = src_conn.cursor()
    # Execute query
    src_cursor.execute(""" select t.name as table_name
    from sys.tables t where t.name in ('DimProduct', 'DimProductSubcategory', 'DimProductSubcategory', 'DimProductCategory', 'DimSalesTerritory', 'FactInternetSales') """)
    src_tables = src_cursor.fetchall()
    for tbl in src_tables:
      # Query and load save data to dataframe
      df = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)
      load(df, tbl[0])
  except Exception as e:
    print('Data extract error: ' + str(e))
  finally:
    src_conn.close()

# Load data to postgres
def load(df, tbl):
  try:
    rows_imported = 0
    # Create conn to Postgress using sqlalchemy create_engine
    engine = create_engine(f'postgres://{pg_uid}:{pg_pwd}@{pg_server}:5432/AdventureWorks')
    print(f'Importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
    # Save df to Postgres
    # Truncate and Load method - erases the data each time it runs and then reloads it into the destination table
    # Call SQL func from pandas - call tbl name and append stg (staging) to declare it as staging table 
    df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
    rows_imports += len(df)
    # Add elapsed time to finish print out
    print('Data imported successfully')
  except Exception as e:
    print('Data load error : ' + str(e))

try:
  # Call extract function
  extract()
except Exception as e:
  print("Error while extracting data: " + str(e))

