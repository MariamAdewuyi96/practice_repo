import pandas as pd
import logging
from sqlalchemy import create_engine, URL, inspect, text
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy import URL as snowflake_URL
from dotenv import load_dotenv
import os

load_dotenv()
#configure logging
logging.basicConfig(level=logging.INFO, filename= 'etl.log', format='%(asctime)s - %(levelname)s - %(message)s')

# -- Configuration --
def get_postgres_engine():
    postgres_url =  URL.create(
        drivername= "postgresql",
        username= os.getenv("postgres_username"),
        password= os.getenv("postgres_password"),
        host= os.getenv("postgres_host"),
        port= os.getenv("postgres_port"),
        database= os.getenv("postgres_database")
    )
    return create_engine(postgres_url)

def extract(source_engine):
    inspector = inspect(source_engine) 
    all_tables = inspect.get_table_names()   
    logging.info(f'available tables for migration: {all_tables}')
    
    dfs = {}
    with source_engine.connect() as conn:
        #looping through the list of tables
        for tables in all_tables:
            logging.info(f"Reading {table}...")
            query = text(f"select * from '{table}' ")
            dfs[table] = pd.read_sql(query, conn)
        logging.info("\n successful!")        
    return dfs

#snowflake configuration
def get_snowflake_engine():
    snow_url = snowflake_URL(
        account= os.getenv('snowflake_account'),
        user= os.getenv('snowflake_user'),
        password= os.getenv('snowflake_password'),
        database= os.getenv('snowflake_database'),
        schema= os.getenv('snowflake_schema'),
        warehouse= os.getenv('snowflake-warehouse'),
        role= os.getenv('snowflake_role'),
    )
    return create_engine(snow_url)

def load_to_snowflake(dfs, engine)
#tset connection
try:
    engine = get_snowflake_engine()
    with engine.connect as sf_conn:
        logging.info("successfully connected to Snowflake!")
except Exception as e:
    logging.error(f"connection error: {e}")

try: 
    for table_name, df in dfs.items():
    #Adding STG_ to all table name    
        destination_name = f"STG_{table_name.upper()}"
        logging.info(f"writing {table_name} to snowflake")
    #wrting to snowflake
        success, nchunks, nrows, _n = write_pandas(
            sf_conn,
            df,
            table_name = destination_name,
            auto_create_table = True,
            use_logical_type = True
        )
        if success:
            logging.info(f"sucessfully moved {nrows} rows into {destination_name}")
except Exception as e:
    logging.error(f"Connection error while loading: {e}")  