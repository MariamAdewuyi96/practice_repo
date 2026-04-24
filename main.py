from dotenv import load_dotenv
from etl_pipeline import extract, get_snowflake_engine, get_postgres_engine, load_to_snowflake

load_dotenv() 

def main():
    pg_engine = get_postgres_engine()
    sf_engine = get_snowflake_engine()
    
    
    dfs = extract(pg_engine) 
    
    load_to_snowflake(dfs, sf_engine)

if __name__ == "__main__":
    main()
