import os
import psycopg2
import requests
from dotenv import load_dotenv
from src.config.schemas import TABLES_SCHEMA
import pandas as pd
import time 

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_DIRECT_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
BASELINE_DATE = "2026-03-06 00:00:00"

DTYPE_MAP = {
    "int64":   "INTEGER",
    "float64": "NUMERIC(10,4)",
    "bool":    "BOOLEAN",
    "object":  "TEXT",
}
 

def get_last_sync_time(table_name):
    try:
        print("trying to get the last sync from our etl db...")
        # 1. Try to get the last record via the REST API to bypass needing the `supabase` library
        url = f"{SUPABASE_URL}/rest/v1/{table_name}?select=createddate&order=createddate.desc&limit=1"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            # Check for specific missing table errors from Supabase REST API
            error_text = response.text.lower()
            if ("relation" in error_text and "does not exist" in error_text) or "could not find the table" in error_text:
                raise Exception(f"TableNotFound: {response.text}") # Custom exception to be caught below
            else:
                raise Exception(f"HTTP Error: {response.status_code} - {response.text}")
            
        data = response.json()
        return data[0]['createddate'] if data else "2026-03-06 00:00:00"

    except Exception as e:
        # 2. If it's a "Table Not Found" error, we build it immediately
        # This now catches our custom "TableNotFound" exception or other errors
        if "tablenotfound" in str(e).lower() or ("relation" in str(e).lower() and "does not exist" in str(e).lower()) or ("could not find the table" in str(e).lower()):
            print(f"Table '{table_name}' missing in Supabase. Building it now...")
            
            # Get the blueprint from your config/schemas.py
            schema = TABLES_SCHEMA.get(table_name)
            if not schema:
                raise ValueError(f"Table name '{table_name}' does not have a schema definition in src/config/schemas.py. Check for typos (singular vs plural)!")
            
            # 3. Connect directly to Supabase via Postgres to run the CREATE command
            # Note: For psycopg2 to work, you will need a postgresql:// connection string, not an https:// URL!
            with psycopg2.connect(os.getenv("SUPABASE_POSTGRES_URL", SUPABASE_URL)) as conn:
                with conn.cursor() as cur:
                    cols = ", ".join([f"{col} {dtype}" for col, dtype in schema['columns']])
                    
                    # Create the table
                    cur.execute(f"CREATE TABLE {table_name} ({cols});")
                    
                    # Add that comment
                    cur.execute(f"COMMENT ON TABLE {table_name} IS '{schema['comments']['table']}';")

                    # Tell PostgREST to reload its schema cache
                    cur.execute("NOTIFY pgrst, 'reload schema';")

                conn.commit()

            # Wait a bit for PostgREST to actually reload
            time.sleep(3)       
            print(f"Table '{table_name}' is live. Starting from baseline date.")
            return "2026-03-06 00:00:00"
        
        raise e



 
def infer_pg_type(series: pd.Series) -> str:
    """Infer a Postgres type from a pandas Series dtype."""
    dtype = str(series.dtype)
    if "date" in series.name:
        return "DATE"
    return DTYPE_MAP.get(dtype, "TEXT")


def get_play_store_last_sync_time(table_name, df): 
    """ Get the last sync time for the play store tables. 
        - Table exists -> return <MAX(createddate)
        - Table does not exist ->  infer columsn from df, create table and return baseline date 
    """
    try: 
        print(f"    Checking last sync for '{table_name}'...")
        url = f"{SUPABASE_URL}/rest/v1/{table_name}?select=createddate&order=createddate.desc&limit=1"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            # Check for specific missing table errors from Supabase REST API
            error_text = response.text.lower()
            if ("relation" in error_text and "does not exist" in error_text) or "could not find the table" in error_text:
                raise Exception(f"TableNotFound: {response.text}") # Custom exception to be caught below
            else:
                raise Exception(f"HTTP Error: {response.status_code} - {response.text}")
            
        data = response.json()
        return data[0]['createddate'] if data else "2026-03-06 00:00:00"
    except Exception as e: 
        if "tablenotfound" in str(e).lower() or "relation" in str(e).lower() or "could not find the table" in str(e).lower():
            print(f"  Table '{table_name}' not found — creating from df columns...")
 
            # Infer columns from the normalised df + add createddate
            col_definitions = ", ".join([
                f"{col} {infer_pg_type(df[col])}" for col in df.columns
            ])
            col_definitions += ", createddate TIMESTAMP WITH TIME ZONE DEFAULT NOW()"
 
            with psycopg2.connect(os.getenv("SUPABASE_POSTGRES_URL")) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_definitions});")
                conn.commit()
 
            print(f"  Table '{table_name}' created. Starting from baseline: {BASELINE_DATE}")
            return "2026-03-06 00:00:00"
        
        raise e 
