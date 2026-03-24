from src.utils.alert import Alert
import pandas as pd
import requests
import os
from dotenv import load_dotenv
import logging
from src.config.schemas import TABLES_SCHEMA
from src.utils.supabase_connector import get_last_sync_time
from src.utils.alert import Alert

load_dotenv()
logger = logging.getLogger(__name__)

class SupabaseLoader:
    @staticmethod
    def load_data(table_name: str, df: pd.DataFrame):
        if df.empty:
            logger.info(f"No new data for {table_name}.")
            return

        # 1. Get the list of columns we actually want
        allowed_cols = [col[0] for col in TABLES_SCHEMA[table_name]['columns']]
        
        # 2. Filter the DataFrame to ONLY those columns
        df_filtered = df[allowed_cols].copy()

        # 3. Convert Timestamps to ISO strings
        if 'createddate' in df_filtered.columns:
            df_filtered['createddate'] = df_filtered['createddate'].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

        # 4. Handle NaNs (replace with None so they become null in JSON)
        # We must cast to 'object' first, otherwise pandas might force it back to 'nan' for numeric columns
        df_filtered = df_filtered.astype(object).where(pd.notnull(df_filtered), None)

        # 5. Convert to JSON-ready list (records format)
        records = df_filtered.to_dict(orient='records')

        # print('records', records)

        # 6. The API Call
        url = f"{os.getenv('SUPABASE_DIRECT_URL')}/rest/v1/{table_name}"
        headers = {
            "apikey": os.getenv("SUPABASE_KEY"),
            "Authorization": f"Bearer {os.getenv('SUPABASE_KEY')}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates" 
        }

        logger.info(f"Uploading {len(records)} cleaned rows to {table_name}...")
        response = requests.post(url, headers=headers, json=records)
        
        if response.status_code in [200, 201]:
            logger.info("Load successful! 🚀")
        elif response.status_code == 409 or (response.status_code == 400 and "23503" in response.text):
            # Foreign Key Constraint violation (or general conflict)
            logger.warning(f"⚠️  Batch load failed for '{table_name}' due to a schema constraint (error 23503).")
            logger.info("Retrying rows individually to skip problematic records...")
            
            success_count = 0
            fail_count = 0
            errors = []

            for record in records:
                try:
                    # Wrapped in a list because the endpoint expects an array of records
                    row_resp = requests.post(url, headers=headers, json=[record])
                    if row_resp.status_code in [200, 201]:
                        success_count += 1
                    else:
                        fail_count += 1
                        # Capture the specific error for this row
                        errors.append(f"Row {record.get('id', 'N/A')}: {row_resp.text}")
                except Exception as ex:
                    fail_count += 1
                    errors.append(str(ex))

            logger.info(f"Individual fallback complete: {success_count} success, {fail_count} skipped.")
            
            if fail_count > 0:
                alert_msg = f"Skipped {fail_count} rows in table '{table_name}' because referenced IDs (like loginid) were missing.\n\n"
                alert_msg += "Samples:\n" + "\n".join(errors[:3])
                Alert.send_alert(alert_msg)
        else:
            logger.error(f"Load failed: {response.status_code} - {response.text}")

    @staticmethod
    def load_playstore_data(table_name: str, df: pd.DataFrame, last_sync: str):
        if df.empty:
            logger.info(f"No data for {table_name}.")
            return
 
        # 1. Filter rows to only those after last_sync using the date column
        df["date"] = pd.to_datetime(df["date"])
        # We ensure the last_sync is naive to match the CSV's date column
        last_sync_dt = pd.to_datetime(last_sync)
        if last_sync_dt.tzinfo is not None:
            last_sync_dt = last_sync_dt.tz_localize(None)
        df_filtered = df[df["date"] > last_sync_dt].copy()
 
        if df_filtered.empty:
            logger.info(f"No new rows for {table_name} since {last_sync}.")
            return
 
        logger.info(f"  {len(df_filtered)} new row(s) to load into '{table_name}'")
 
        # 2. Add createddate so get_play_last_sync_time can track it next run
        df_filtered["createddate"] = pd.Timestamp.utcnow().isoformat()
 
        # 3. Convert date column to string for JSON serialisation
        df_filtered["date"] = df_filtered["date"].dt.strftime("%Y-%m-%d")
 
        # 4. Handle NaNs → None (becomes null in JSON)
        df_filtered = df_filtered.astype(object).where(pd.notnull(df_filtered), None)
 
        # 5. Convert to records
        records = df_filtered.to_dict(orient="records")
 
        # 6. Upsert via Supabase REST API
        url = f"{os.getenv('SUPABASE_DIRECT_URL')}/rest/v1/{table_name}"
        headers = {
            "apikey": os.getenv("SUPABASE_KEY"),
            "Authorization": f"Bearer {os.getenv('SUPABASE_KEY')}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
 
        response = requests.post(url, headers=headers, json=records)
 
        if response.status_code in [200, 201]:
            logger.info(f"  Loaded {len(records)} rows into '{table_name}' successfully ✅")
        else:
            logger.error(f"  Load failed for '{table_name}': {response.status_code} - {response.text}")

    @staticmethod
    def record_etl_run(status: str, message: str = None):
        """Logs the ETL run status and time to the last_etl_run_time table."""
        print('...recording etl run...')
        last_sync = get_last_sync_time('last_etl_run_time')
        url = f"{os.getenv('SUPABASE_DIRECT_URL')}/rest/v1/last_etl_run_time"
        headers = {
            "apikey": os.getenv("SUPABASE_KEY"),
            "Authorization": f"Bearer {os.getenv('SUPABASE_KEY')}",
            "Content-Type": "application/json"
        }
        payload = {
            "status": status,
            "message": message
        }
        try:
            # We use POST to create a new log entry
            resp = requests.post(url, headers=headers, json=payload)
            if resp.status_code not in [200, 201]:
                logger.error(f"Failed to log ETL run: {resp.status_code} - {resp.text}")
        except Exception as e:
            logger.error(f"System error while logging ETL run: {e}")