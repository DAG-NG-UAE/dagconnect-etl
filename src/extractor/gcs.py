from src.utils.GCS.play_console_etl import extract_table_name
from google.cloud import storage
import pandas as pd
import io
import os
from google.oauth2 import service_account
from src.utils.GCS.play_console_etl import read_file
from src.utils.supabase_connector import get_play_store_last_sync_time
from src.loader.database import SupabaseLoader

class GCSExtractor:
    @staticmethod
    def begin_fetch(): 
        credentials = service_account.Credentials.from_service_account_file(
            os.getenv("GCP_SERVICE_ACCOUNT_KEY", "service-account.json"),
            scopes=[
                "https://www.googleapis.com/auth/androidpublisher",
                "https://www.googleapis.com/auth/devstorage.read_only",
            ],
        )
        client = storage.Client(credentials=credentials)
        bucket = "pubsite_prod_8540944584425013860"

        print("ALL files in bucket:\n")
        for blob in client.list_blobs(bucket):
            table_name = extract_table_name(blob.name, "com.dagconnect")
            print(f"  Reading  : {blob.name}")
            print(f"  → Table  : {table_name}")

            df = read_file(client, bucket, blob.name)
            last_sync = get_play_store_last_sync_time(table_name, df)
            SupabaseLoader.load_playstore_data(table_name, df, last_sync)
            print(f"  Rows: {len(df)}  |  Columns: {list(df.columns)}")
            # print(df.head(3).to_string())
            print()
    