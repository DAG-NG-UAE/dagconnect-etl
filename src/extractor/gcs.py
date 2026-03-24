from src.utils.GCS.play_console_etl import extract_table_name
from google.cloud import storage
import pandas as pd
import io
import os
import json
from google.oauth2 import service_account
from src.utils.GCS.play_console_etl import read_file
from src.utils.supabase_connector import get_play_store_last_sync_time
from src.loader.database import SupabaseLoader

class GCSExtractor:
    @staticmethod
    def begin_fetch(): 
        gcp_key = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
        bucket = os.getenv("PLAY_BUCKET_NAME")
        package_name = os.getenv("PLAY_PACKAGE_NAME")

        if not gcp_key:
            raise ValueError("GCP_SERVICE_ACCOUNT_JSON is missing from environment variables!")
        if not bucket:
            raise ValueError("PLAY_BUCKET_NAME is missing from environment variables!")
        if not package_name:
            raise ValueError("PLAY_PACKAGE_NAME is missing from environment variables!")
        
        key_data = json.loads(gcp_key)
        credentials = service_account.Credentials.from_service_account_info(
            key_data,
            scopes=[
                "https://www.googleapis.com/auth/androidpublisher",
                "https://www.googleapis.com/auth/devstorage.read_only",
            ],
        )
        client = storage.Client(credentials=credentials)
        

        print("ALL files in bucket:\n")
        for blob in client.list_blobs(bucket):
            table_name = extract_table_name(blob.name, package_name)
            print(f"  Reading  : {blob.name}")
            print(f"  → Table  : {table_name}")

            df = read_file(client, bucket, blob.name)
            last_sync = get_play_store_last_sync_time(table_name, df)
            SupabaseLoader.load_playstore_data(table_name, df, last_sync)
            print(f"  Rows: {len(df)}  |  Columns: {list(df.columns)}")
            # print(df.head(3).to_string())
            print()
    