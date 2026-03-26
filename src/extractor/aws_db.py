import pandas as pd
from src.utils.db_connector import DBConnector
from src.utils.supabase_connector import get_last_sync_time
import logging
import time
import requests
import random
from sqlalchemy import text
import os
from src.loader.database import SupabaseLoader

logger = logging.getLogger(__name__)

class AWSExtractor:
    @staticmethod
    def fetch_source_data(query: str, params: dict = None) -> pd.DataFrame:
        """Fetch data from AWS RDS with optional parameters."""
        engine = DBConnector.get_engine()
        try:
            logger.info("Executing extraction from AWS RDS...")
            
            # Wrap the string query in SQLAlchemy's text() function
            statement = text(query)
            
            # Now pass the 'statement' instead of the raw string
            df = pd.read_sql_query(statement, engine, params=params)
            
            logger.info(f"Extracted {len(df)} rows.")
            return df
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise

    @staticmethod
    def fetch_stagnant_data(query: str) -> pd.DataFrame: 
        print("Fetching data that does not seem to change from the AWS database...")
        result = AWSExtractor.fetch_source_data(query)
        return result

    @staticmethod
    def daily_aws_fetch(): 
        try:
            logger.info("--- 🚀 Starting Daily AWS Migration ---")
            
            # These will now all share the singleton engine/tunnel 
            # created inside DBConnector the first time one is called.
            AWSExtractor.fetch_service_requests("service_request")
            AWSExtractor.fetch_seller_data("seller")
            AWSExtractor.fetch_user_login_accounts("user_login_accounts")
            AWSExtractor.fetch_verified_customers("customer_user_login_account_mapping")
            AWSExtractor.fetch_purchased_items("purchased_items")
            AWSExtractor.fetch_purchased_vehicle_items("purchased_vehicle_items")
            AWSExtractor.fetch_user_login_activities("user_login_activities")
            
            logger.info("--- ✅ All AWS tables synced successfully ---")
            
        except Exception as e:
            logger.error(f"❌ Migration interrupted: {e}")
            raise # Re-raise so your main.py knows the cron job failed
            
        finally:
            # This is the "Cleanup" phase
            logger.info("Cleaning up database connections and closing tunnel...")
            DBConnector.dispose()
            logger.info("--- 🔒 Systems disconnected safely ---")

    @staticmethod
    def fetch_user_login_activities(table_name: str) -> pd.DataFrame: 
        print("Fetching the user login activities from AWS...")

        last_sync = get_last_sync_time(table_name)
        print(f"    - Last sync for {table_name}: {last_sync}")
        
        query = f"SELECT * FROM {table_name} where activity = 'LOGIN_REFRESH' and createddate > :last_sync"
        df = AWSExtractor.fetch_source_data(query, params={"last_sync": last_sync})
        
        if df.empty:
            print("    - No new login activities found.")
            return df

        # --- Step 1: Extract unique rounded coordinates from this batch ---
        print(f"    - {len(df)} new rows. Extracting unique coordinates...")
        unique_coords = set()
        for _, row in df.iterrows():
            lat, lon = row.get("latitude"), row.get("longitude")
            if pd.notnull(lat) and pd.notnull(lon):
                lat_f, lon_f = float(lat), float(lon)
                if not (lat_f == 0.0 and lon_f == 0.0):
                    unique_coords.add((round(lat_f, 4), round(lon_f, 4)))
        
        print(f"    - Found {len(unique_coords)} unique coordinates in this batch.")

        # --- Step 2: Fetch everything already in our geocoded_locations cache ---
        get_last_sync_time("geocoded_locations")  # Automatically builds it if missing
        supabase_headers = {
            "apikey": os.getenv("SUPABASE_KEY"),
            "Authorization": f"Bearer {os.getenv('SUPABASE_KEY')}",
        }
        existing_cache = {}
        offset = 0
        batch_size = 1000
        while True:
            # Supabase defaults to 1000 rows. We use the Range header to fetch everything.
            paginated_headers = {**supabase_headers, "Range": f"{offset}-{offset + batch_size - 1}"}
            cache_url = f"{os.getenv('SUPABASE_DIRECT_URL')}/rest/v1/geocoded_locations?select=lat_rounded,lon_rounded,country,state,city,full_address"
            cache_response = requests.get(cache_url, headers=paginated_headers)
            
            if cache_response.status_code not in [200, 206]:
                print(f"    - ⚠️ Cache fetch error (batch {offset}): {cache_response.status_code}")
                break
                
            batch_data = cache_response.json()
            if not batch_data:
                break
            
            for entry in batch_data:
                key = (float(entry["lat_rounded"]), float(entry["lon_rounded"]))
                existing_cache[key] = entry
                
            if len(batch_data) < batch_size:
                break
            offset += batch_size
        
        print(f"    - {len(existing_cache)} coordinates loaded from cache.")

        # --- Step 3: Identify which coordinates are NEW (not in cache) ---
        new_coords = unique_coords - set(existing_cache.keys())
        print(f"    - {len(new_coords)} new coordinates need geocoding.")

        # --- Step 4: Geocode only the new coordinates and save to cache ---
        for lat_r, lon_r in new_coords:
            try:
                url = "https://nominatim.openstreetmap.org/reverse"
                params = {"lat": lat_r, "lon": lon_r, "format": "jsonv2", "addressdetails": 1}
                headers = {"User-Agent": "DagConnect-Analytics/1.0 (isabella.k@bajajnigeria.com)"}

                resp = requests.get(url, params=params, headers=headers, timeout=10)

                if resp.status_code == 403:
                    print("      ⛔ Blocked by Nominatim. Stopping geocoding for this run.")
                    break
                
                if resp.status_code == 429:
                    print("      ⏳ Rate limited. Sleeping 60s before retrying...")
                    time.sleep(60)
                    continue

                resp.raise_for_status()
                data = resp.json()

                address = {} if "error" in data else data.get("address", {})
                city = address.get("city") or address.get("town") or address.get("village")

                # Save to geocoded_locations cache in Supabase
                cache_payload = {
                    "lat_rounded": lat_r,
                    "lon_rounded": lon_r,
                    "country": address.get("country"),
                    "state": address.get("state"),
                    "city": city,
                    "full_address": address,  # Supabase handles dict -> JSONB automatically
                }
                save_url = f"{os.getenv('SUPABASE_DIRECT_URL')}/rest/v1/geocoded_locations"
                save_headers = {
                    **supabase_headers,
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates"
                }
                save_resp = requests.post(save_url, headers=save_headers, json=cache_payload)
                if save_resp.status_code not in [200, 201]:
                    print(f"      ⚠️ Cache save failed for {lat_r}, {lon_r}: {save_resp.status_code} - {save_resp.text}")
                    if "23505" in save_resp.text:
                         print("        (Duplicate found, resolution handled conflict)")

                # Also add to our local existing_cache so the join below can use it immediately
                existing_cache[(lat_r, lon_r)] = cache_payload

                print(f"      ✅ Geocoded and cached: {lat_r}, {lon_r} → {address.get('state', 'Unknown')}")
                time.sleep(random.uniform(1.1, 1.5))  # Nominatim: max 1 request/second

            except Exception as e:
                print(f"      ❌ Failed for {lat_r}, {lon_r}: {e}")

            # --- Step 5: Join the cache onto every row in df ---
        def resolve_address(lat, lon):
            if pd.isnull(lat) or pd.isnull(lon):
                return None
            lat_f, lon_f = float(lat), float(lon)
            if lat_f == 0.0 and lon_f == 0.0:
                return None
            key = (round(lat_f, 4), round(lon_f, 4))
            return existing_cache.get(key, {}).get("full_address")

        df["address"] = df.apply(lambda row: resolve_address(row.get("latitude"), row.get("longitude")), axis=1)
        print(f"    - Address enrichment complete.")

        # --- Step 6: Load enriched data into Supabase ---
        SupabaseLoader.load_data(table_name, df)

    @staticmethod
    def fetch_purchased_vehicle_items(table_name: str) -> pd.DataFrame: 
        print("Fetching the purchased vehicle items...")

        last_sync = get_last_sync_time(table_name)
        print(f" the last sync time is {last_sync}")
        query = f"SELECT * FROM {table_name} where createddate > :last_sync"
        result = AWSExtractor.fetch_source_data(query, params={"last_sync": last_sync})
        # load the result into the supabase
        SupabaseLoader.load_data(table_name, result)

    @staticmethod
    def fetch_purchased_items(table_name: str) -> pd.DataFrame: 
        print("Fetching the purchased items...")

        last_sync = get_last_sync_time(table_name)
        query = f"SELECT * FROM {table_name} where createddate > :last_sync"
        result = AWSExtractor.fetch_source_data(query, params={"last_sync": last_sync})
        # load the result into the supabase
        SupabaseLoader.load_data(table_name, result)

    @staticmethod
    def fetch_user_login_accounts(table_name: str) -> pd.DataFrame: 
        print("Fetching the user login accounts...")

        last_sync = get_last_sync_time(table_name)
        query = f"SELECT * FROM {table_name} where createddate > :last_sync"
        result = AWSExtractor.fetch_source_data(query, params={"last_sync": last_sync})

        
        # load the result into the supabase
        SupabaseLoader.load_data(table_name, result)

    @staticmethod
    #these are accounts that entered their NIN and BVN 
    def fetch_verified_customers(table_name: str) -> pd.DataFrame: 
        print("Fetching the accounts on the app who have verified with their NIN...")

        last_sync = get_last_sync_time(table_name)
        query = f"SELECT * FROM {table_name} where createddate > :last_sync"
        result = AWSExtractor.fetch_source_data(query, params={"last_sync": last_sync})
        # load the result into the supabase
        SupabaseLoader.load_data(table_name, result)

    
    @staticmethod
    def fetch_seller_data(table_name:str) -> pd.DataFrame: 
        print("Fetching the seller data...")
        # create the table in supabase
        get_last_sync_time(table_name)
        query = f"SELECT * FROM {table_name}"
        result = AWSExtractor.fetch_stagnant_data(query)
        # load the result into the supabase
        SupabaseLoader.load_data(table_name, result)

    @staticmethod
    def fetch_service_requests(table_name: str) -> pd.DataFrame: 
        print("Fetching the service requests...")
        # 1. Get the date from Supabase
        last_sync_value = get_last_sync_time(table_name)
        
        # 2. Define the query with a "seat" called :last_sync
        query = f"SELECT * FROM {table_name} WHERE createddate > :last_sync"
    
        # 3. Hand the value to the extractor
        # The key "last_sync" here matches the ":last_sync" above.
        result = AWSExtractor.fetch_source_data(query, params={"last_sync": last_sync_value})
        # load the result into the supabase
        SupabaseLoader.load_data(table_name, result)
