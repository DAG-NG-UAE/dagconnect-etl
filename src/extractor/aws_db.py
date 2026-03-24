import pandas as pd
from src.utils.db_connector import DBConnector
from src.utils.supabase_connector import get_last_sync_time
import logging
from sqlalchemy import text
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
