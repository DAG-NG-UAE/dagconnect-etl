from src.loader.database import SupabaseLoader
from google_play_scraper import app
from src.utils.supabase_connector import get_last_sync_time
import pandas as pd
from datetime import datetime

class PlayStoreExtractor:
    @staticmethod
    def fetch_app_stats(package_id: str):
        print(f"Fetching Play Store data for {package_id}...")
        
        result = app(package_id, lang='en', country='ng') # 'ng' for Nigeria
        
        # We only want the high-level metrics for the dashboard
        stats = {
            "package_id": package_id,
            "title": result['title'],
            "installs": result['installs'],          # e.g., "1,000+"
            "min_installs": result['minInstalls'],    # e.g., 1000 (The real number)
            "score": result['score'],                 # Average Rating
            "ratings": result['ratings'],             # Total count of ratings
            "reviews": result['reviews'],             # Total count of reviews
            "fetched_at": datetime.now().isoformat()
        }

        # we want to check if the table has already existed
        last_sync_time = get_last_sync_time("play_store_stats")
        print(f"Last sync time: {last_sync_time}")

        SupabaseLoader.load_data("play_store_stats", pd.DataFrame([stats]))
        
        return