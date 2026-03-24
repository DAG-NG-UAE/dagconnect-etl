
from src.loader.database import SupabaseLoader
from src.extractor.aws_db import AWSExtractor
from src.extractor.gcs import GCSExtractor
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_run():
    logger.info("--- Starting ETL Test Run ---")
    try:
        # 1. AWS RDS SYNC
        AWSExtractor.daily_aws_fetch()

        # 2. GCS SYNC
        GCSExtractor.begin_fetch()
        
        logger.info("--- ALL SYSTEMS SYNCED🍻 ---")
        # 3. Record successful run
        SupabaseLoader.record_etl_run("passed")
             
    except Exception as e:
        logger.error(f"Test run failed at the main level: {e}")
        # Record failed run
        SupabaseLoader.record_etl_run("failed", str(e))

if __name__ == "__main__":
    test_run()