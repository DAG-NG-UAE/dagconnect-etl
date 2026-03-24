# save as: check_access.py
import os
from google.oauth2 import service_account
from google.cloud import storage

credentials = service_account.Credentials.from_service_account_file(
    os.getenv("GCP_SERVICE_ACCOUNT_KEY", "service-account.json"),
    scopes=[
        "https://www.googleapis.com/auth/androidpublisher",
        "https://www.googleapis.com/auth/devstorage.read_only",
    ],
)
client = storage.Client(credentials=credentials)

try:
    blobs = list(client.list_blobs("pubsite_prod_8540944584425013860", max_results=1))
    print("✅ ACCESS GRANTED — you can now run the full ETL!")
except Exception as e:
    print(f"❌ Not yet: {e}")