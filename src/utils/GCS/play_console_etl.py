
from google.cloud import storage
import pandas as pd
import io
import re 

def normalize_columns(df:pd.DataFrame) -> pd.DataFrame:
    """Normalize the columns of a DataFrame."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("/", "_", regex=False)
        .str.replace(r"_+", "_", regex=True)
    )
    return df

def extract_table_name(blob_name: str, package_name: str) -> str: 
    """ Dervive a clean table name from the GCS blob path. 
        stats/store_performance/com.dagconnect.app_2026-03-16_2026-03-23_country.csv -> store_perfomance_country
        Steps:
            1. Take just the filename  (drop the folder path)
            2. Strip the .csv extension
            3. Remove the package name  (e.g. _com.dagconnect)
            4. Remove the YYYYMM date   (e.g. _202603)
            5. Strip any leftover leading/trailing underscores
    """
    filename = blob_name.split("/")[-1]
    table_name = filename.replace(".csv", "")
    table_name = table_name.replace(f"_{package_name}", "")
    table_name = re.sub(r"_\d{6}", "", table_name)
    table_name = table_name.strip("_")
    return table_name

def read_file(client: storage.Client, bucket_name: str, file_path: str) -> pd.DataFrame:
    """Read a CSV file from GCS and return it as a DataFrame."""

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    raw = blob.download_as_bytes()

    for encoding in ('utf-16', "utf-8-sig", "utf-8"): 
        try: 
            df = pd.read_csv(io.BytesIO(raw), encoding=encoding)
            if not df.empty: 
                return normalize_columns(df)
        except Exception: 
            continue

    raise ValueError("Could not decode CSV with any of the tested encodings.")