import json
import os
import io
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient

def validate_silver_data(df, blob_name):
    """
    INTERNAL DATA QUALITY CHECK:
    Ensures the transformation produced a valid, clean dataset.
    """
    if df is None or df.empty:
        raise ValueError(f"DQ Error: Dataframe for {blob_name} is empty.")

    required_columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
    
    # Check for missing columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"DQ Error: {blob_name} missing columns: {missing_cols}")

    # Check for nulls in critical price data
    if df[required_columns].isnull().any().any():
        logging.warning(f"⚠️ DQ Warning: Null values found in {blob_name}. Cleaning...")
        df = df.dropna(subset=required_columns)

    return df

def run_silver():
    """
    PURPOSE: Transforms raw JSON from Bronze to cleaned Parquet in Silver.
    """
    # 1. AUTHENTICATION (Azure Native)
    connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connect_str:
        logging.error("❌ Storage Connection String not found.")
        return

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    # 2. STORAGE TARGETS
    # We read from Bronze and write to Silver
    bronze_container = "bronze"
    silver_container = "silver"
    
    bronze_client = blob_service_client.get_container_client(bronze_container)
    silver_client = blob_service_client.get_container_client(silver_container)

    logging.info("🚀 Starting Silver Transformation...")
    
    # 3. DATA DISCOVERY
    blobs = bronze_client.list_blobs()
    files_processed = 0

    for blob in blobs:
        if blob.name.lower().endswith(".json"):
            logging.info(f"🔍 Processing: {blob.name}")
            
            # Extract symbol from filename (e.g., AAPL_20260424.json)
            symbol = blob.name.split('_')[0]
            blob_client = bronze_client.get_blob_client(blob)
            
            try:
                # 4. IN-MEMORY PROCESSING
                download_stream = blob_client.download_blob()
                data = json.loads(download_stream.readall())

                # API Error Check
                if "Note" in data or "Information" in data:
                    logging.warning(f"⏭️ Skipping {blob.name}: API Rate Limit found.")
                    continue

                ts = data.get("Time Series (Daily)", {})
                if not ts:
                    logging.error(f"❌ Skipping {blob.name}: No Time Series data.")
                    continue

                # 5. DATA STRUCTURING
                rows = []
                for date_str, values in ts.items():
                    rows.append({
                        "date": date_str,
                        "open": values.get("1. open"),
                        "high": values.get("2. high"),
                        "low": values.get("3. low"),
                        "close": values.get("4. close"),
                        "volume": values.get("5. volume"),
                        "symbol": symbol
                    })

                # 6. SCHEMA ENFORCEMENT
                df = pd.DataFrame(rows)
                df["date"] = pd.to_datetime(df["date"])
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

                # 7. INTERNAL VALIDATION (DQ CHECK)
                df = validate_silver_data(df, blob.name)

                # 8. STORAGE OPTIMIZATION (JSON -> Parquet)
                # Change the folder/extension for the Silver layer
                silver_blob_name = blob.name.replace(".json", ".parquet").replace(".JSON", ".parquet")
                
                # 9. CLOUD PERSISTENCE
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False, engine='pyarrow') 
                parquet_buffer.seek(0) 

                # Upload to the SILVER container
                target_blob_client = silver_client.get_blob_client(silver_blob_name)
                target_blob_client.upload_blob(parquet_buffer.read(), overwrite=True)
                
                logging.info(f"✅ Created Silver: {silver_blob_name}")
                files_processed += 1

            except Exception as e:
                logging.error(f"❗ Error processing {blob.name}: {e}")

    logging.info(f"--- Transformation Complete: {files_processed} files moved to Silver ---")

if __name__ == "__main__":
    run_silver()