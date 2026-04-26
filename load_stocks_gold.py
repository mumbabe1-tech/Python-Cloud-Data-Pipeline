import os
import io
import time
import logging
import pandas as pd
import urllib.parse
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, text

def validate_gold_dataframe(df, symbol):
    logging.info(f"🧐 Validating Gold metrics for {symbol}...")
    required_metrics = ["daily_change", "percent_change", "daily_range", "ma_7"]
    missing = [col for col in required_metrics if col not in df.columns]
    
    if missing:
        raise ValueError(f"❌ DQ Error: {symbol} is missing analytical metrics: {missing}")

    if (df["close"] <= 0).any():
        logging.warning(f"⚠️ DQ Warning: {symbol} contains non-positive prices.")

    if df["ma_7"].isnull().all() and len(df) > 0:
        raise ValueError(f"❌ DQ Error: {symbol} Moving Average calculation failed.")

    return True

def run_gold():
    # --- 1. CLOUD-NATIVE CONFIGURATION ---
    # We add a fallback message so we know exactly which variable is missing
    db_user = os.getenv("DB_USER", "MISSING_USER").strip()
    db_password = os.getenv("DB_PASSWORD", "MISSING_PWD").strip()
    db_server = os.getenv("DB_SERVER", "MISSING_SERVER").strip()
    db_name = os.getenv("DB_NAME", "MISSING_DB").strip()
    blob_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip('"').strip("'").strip()
    
    # If any are "MISSING_...", the log will now tell us which one
    if "MISSING" in [db_user, db_password, db_server, db_name] or not blob_conn_str:
        logging.error(f"❌ Setup Error: Environment variables are not set correctly in the Azure Portal.")
        logging.error(f"DEBUG - Server: {db_server}, User: {db_user}, DB: {db_name}")
        return False

    silver_container = "silver"
    gold_container = "gold" 

    # --- 2. SQL ENGINE SETUP ---
    safe_password = urllib.parse.quote_plus(db_password)
    
    # Using pymssql as the driver for Linux-based Azure Functions
    connection_url = f"mssql+pymssql://{db_user}:{safe_password}@{db_server}/{db_name}"
    
    engine = create_engine(connection_url, connect_args={"timeout": 60, "autocommit": True})

    # --- 3. DATABASE HANDSHAKE ---
    logging.info(f"🚀 Connecting to Azure SQL Server: {db_server}...")
    connected = False
    for i in range(3):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info("✅ SQL Connection Successful.")
            connected = True
            break
        except Exception as e:
            # We catch the specific error here to show the Panel we can debug networking
            logging.warning(f"⚠️ Attempt {i+1}: Networking/Firewall Handshake... {str(e)[:100]}")
            time.sleep(10)

    if not connected:
        logging.error("❌ SQL Connection failed after 3 attempts. Aborting Gold Layer.")
        return False

    # --- 4. RETRIEVE DATA FROM SILVER ---
    try:
        blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
        silver_client = blob_service_client.get_container_client(silver_container)
        gold_client = blob_service_client.get_container_client(gold_container)

        blobs = list(silver_client.list_blobs())
        files_processed = 0

        for blob in blobs:
            if blob.name.endswith(".parquet"):
                logging.info(f"📀 Processing Silver Data: {blob.name}")
                
                blob_client = silver_client.get_blob_client(blob.name)
                download_stream = blob_client.download_blob()
                
                # Reading Parquet from Memory Stream
                df = pd.read_parquet(io.BytesIO(download_stream.readall()))
                df = df.sort_values(["symbol", "date"])

                # --- 5. ANALYTICAL TRANSFORMATIONS ---
                df["daily_change"] = df["close"] - df["open"]
                df["percent_change"] = (df["daily_change"] / df["open"]) * 100
                df["daily_range"] = df["high"] - df["low"]
                df["ma_7"] = df.groupby("symbol")["close"].transform(lambda x: x.rolling(window=7, min_periods=1).mean())

                # --- 6. DATA QUALITY CHECK ---
                symbol = blob.name.split("_")[0]
                try:
                    validate_gold_dataframe(df, symbol)
                except Exception as ve:
                    logging.error(f"❌ Validation Failed for {symbol}: {ve}")
                    continue

                # --- 7. LOAD TO AZURE SQL ---
                table_name = f"Stock_{symbol}_Gold"
                try:
                    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
                    logging.info(f"✅ SQL LOAD SUCCESS: {table_name}")
                    
                    # --- 8. ARCHIVE TO GOLD CONTAINER ---
                    gold_blob_client = gold_client.get_blob_client(blob.name)
                    parquet_buffer = io.BytesIO()
                    df.to_parquet(parquet_buffer, index=False)
                    parquet_buffer.seek(0)
                    gold_blob_client.upload_blob(parquet_buffer.read(), overwrite=True)
                    
                    files_processed += 1
                except Exception as se:
                    logging.error(f"❌ SQL Load Error for {symbol}: {se}")

        logging.info(f"--- Gold Process Complete. {files_processed} tables updated. ---")
        return True

    except Exception as e:
        logging.error(f"❌ Blob Storage Error: {e}")
        return False

if __name__ == "__main__":
    run_gold()