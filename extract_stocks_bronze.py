import os
import requests
import json
import logging
from datetime import datetime
from azure.storage.blob import BlobServiceClient

def run_bronze():
    """
    PURPOSE: Ingestion layer. Captures raw data from AlphaVantage API 
    and lands it in the Bronze container of Azure Blob Storage.
    """
    
    # 1. SECURE CONFIGURATION
    # Values are pulled from local.settings.json (local) or Azure App Settings (cloud)
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    
    if not api_key or not connect_str:
        logging.error("❌ Setup Error: Missing credentials in local.settings.json")
        return

    # 2. TARGETING & METADATA
    symbols = ["AAPL", "AMZN", "GOOGL", "MSFT", "TSLA"]
    date_str = datetime.now().strftime("%Y%m%d")
    container_name = "bronze" 

    # 3. DATA LAKE CONNECTIVITY
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(container_name)
    except Exception as e:
        logging.error(f"❌ Connection Error: Could not connect to Azure Storage: {e}")
        return

    # 4. EXTRACTION LOOP
    for symbol in symbols:
        blob_name = f"{symbol}_{date_str}.json"
        blob_client = container_client.get_blob_client(blob_name)

        # 5. INCREMENTAL LOADING CHECK
        # Avoids redundant API calls if data for today already exists
        if blob_client.exists():
            logging.info(f"⏭️ Skipping {symbol}: {blob_name} already exists.")
            continue

        # 6. API EXTRACTION
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        logging.info(f"📡 Requesting financial data for {symbol}...")
        
        try:
            response = requests.get(url)
            data = response.json()

            # 7. DATA INTEGRITY & PERSISTENCE
            if "Time Series (Daily)" in data:
                # RAW PERSISTENCE: Save JSON exactly as received
                blob_client.upload_blob(json.dumps(data), overwrite=True)
                logging.info(f"✅ Success: {symbol} persisted to Bronze.")
            
            elif "Note" in data:
                # Handle AlphaVantage's standard 5 calls/minute limit
                logging.warning(f"⚠️ API Limit Hit for {symbol}: {data['Note']}")
            
            else:
                logging.error(f"❌ Data Error: Unexpected response format for {symbol}")
        
        except Exception as e:
            logging.error(f"❌ Processing Error: Failed to ingest {symbol}: {str(e)}")

# This allows for manual testing without running the whole Function App
if __name__ == "__main__":
    run_bronze()