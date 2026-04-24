import os
import pymssql
import logging
# We still use this for local testing if you have a .env, 
# though local.settings.json is our primary goal now!
from dotenv import load_dotenv 

# 1. LOAD CONFIGURATION
# This will try to find your credentials
load_dotenv() 

def test_pymssql_connection():
    """
    Diagnostic tool to verify Azure SQL connectivity using the 
    same driver as our production Gold Layer.
    """
    # 2. EXTRACT CREDENTIALS
    # These names should match what you have in local.settings.json or .env
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    print(f"--- 🔎 Connection Diagnostic ---")
    print(f"Target Server: {server}")
    print(f"Target Database: {database}")

    try:
        print("\n🚀 Attempting handshake with Azure SQL...")
        
        # 3. THE CONNECTION
        conn = pymssql.connect(
            server=server, 
            user=user, 
            password=password, 
            database=database,
            timeout=10  # Don't wait forever if it fails
        )
        
        print("✅ SUCCESS: Your credentials and Azure Firewall are configured correctly!")
        
        # Quick query test
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION;")
        row = cursor.fetchone()
        print(f"Connected to: {row[0][:50]}...")
        
        conn.close()

    except Exception as e:
        print(f"❌ CONNECTION FAILED")
        print(f"Error Detail: {str(e)}")
        print("\n💡 DE Check: Is your IP address whitelisted in the Azure SQL Networking tab?")

if __name__ == "__main__":
    test_pymssql_connection()