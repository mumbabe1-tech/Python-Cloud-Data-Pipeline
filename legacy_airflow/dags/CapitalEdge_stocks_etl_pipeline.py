from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# --- ORCHESTRATION ARCHITECTURE ---
# 1. ENVIRONMENT SYNCHRONIZATION: 
# This ensures that Airflow's worker environment can locate our custom Python modules 
# regardless of where the project is deployed, solving local pathing issues.
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# 2. MODULAR COMPONENT IMPORTS: 
# Importing the logic for our Medallion layers. This keeps the DAG file clean 
# and adheres to the 'Separation of Concerns' principle.
from extract_stocks_bronze import run_bronze
from transform_stocks_silver import run_silver
from load_stocks_gold import run_gold

# --- PIPELINE RELIABILITY SETTINGS ---
# 3. FAULT TOLERANCE: 
# Setting retries and delays ensures that minor network blips or API timeouts 
# don't break the entire daily run, addressing the 'Manual Error' problem statement.
default_args = {
    "owner": "airflow_admin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# --- DAG DEFINITION ---
with DAG(
    dag_id="stock_etl_pipeline_v2",
    default_args=default_args,
    description="CapitalEdge Analytics ETL: Bronze (JSON) -> Silver (Parquet) -> Gold (SQL)",
    
    # 4. SCHEDULING & AUTOMATION: 
    # Moving from manual extraction to a daily @schedule ensures 'Data Timeliness'.
    # Start_date and catchup=False prevent the system from trying to backfill 
    # historical data unnecessarily, saving on cloud costs.
    schedule="@daily", 
    start_date=datetime(2026, 4, 1), 
    catchup=False,
    tags=['capitaledge', 'azure', 'medallion']
) as dag:

    # 5. LAYER 1: RAW INGESTION (BRONZE)
    # The 'bronze_task' creates our landing zone in the cloud.
    # It addresses the 'Manual Data Extraction' problem by automating the API handshake.
    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze,
    )

    # 6. LAYER 2: VALIDATION & TRANSFORMATION (SILVER)
    # This task solves 'Data Inconsistency'. It enforces a schema and 
    # optimizes storage by converting JSON to Parquet format.
    silver_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_silver,
    )

    # 7. LAYER 3: BUSINESS LOGIC & LOADING (GOLD)
    # The final step for 'Incremental Data Loading'. It pushes curated data 
    # into our SQL Database for real-time executive dashboards.
    gold_task = PythonOperator(
        task_id="gold_load",
        python_callable=run_gold,
    )

    # 8. LINEAGE & DEPENDENCIES: 
    # Defining the directional flow. Airflow ensures that Silver never runs 
    # unless Bronze succeeds, maintaining strict 'Data Integrity'.
    bronze_task >> silver_task >> gold_task