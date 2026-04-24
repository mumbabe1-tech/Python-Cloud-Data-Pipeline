import azure.functions as func
import logging

# Connect the Brain to ALL Workers
from extract_stocks_bronze import run_bronze
from transform_stocks_silver import run_silver
from load_stocks_gold import run_gold

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 8 * * *", arg_name="myTimer", run_on_startup=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    logging.info('🚀 CapitalEdge Orchestrator starting...')

    try:
        logging.info('Step 1: Extracting to Bronze...')
        run_bronze()
        
        logging.info('Step 2: Transforming to Silver...')
        run_silver()
        
        logging.info('Step 3: Loading to Gold (SQL)...')
        run_gold()
        
        logging.info('✅ Full Pipeline Finished Successfully.')
    except Exception as e:
        logging.error(f"❌ Pipeline Failed at some stage: {e}")