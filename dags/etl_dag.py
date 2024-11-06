from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
from datetime import datetime, timedelta
import sys

# Add the path where etl.py is located
sys.path.append('/app')  # Adjust if necessary

from etl import main  # Import the main function from your ETL script

def run_etl():
    main()  # Execute the main function from the ETL script

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ecommerce_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust based on your needs
    catchup=False,
)

# Define the task that runs the ETL process
run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag,
)

run_etl_task
