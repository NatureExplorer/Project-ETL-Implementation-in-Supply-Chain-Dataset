from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/api_req')

from insert_rec import main


default_args = {
    'description':'DAG SUSTAINABILITY CORP',
    'owner': 'alam',
    'start_date': datetime(2026, 9, 5),
    'retries': 1,
}


with DAG(
    'upload_data', 
    default_args=default_args, 
    schedule=timedelta(minutes=5)):
    task1 = PythonOperator(
        task_id = 'ingest_data',
        python_callable = main
    )