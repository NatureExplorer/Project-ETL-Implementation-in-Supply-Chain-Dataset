import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow.decorators import dag, task

SCRIPTS_DIR = Path("/opt/airflow/scripts")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from Silver_layer import silver_transform_orders
from Gold_layer import gold_layer_orders
from load_db import gold_to_db


@dag(
    dag_id="orders_medallion_pipe",
    start_date=datetime(2026, 11, 9),
    schedule="*/30 * * * *",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ETL", "Medallion Architecture"],
)
def orders_medallion_pipe():

    @task
    def silver_transform_orders_task():
        silver_transform_orders()

    @task
    def gold_layer_orders_task():
        gold_layer_orders()

    @task
    def gold_to_db_task():
        gold_to_db()

    silver_task = silver_transform_orders_task()
    gold_task = gold_layer_orders_task()
    db_task = gold_to_db_task()

    silver_task >> gold_task >> db_task


orders_medallion_pipe()
