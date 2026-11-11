import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow.decorators import dag, task

SCRIPTS_DIR = Path("/opt/airflow/scripts")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from Silver_layer import table_silver_transaction
from Gold_layer import agregated_order_bytime, agregated_revenue_bycategory, agregated_city_byorder, ratio_shipandprice_bycity
from load_db import gold_to_db


@dag(
    dag_id="orders_medallion_pipe",
    start_date=datetime(2026, 11, 9),
    schedule="*/30 * * * *",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ETL", "Medallion Architecture"],
)
def orders_medallion_pipe():

    @task()
    def silver_layer():
        table_silver_transaction()

    @task()
    def gold_layer():
        agregated_order_bytime()
        agregated_revenue_bycategory()
        agregated_city_byorder()
        ratio_shipandprice_bycity()

    @task()
    def load_to_db():
        gold_to_db()


    silver_layer() >> gold_layer() >> load_to_db()

orders_medallion_pipe()
