import pandas as pd
import sys
from pathlib import Path

SCRIPTS_DIR = Path(f"/opt/airflow/data")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

gold_path = Path("/opt/airflow/data/gold")
gold_path.mkdir(parents=True, exist_ok=True)


def gold_layer_orders():
    # Load the silver orders data
    silver_orders = pd.read_csv("/opt/airflow/data/silver/orders.csv")

    # Perform transformations to create the gold orders data
    gold_orders = silver_orders.copy()

    aggregated_orders = gold_orders.groupby('customer_id').agg(
        total_orders=('order_id', 'count')
    ).reset_index()

    aggregated_orders.to_csv('/opt/airflow/data/gold/aggregated_orders.csv', index=False)