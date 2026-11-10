import pandas as pd
import psycopg2
from airflow.hooks.base import BaseHook
from pathlib import Path
import sys

SCRIPTS_DIR = Path(f"/opt/airflow/data")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def gold_to_db():
    # Load the gold aggregated orders data
    gold_aggregated_orders = pd.read_csv("/opt/airflow/data/gold/aggregated_orders.csv")

    # Database connection parameters
    conn = BaseHook.get_connection('postgres_default')
    db_params = {
        'host': conn.host,
        'port': conn.port,
        'database': conn.schema,
        'user': conn.login,
        'password': conn.password
    }

    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    # Create the table if it doesn't exist
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS aggregated_orders (
            customer_id VARCHAR PRIMARY KEY,
            total_orders INT
        );
    '''
    cursor.execute(create_table_query)

    # Insert data into the table
    for index, row in gold_aggregated_orders.iterrows():
        insert_query = '''
            INSERT INTO aggregated_orders (customer_id, total_orders)
            VALUES (%s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET total_orders = EXCLUDED.total_orders;
        '''
        cursor.execute(insert_query, (row['customer_id'], row['total_orders']))

    # Commit the changes and close the connection
    connection.commit()
    cursor.close()
    connection.close()


