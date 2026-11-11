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
    gold_aggregated_revenue = pd.read_csv("/opt/airflow/data/gold/aggregated_revenue_bycategory.csv")
    gold_aggregated_city = pd.read_csv("/opt/airflow/data/gold/aggregated_city_byorder.csv")
    gold_ratio_shipprice = pd.read_csv("/opt/airflow/data/gold/ratio_shipandprice_bycity.csv")

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
            id SERIAL PRIMARY KEY,
            order_purchase_timestamp DATE,
            total_orders INT,
            total_revenue DECIMAL(10,2)
        );
        CREATE TABLE IF NOT EXISTS aggregated_revenue_bycategory (
            id SERIAL PRIMARY KEY,
            product_category_name VARCHAR PRIMARY KEY,
            total_orders INT,
            total_revenue DECIMAL(10,2)
        );
        
        CREATE TABLE IF NOT EXISTS aggregated_city_byorder (
            id SERIAL PRIMARY KEY,
            customer_city VARCHAR PRIMARY KEY,
            total_orders INT,
            total_revenue DECIMAL(10,2)
        );

        CREATE TABLE IF NOT EXISTS ratio_shipandprice_bycity (
            id SERIAL PRIMARY KEY,
            customer_city VARCHAR PRIMARY KEY,
            total_orders INT,
            total_revenue DECIMAL(10,2),
            total_shipping_charges DECIMAL(10,2),
            ratio_shipping_price DECIMAL(10,2)
        );
    '''
    cursor.execute(create_table_query)

    # Insert data into the table
    for _, row in gold_aggregated_orders.iterrows():
        insert_query = '''
            INSERT INTO aggregated_orders (order_purchase_timestamp, total_orders, total_revenue)
            VALUES (%s, %s, %s)
            ON CONFLICT (order_purchase_timestamp) DO UPDATE SET total_orders = EXCLUDED.total_orders, total_revenue = EXCLUDED.total_revenue;
        '''
        cursor.execute(insert_query, (row['order_purchase_timestamp'], row['total_orders'], row['total_revenue']))

    for _, row in gold_aggregated_revenue.iterrows():
        insert_query = '''
            INSERT INTO aggregated_revenue_bycategory (product_category_name, total_orders, total_revenue)
            VALUES (%s, %s, %s)
            ON CONFLICT (product_category_name) DO UPDATE SET total_orders = EXCLUDED.total_orders, total_revenue = EXCLUDED.total_revenue;
        '''
        cursor.execute(insert_query, (row['product_category_name'], row['total_orders'], row['total_revenue']))

    for _, row in gold_aggregated_city.iterrows():
        insert_query = '''
            INSERT INTO aggregated_city_byorder (customer_city, total_orders, total_revenue)
            VALUES (%s, %s, %s)
            ON CONFLICT (customer_city) DO UPDATE SET total_orders = EXCLUDED.total_orders, total_revenue = EXCLUDED.total_revenue;
        '''
        cursor.execute(insert_query, (row['customer_city'], row['total_orders'], row['total_revenue']))

    for _, row in gold_ratio_shipprice.iterrows():
        insert_query = '''
            INSERT INTO ratio_shipandprice_bycity (customer_city, total_orders, total_revenue, total_shipping_charges, ratio_shipping_price)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (customer_city) DO UPDATE SET total_orders = EXCLUDED.total_orders, total_revenue = EXCLUDED.total_revenue, total_shipping_charges = EXCLUDED.total_shipping_charges, ratio_shipping_price = EXCLUDED.ratio_shipping_price;
        '''
        cursor.execute(insert_query, (row['customer_city'], row['total_orders'], row['total_revenue'], row['total_shipping_charges'], row['ratio_shipping_price']))

    # Commit the changes and close the connection
    connection.commit()
    cursor.close()
    connection.close()


