import pandas as pd
import sys
from pathlib import Path

SCRIPTS_DIR = Path(f"/opt/airflow/data")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

silver_path = Path("/opt/airflow/data/silver")
silver_path.mkdir(parents=True, exist_ok=True)


# def silver_transform_customers():
#     # Load the bronze customers data
#     bronze_customers = pd.read_csv('/opt/airflow/data/df_Customers.csv')

#     # Perform transformations to create the silver customers data
#     silver_customers = bronze_customers.copy()
#     silver_customers['customer_id'] = silver_customers['customer_id'].astype(str)
#     silver_customers['customer_id'] = silver_customers['customer_id'].drop_duplicates()  # delete duplicate
    

#     # Save the silver customers data
#     silver_customers.to_csv('/opt/airflow/data/silver/customers.csv', index=False)

def silver_transform_orders():
    # Load the bronze orders data
    bronze_orders = pd.read_csv('/opt/airflow/data/df_Orders.csv')

    # Perform transformations to create the silver orders data
    silver_orders = bronze_orders.copy()
    silver_orders['order_id'] = silver_orders['order_id'].astype(str)
    silver_orders['customer_id'] = silver_orders['customer_id'].astype(str)
    silver_orders['order_purchase_timestamp'] = pd.to_datetime(silver_orders['order_purchase_timestamp'])
    silver_orders['order_id'] = silver_orders['order_id'].drop_duplicates()  # delete duplicate

    # Save the silver orders data
    silver_orders.to_csv('/opt/airflow/data/silver/orders.csv', index=False)

# def silver_transform_orders_item():
#     # Load the bronze order items data
#     bronze_order_items = pd.read_csv('/opt/airflow/data/df_OrderItems.csv')

#     # Perform transformations to create the silver order items data
#     silver_order_items = bronze_order_items.copy()
#     silver_order_items['order_id'] = silver_order_items['order_id'].astype(str)
#     silver_order_items['product_id'] = silver_order_items['product_id'].astype(str)
#     silver_order_items['order_item_id'] = silver_order_items['order_item_id'].drop_duplicates()  # delete duplicate

#     # Save the silver order items data
#     silver_order_items.to_csv('/opt/airflow/data/silver/order_items.csv', index=False)

# def silver_transform_products():
#     # Load the bronze products data
#     bronze_products = pd.read_csv('/opt/airflow/data/df_Payments.csv')

#     # Perform transformations to create the silver products data
#     silver_products = bronze_products.copy()
#     silver_products['product_id'] = silver_products['product_id'].astype(str)
#     silver_products['product_id'] = silver_products['product_id'].drop_duplicates()  # delete duplicate

#     # Save the silver products data
#     silver_products.to_csv('/opt/airflow/data/silver/products.csv', index=False)

# def silver_transform_payments():
#     # Load the bronze payments data
#     bronze_payments = pd.read_csv('/opt/airflow/data/df_Products.csv')

#     # Perform transformations to create the silver payments data
#     silver_payments = bronze_payments.copy()
#     silver_payments['payment_id'] = silver_payments['payment_id'].astype(str)
#     silver_payments['order_id'] = silver_payments['order_id'].astype(str)
#     silver_payments['payment_id'] = silver_payments['payment_id'].drop_duplicates()  # delete duplicate

#     # Save the silver payments data
#     silver_payments.to_csv('/opt/airflow/data/silver/payments.csv', index=False)