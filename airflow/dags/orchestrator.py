from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
from Kode.load import main, data_quality_checks, cleaning_process
import logging
import glob
import os

sys.path.append('/opt/airflow/train')


def data_quality_checks(tables):
    """
    function that checks if data was loaded to database schema

    Args:
        tables: list of tables to perform data quality checks on 
    """
    tables = tables.split(',')
    for table in tables:
        engine = engine.connect()
        conn = engine.raw_connection()
        cursor = conn.cursor()
        query_records = '''SELECT COUNT(*) FROM {table}'''
        if len(query_records) < 1 or len(query_records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no result.")
        logging.info(f"Data quality on table {table} check passed with {len(query_records)} number of records")
        num_records = query_records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        logging.info(f"Data quality on table {table} check passed with {query_records[0][0]} number of records")

def cleaning_process():
    """
    This functions cleans the process after pipeline has succeeded
    Input:
        None
    Output:
        None
    """
    staging_path = '/opt/airflow/staging'
    for f in glob.glob(staging_path):
        if f.rsplit('/')[-1].endswith('.csv'):
            os.remove(f)
    logging.info("cleaning process completed successfully...")

    

default_args = {
    'description':'DAG Order retail',
    'owner': 'alam',
    'start_date': datetime.now(),
    'retries': 1,
}


with DAG(
    'Order_pipeline', 
    default_args=default_args, 
    schedule=timedelta(minutes=5)):

    tugas_et = BashOperator(
        task_id = 'run extract and transform spark',
        bash_command = 'python3.11 /opt/airflow/Kode/spark_etl.py',
    )

    transform_customer = FileSensor(
        task_id = 'Transform customer data',
        fs_conn_id= 'customer_default',
        filepath = '/opt/airflow/staging/customers.csv',
        poke_interval=10,
    )

    transform_orderitems = FileSensor(
            task_id = 'Transform order items data',
            fs_conn_id= 'orderitems_default',
            filepath = '/opt/airflow/staging/order_items.csv',
            poke_interval=10,
        )

    transform_orders = FileSensor(
            task_id = 'Transform orders data',
            fs_conn_id= 'orders_default',
            filepath = '/opt/airflow/staging/orders.csv',
            poke_interval=10,
        )

    transform_payments = FileSensor(
            task_id = 'Transform payments data',
            fs_conn_id= 'payments_default',
            filepath = '/opt/airflow/staging/payments.csv',
            poke_interval=10,
        )

    transform_products = FileSensor(
            task_id = 'Transform products data',
            fs_conn_id= 'products_default',
            filepath = '/opt/airflow/staging/products.csv',
            poke_interval=10,
        )

    loading_customers_to_Postgres_task = PythonOperator(
        task_id = 'loading_customers_report_to_postgres',
        python_callable=main,
        op_kwargs={
            'data_path': '/opt/airflow/staging/customers.csv',
            'table_name':'customers',
            }, 
    )

    loading_OrderItems_report_to_Postgres_task = PythonOperator(
        task_id = 'loading_OrderItems_report_to_postgres',
        python_callable=main,
        op_kwargs={
            'data_path': '/opt/airflow/staging/order_items.csv',
            'table_name':'orderitems',
            },  )

    loading_orders_report_to_Postgres_task = PythonOperator(
            task_id = 'loading_orders_report_to_postgres',
            python_callable=main,
            op_kwargs={
                'data_path': '/opt/airflow/staging/orders.csv',
                'table_name':'orders',
                },  )

    loading_payments_report_to_Postgres_task = PythonOperator(
            task_id = 'loading_payments_report_to_postgres',
            python_callable=main,
            op_kwargs={
                'data_path': '/opt/airflow/staging/payments.csv',
                'table_name':'payments',
                },  )

    loading_products_report_to_Postgres_task = PythonOperator(
            task_id = 'loading_products_report_to_postgres',
            python_callable=main,
            op_kwargs={
                'data_path': '/opt/airflow/staging/products.csv',
                'table_name':'products',
                },  )

    data_quality_check_task = PythonOperator(
        task_id = 'data_quality_checks',
        python_callable=data_quality_checks,
        op_kwargs={'tables': 'customers, orderitems ,orders, payments, products'},
        
    )

    start_execution_task = EmptyOperator(
        task_id = 'start_execution',
        
    )

    transformed_data_ready_task = EmptyOperator(
        task_id = 'transformed_data_ready',
        
    )

    loaded_data_ready_task = EmptyOperator(
        task_id = 'loaded_data_ready',
        
    )

    clean_up_task = PythonOperator(
        task_id = 'clean_up_process',
        python_callable=cleaning_process,
    )

    end_execution_task = EmptyOperator(
        task_id = 'end_execution',
        
    )

    start_execution_task >> tugas_et >> [transform_customer, 
                                                    transform_orderitems, transform_orders, transform_payments, transform_products]

    [transform_customer, 
    transform_orderitems, transform_orders, transform_payments, transform_products] >> transformed_data_ready_task
    transformed_data_ready_task >> [loading_customers_to_Postgres_task,
                                    loading_OrderItems_report_to_Postgres_task,
                                    loading_orders_report_to_Postgres_task,
                                    loading_payments_report_to_Postgres_task,
                                    loading_products_report_to_Postgres_task,
                                    ]
    [loading_customers_to_Postgres_task,
     loading_OrderItems_report_to_Postgres_task,
     loading_orders_report_to_Postgres_task,
     loading_payments_report_to_Postgres_task,
     loading_products_report_to_Postgres_task,
     ] >> loaded_data_ready_task >> data_quality_check_task

    data_quality_check_task >> clean_up_task >> end_execution_task
    
    

    