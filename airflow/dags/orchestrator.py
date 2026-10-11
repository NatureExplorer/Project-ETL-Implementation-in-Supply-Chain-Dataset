from airflow.models import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import sys
from Kode.ET import main
from sqlalchemy import create_engine
import logging
import io
import glob
import pandas as pd

sys.path.append('/opt/airflow/train')



def load_files_to_DB(file_name, data_path):
    """
    function that inserts all transformed data into a Postgres database
    
    Args:
        file_name: name of file to copy to DB
        data_path: directory pointing to file
    
    """
    engine = create_engine(str(Variable.get("DB_CONN")))
    for f in glob.glob(data_path):
       filename = f.rsplit('/')[-1]
    if filename == file_name:
        df = pd.read_csv(data_path)
        #drops old table and creates new empty table using dataframe schema
        df.head(0).to_sql('prescriber_report', engine, if_exists='replace',index=False) 
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        try:
            cur.copy_from(output, 'prescriber_report', null="") # set null values to ''
        except:
            pass
    else:
        df = pd.read_csv(data_path)
        df.head(0).to_sql('city_report', engine, if_exists='replace',index=False) 
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        # skip any line which does not match the required field
        try:
            cur.copy_from(output, 'city_report', null="") # set null values to ''
        except:
            pass
    if conn is not None:
        conn.commit()
        cur.close()
        conn.close()
        print("data insertion to database is completed...")

def data_quality_checks(tables):
    """
    function that checks if data was loaded to database schema

    Args:
        tables: list of tables to perform data quality checks on 
    """
    tables = tables.split(',')
    for table in tables:
        engine = create_engine(CONN_STRING, pool_size=10, max_overflow=20)
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
    task1 = PythonOperator(
        task_id = 'ingest_data',
        python_callable = main
    )