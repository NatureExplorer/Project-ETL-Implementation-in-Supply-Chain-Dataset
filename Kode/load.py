import csv
import glob
import tempfile
from psycopg2 import sql
from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, BooleanType, DateType,
    TimestampType, DecimalType, BinaryType)
import logging
from sqlalchemy import create_engine
from airflow.models import Variable
import os


def get_pg_connection():
    """koneksi ke PostgreSQL."""
    engine = create_engine(str(Variable.get("DB_CONN")))
    return engine


def spark_to_postgres_type(data_type):
    """tipe data PySpark menjadi tipe data PostgreSQL."""

    if isinstance(data_type, StringType):
        return "TEXT"
    elif isinstance(data_type, (ByteType, ShortType, IntegerType)):
        return "INTEGER"
    elif isinstance(data_type, LongType):
        return "BIGINT"
    elif isinstance(data_type, FloatType):
        return "REAL"
    elif isinstance(data_type, DoubleType):
        return "DOUBLE PRECISION"
    elif isinstance(data_type, BooleanType):
        return "BOOLEAN"
    elif isinstance(data_type, DateType):
        return "DATE"
    elif isinstance(data_type, TimestampType):
        return "TIMESTAMP"
    elif isinstance(data_type, DecimalType):
        return f"NUMERIC({data_type.precision},{data_type.scale})"
    elif isinstance(data_type, BinaryType):
        return "BYTEA"
    else:
        raise TypeError(
            f"Tipe data PySpark belum didukung: {data_type}"
        )


def create_table_from_spark_df(engine, df, table_name):
    """tabel PostgreSQL berdasarkan skema DataFrame."""

    columns = [
        sql.SQL("{} {}").format(
            sql.Identifier(field.name),
            sql.SQL(spark_to_postgres_type(field.dataType))
        )
        for field in df.schema.fields
    ]

    create_query = sql.SQL(
        "CREATE TABLE IF NOT EXISTS public.{} ({})"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(columns)
    )

    conn = engine.raw_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(create_query)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def load_spark_df_to_postgres(engine, df, table_name):
    """DataFrame PySpark ke tabel PostgreSQL."""

    column_names = df.columns

    copy_query = sql.SQL(
        r"COPY public.{} ({}) FROM STDIN WITH "
        r"(FORMAT CSV, DELIMITER E'\t', NULL '\N')"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(
            sql.Identifier(col) for col in column_names
        )
    )

    conn = engine.raw_connection()

    try:
        with conn.cursor() as cur:
            with tempfile.TemporaryFile(
                mode="w+t",
                encoding="utf-8",
                newline=""
            ) as output:

                writer = csv.writer(
                    output,
                    delimiter="\t",
                    quoting=csv.QUOTE_MINIMAL,
                    lineterminator="\n"
                )

                for row in df.toLocalIterator():
                    writer.writerow([
                        r"\N" if value is None else value
                        for value in row
                    ])

                output.seek(0)
                cur.copy_expert(copy_query.as_string(conn), output)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def data_quality_checks(engine, tables):

    if isinstance(tables, str):
        tables = [table.strip() for table in tables.split(",") if table.strip()]

    conn = engine.raw_connection()

    try:
        with conn.cursor() as cursor:
            for table in tables:
                query = sql.SQL(
                    "SELECT COUNT(*) FROM {}"
                ).format(sql.Identifier(table))

                cursor.execute(query)
                result = cursor.fetchone()

                if result is None:
                    raise ValueError(
                        f"Data quality check failed. "
                        f"{table} returned no result."
                    )

                num_records = result[0]

                if num_records < 1:
                    raise ValueError(
                        f"Data quality check failed. "
                        f"{table} contained 0 rows."
                    )

                logging.info(
                    "Data quality check passed for table %s: %s records",
                    table,
                    num_records
                )

    finally:
        conn.close()

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

def main(data_path, table_name):

    engine = get_pg_connection()

    try:
        logging.info(f'create and load the table in database')
        create_table_from_spark_df(engine, data_path, table_name)
        load_spark_df_to_postgres(engine, data_path, table_name)

    finally:
        engine.dispose
