from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import sys
    

sys.path.append('./Dataset/Ecommerce/train')


def sesi():
    spark = SparkSession.builder \
        .appName("ETL ESupply Chain") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g")\
        .config("spark.sql.shuffle.partitions", "4")\
        .getOrCreate()
    return spark

def transform_cust(spark, file_path):
    try:
        logging.info("extract and transforming Customer Data ....")
        load_path = '/opt/airflow/staging/customers.csv'
        df_cust = spark.read.option('delimiter',',').option('header', True).csv(file_path)
        df_cust  = df_cust.withColumn('customer_zip_code_prefix', F.col('customer_zip_code_prefix').cast("integer"))

        df_cust = df_cust.filter((F.col('customer_id').isNotNull()))

        df_cust.write.csv(load_path)

    except Exception as e:
        logging.error(f"Error reading customer data: {e}")
    else:
        logging.info("Customer Data transformed successfully.")

        return df_cust

def transform_oi(spark,file_path):
    try:
        logging.info("extract and transforming Order Item Data ....")
        load_path = '/opt/airflow/staging/order_items.csv'
        df_oi = spark.read.option('delimiter',',').option('header', True).csv(file_path)
        df_oi = df_oi.withColumn("price", F.col("price").cast("double"))
        df_oi = df_oi.withColumn("shipping_charges", F.col("shipping_charges").cast("double"))
        
        df_oi = df_oi.withColumn('total charge', F.col('shipping_charges') + F.col('price'))

        df_oi = df_oi.filter((F.col('order_id').isNotNull()) & (F.col('product_id').isNotNull())
                             & (F.col('seller_id').isNotNull()))

        df_oi.toPandas().csv(load_path)

    except Exception as e:
        logging.error(f"Error reading order item data: {e}")
    else:
        logging.info("Order Item Data transformed successfully.")

        return df_oi

def transform_order(spark,file_path):
    try:
        logging.info("extract and transforming Order Data ....")
        load_path = '/opt/airflow/staging/orders.csv'
        df_order = spark.read.option('delimiter',',').option('header', True).csv(file_path)
        df_order = df_order.dropna(subset='order_id')

        df_order = (
            df_order.withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss")),
            df_order.withColumn("order_approved_at", F.to_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss")),
            df_order.withColumn("order_delivered_timestamp", F.to_timestamp("order_delivered_timestamp", "yyyy-MM-dd HH:mm:ss")),
            df_order.withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date", "yyyy-MM-dd"))
            )

        df_order.toPandas().csv(load_path)

    except Exception as e:
        logging.error(f"Error reading order data: {e}")
    else:
        logging.info("Order Data transformed successfully.")

        return df_order

def transform_payments(spark,file_path):
    try:
        logging.info("extract and transforming Payment Data ....")
        load_path = '/opt/airflow/staging/payments.csv'
        df_payments = spark.read.option('delimiter',',').option('header', True).csv(file_path)
        df_payments = df_payments.dropna(subset='order_id')
        df_payments = (
            df_payments.withColumn("payment_value", F.col("payment_value").cast("double")),
            df_payments.withColumn("payment_installments", F.col("payment_installments").cast("integer"))
            )

        df_payments.toPandas().csv(load_path)

    except Exception as e:
        logging.error(f"Error reading payment data: {e}")
    else:
        logging.info("Payment Data transformed successfully.")

        return df_payments

def transform_product(spark,file_path):
    try:
        logging.info("extract and transforming Product Data ....")
        load_path = '/opt/airflow/staging/products.csv'
        df_product = spark.read.option('delimiter',',').option('header', True).csv(file_path)
        df_product = df_product.dropna(subset='product_id')
        df_product = df_product.withColumn('product_length_cm',F.col('product_length_cm').cast('double'))
        df_product = df_product.withColumn('product_height_cm',F.col('product_height_cm').cast('double'))
        df_product = df_product.withColumn('product_width_cm',F.col('product_width_cm').cast('double'))

        df_product = df_product.withColumn('volume product', df_product.product_length_cm * df_product.product_height_cm * df_product.product_width_cm)
        df_product = df_product.withColumn('volume product',F.col('volume product').cast('double'))

        df_product.toPandas().csv(load_path)

    except Exception as e:
        logging.error(f"Error reading product data: {e}")
    else:
        logging.info("Product Data transformed successfully.")

        return df_product


def main():
    try:
            spark = sesi()
            file_path_cust = '/opt/airflow/train/df_Customers.csv'
            # file_path_oi = '/opt/airflow/train/df_OrderItems.csv'
            # file_path_order = '/opt/airflow/train/df_Orders.csv'
            # file_path_payments = '/opt/airflow/train/df_Payments.csv'
            # file_path_product = '/opt/airflow/train/df_Products.csv'
        
            df_cust = transform_cust(spark, file_path_cust)
            # df_oi = transform_oi(spark, file_path_oi)
            # df_order = transform_order(spark, file_path_order)
            # df_payments = transform_payments(spark, file_path_payments)
            # df_product = transform_product(spark, file_path_product)


    except Exception as exp:
            logging.error("Error Occured in the main() method. check the Stack Trace to go to the respective module "
                "and fix it." + str(exp))
            sys.exit(1)


if __name__ == "__main__" :
    logging.info("spark_etl is Started ...")
    main()


