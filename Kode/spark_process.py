from pyspark.sql import SparkSession
import logging


def sesiSpark():
    spark = SparkSession.builder \
        .appName("Sustainability Data Processing") \
        .getOrCreate()

    return spark


def load_fact_data(spark,file_path,file_format):
    try:
        logging.info(f"Loading fact data from {file_path} with format {file_format}")
        df = spark.read.load(
            file_path,
            format=file_format,
            sep = ',',
            inferSchema=True,
            header=True
         )

        df = df.select([df.columns[0],df.columns[1],df.columns[6],df.columns[9],df.columns[11],df.columns[14],df.columns[-1],df.columns[-3],])

    except Exception as e:
        logging.error(f"Error loading fact data: {e}")
        raise 
    else:
        logging.info(f"Successfully loaded fact data from {file_path}")

    return df


def load_data_dim(spark,file_path,file_format):
    try:
        logging.info(f"Loading dimension data from {file_path} with format {file_format}")
        df = spark.read.load(
            file_path,
            format=file_format,
            sep = ',',
            inferSchema=True,
            header=True
         )

        time = df.select(df.columns['time_id'], df.columns['Time_Date'],df.columns['Time_Year'],df.columns['Time_Month'], df.columns['Time_Day'])
        site = df.select(df.columns['site_id'], df.columns['Site_Name'],df.columns['Site_Location'])
        weather = df.select(df.columns['weather_id'], df.columns['Weather_Condition'],df.columns['Weather_Temperature'])
        equipment = df.select(df.columns['equipment_id'], df.columns['Equipment_Name'],df.columns['Equipment_Type'], df['Equipment_Rated_Capacity'], df['Equipment_Operational_Status'])
        energy = df.select(df.columns['energy_id'], df.columns['energy_type_id'],df.columns['Energy_Type_Name'])


    except Exception as e:
        logging.error(f"Error loading dimension data: {e}")
        raise 
    else:
        logging.info(f"Successfully loaded dimension data from {file_path}")

    return time, site, weather, equipment, energy


def clean_data(dimensi=list):
    try:
        logging.info("Cleaning dimension data")
        for df in dimensi:
            df = df.dropDuplicates()
            df = df.dropna()
    except Exception as e:
        logging.error(f"Error cleaning dimension data: {e}")
        raise 
    else:
        logging.info("Successfully cleaned dimension data")

    return dimensi

