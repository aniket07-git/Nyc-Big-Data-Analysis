from file_rename import rename_spark_output_csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, round
from pyspark.sql import DataFrame
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def read_csv_to_df(spark: SparkSession, path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
    try:
        return spark.read.csv(path, header=header, inferSchema=infer_schema)
    except Exception as e:
        logger.error(f"Failed to read CSV at path {path}: {e}")
        raise

def calculate_duration(df: DataFrame, pickup_col: str, dropoff_col: str) -> DataFrame:
    return df.withColumn("duration", round((unix_timestamp(dropoff_col) - unix_timestamp(pickup_col)) / 60, 2))

def write_df_to_csv(df: DataFrame, path: str, num_partitions: int = None):
    try:
        if num_partitions:
            df = df.repartition(num_partitions)
        df.write.csv(path, header=True, mode="overwrite")
        logger.info(f"Dataframe written to CSV at path {path}")
    except Exception as e:
        logger.error(f"Failed to write dataframe to CSV at path {path}: {e}")
        raise

def clean_and_write_taxi_zone_data(spark: SparkSession, input_path: str, output_path: str):
    taxi_zone_df = read_csv_to_df(spark, input_path)
    taxi_zone_df = taxi_zone_df.drop('the_geom', 'OBJECTID')
    write_df_to_csv(taxi_zone_df, output_path, num_partitions=1) # Coalesce is used here to write to a single file for renaming

def clean_and_write_fhvhv_data(spark: SparkSession, input_path: str, output_path: str):
    fhvhv_df = read_csv_to_df(spark, input_path)
    fhvhv_df = calculate_duration(fhvhv_df, "pickup_datetime", "dropOff_datetime")
    write_df_to_csv(fhvhv_df, output_path, num_partitions=1) # Coalesce is used here to write to a single file for renaming

def main():
    spark = SparkSession.builder.appName("TaxiZonesDataCleaning").getOrCreate()

    try:
        # Define paths
        taxi_zone_raw_path = "resources/data/raw/taxi_zones.csv"
        taxi_zone_clean_path = "resources/data/cleaned/cleaned_taxi_zone_dataset"
        fhvhv_converted_path = "resources/data/converted/converted_fhvhv.csv"
        fhvhv_clean_path = "resources/data/cleaned/cleaned_highvolume_dataset"

        # Clean and write Taxi Zone Data
        clean_and_write_taxi_zone_data(spark, taxi_zone_raw_path, taxi_zone_clean_path)

        # Clean and write FHVHV Data
        clean_and_write_fhvhv_data(spark, fhvhv_converted_path, fhvhv_clean_path)

        # Rename files
        taxi_zone_cleaned_source_dir = "resources/data/cleaned/cleaned_taxi_zone_dataset/"
        taxi_zone_cleaned_new_name = "cleaned_taxiZone.csv"
        rename_spark_output_csv(taxi_zone_cleaned_source_dir, taxi_zone_cleaned_new_name)
        fhvhv_source_dir = "resources/data/cleaned/cleaned_highvolume_dataset/"
        fhvhv_new_name = "cleaned_fhvhv.csv"
        rename_spark_output_csv(fhvhv_source_dir, fhvhv_new_name)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
