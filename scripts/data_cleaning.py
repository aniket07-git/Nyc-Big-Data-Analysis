from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, round
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging

# Initialize logging for the application
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def read_csv_to_df(spark: SparkSession, path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
    try:
        # Read a CSV file into a DataFrame using provided SparkSession, path, and optional schema inference
        return spark.read.csv(path, header=header, inferSchema=infer_schema)
    except Exception as e:
        # Log an error if reading the CSV fails and re-raise the exception
        logger.error(f"Failed to read CSV at path {path}: {e}")
        raise

def calculate_duration(df: DataFrame, pickup_col: str, dropoff_col: str) -> DataFrame:
    # Add a new column 'duration' to DataFrame, calculated as the difference between pickup and dropoff times in minutes
    return df.withColumn("duration", round((unix_timestamp(dropoff_col) - unix_timestamp(pickup_col)) / 60, 2))

def write_df_to_csv(df: DataFrame, path: str, num_partitions: int = None):
    try:
        # Repartition the DataFrame if a number of partitions is specified
        if num_partitions:
            df = df.repartition(num_partitions)
        # Write the DataFrame to a CSV file at the specified path, overwriting existing files
        df.write.csv(path, header=True, mode="overwrite")
        # Log a message indicating successful writing
        logger.info(f"Dataframe written to CSV at path {path}")
    except Exception as e:
        # Log an error if writing the CSV fails and re-raise the exception
        logger.error(f"Failed to write dataframe to CSV at path {path}: {e}")
        raise

def clean_and_write_taxi_zone_data(spark: SparkSession, input_path: str, output_path: str):
    # Read taxi zone data into a DataFrame
    taxi_zone_df = read_csv_to_df(spark, input_path)
    # Drop unnecessary columns from the DataFrame
    taxi_zone_df = taxi_zone_df.drop('the_geom', 'OBJECTID')
    # Write the cleaned DataFrame to a CSV file
    write_df_to_csv(taxi_zone_df, output_path, num_partitions=16)

def clean_and_write_fhvhv_data(spark: SparkSession, input_path: str, output_path: str):
    # Read FHVHV (For-Hire Vehicle and High-Volume For-Hire Vehicle) data into a DataFrame
    fhvhv_df = read_csv_to_df(spark, input_path)
    # Filter out rows from the DataFrame based on specific conditions
    fhvhv_df = fhvhv_df.filter((col("trip_time") > 0) & 
                                (col("base_passenger_fare") > 0) & 
                                (col("trip_miles") > 0))
    # Calculate the duration of each trip and add it as a new column
    fhvhv_df = calculate_duration(fhvhv_df, "pickup_datetime", "dropOff_datetime")
    # Write the cleaned DataFrame to a CSV file
    write_df_to_csv(fhvhv_df, output_path, num_partitions=1)

def main():
    # Initialize a SparkSession
    spark = SparkSession.builder.appName("TaxiZonesDataCleaning").getOrCreate()

    try:
        # Defined file paths for raw and cleaned data
        taxi_zone_raw_path = "resources/data/raw/taxi_zones.csv"
        taxi_zone_clean_path = "resources/data/cleaned/cleaned_taxi_zone_dataset"
        fhvhv_converted_path = "resources/data/converted/*.csv"
        fhvhv_clean_path = "resources/data/cleaned/cleaned_highvolume_dataset"

        # Process and clean Taxi Zone and FHVHV data
        clean_and_write_taxi_zone_data(spark, taxi_zone_raw_path, taxi_zone_clean_path)
        clean_and_write_fhvhv_data(spark, fhvhv_converted_path, fhvhv_clean_path)

    except Exception as e:
        # Log an error if any part of the main process fails
        logger.error(f"An error occurred: {e}")
        raise e
    finally:
        # Stop the SparkSession to free up resources
        spark.stop()

if __name__ == "__main__":
    # Run the main function if the script is executed as the main program
    main()
