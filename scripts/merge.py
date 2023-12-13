from dotenv import load_dotenv
import os
import glob
from start_hdfs_services import stop_and_start_hadoop_services
import logging
from pyspark.sql import SparkSession

# Load environment variables from a .env file
load_dotenv()

# Configure logging for the application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delete_csv_files_in_directory(directory_path):
    try:
        # Find all .csv files in the specified directory
        csv_files = glob.glob(os.path.join(directory_path, '*.csv'))
        
        # Iterate through the found .csv files and delete each one
        for file in csv_files:
            os.remove(file)
            # Log the deletion of each file
            logger.info(f"Deleted file: {file}")

    except Exception as e:
        # Log an error if deletion fails
        logger.error(f"Failed to delete .csv files in {directory_path}: {e}")

def main():
    # Create a Spark session for data processing
    spark = SparkSession.builder.appName("MergeCSV").getOrCreate()

    try:
        # Define paths to the CSV files to be read
        df1_path = "resources/data/cleaned/cleaned_highvolume_dataset/*.csv"
        df2_path = "resources/data/cleaned/cleaned_taxi_zone_dataset/*.csv"
        # Read the CSV files into Spark DataFrames
        df1 = spark.read.csv(df1_path, header=True, inferSchema=True)
        df2 = spark.read.csv(df2_path, header=True, inferSchema=True)

        # Define a dictionary for renaming columns
        rename_mappings = {
            'Shape_Leng': 'ShapeLength',
            'Shape_Area': 'ShapeArea',
            'LocationID': 'LocationID',
            'zone': 'zone',
            'borough': 'borough'
        }

        # Create an alias for df2 for ease of join operations
        df2_alias = df2.alias("df2")

        # Perform a left join of df1 with df2 on the PULocationID
        df1 = df1.join(df2_alias, df1["PULocationID"] == df2_alias["LocationID"], "left")

        # Rename columns in df1 based on the rename_mappings
        for original, new in rename_mappings.items():
            df1 = df1.withColumnRenamed(original, f"PU_{new}")

        # Drop the redundant 'PU_LocationID' column from df1
        df1 = df1.drop("PU_LocationID")

        # Perform a second left join of df1 with df2 on the DOLocationID
        df1 = df1.join(df2_alias, df1["DOLocationID"] == df2_alias["LocationID"], "left")

        # Rename columns in df1 for the second join
        for original, new in rename_mappings.items():
            df1 = df1.withColumnRenamed(original, f"DO_{new}")

        # Drop the redundant 'DO_LocationID' column from df1
        df1 = df1.drop("DO_LocationID")

        # Get the output directory from environment variables
        hdfs_output_dir = os.getenv('HDFS_OUTPUT_DIR')
        hdfs_output_file = f"{hdfs_output_dir}/merged.parquet"

        # Restart HDFS services before writing the output
        stop_and_start_hadoop_services()

        # Write the merged DataFrame to a Parquet file in HDFS
        df1.repartition(16)
        df1.write.parquet(hdfs_output_dir, mode="overwrite")
        # Log the successful write operation
        logger.info(f"Merged data written to {hdfs_output_file}")

        # List of directories where .csv files will be deleted
        directories = [
            'resources/data/converted',
            'resources/data/cleaned/cleaned_taxi_zone_dataset',
            'resources/data/cleaned/cleaned_highvolume_dataset'
        ]

        # Delete .csv files in each listed directory
        for directory in directories:
            delete_csv_files_in_directory(directory)

    except Exception as e:
        # Log any exceptions that occur during the process
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        # Stop the Spark session and log the closure
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == '__main__':
    main()
