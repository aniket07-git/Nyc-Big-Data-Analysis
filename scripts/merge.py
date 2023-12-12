import os
from start_hdfs_services import stop_and_start_hadoop_services
import logging
from pyspark.sql import SparkSession
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delete_hdfs_dir(hdfs_dir):
    try:
        subprocess.run(['hdfs', 'dfs', '-rm', '-r', hdfs_dir], check=True)
        logger.info(f"Successfully deleted all files in {hdfs_dir}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to delete files in {hdfs_dir}: {e}")
        raise

def main():
    spark = SparkSession.builder.appName("MergeCSV").getOrCreate()

    try:
        # Reading the data into DataFrames with appropriate error handling
        df1_path = "resources/data/cleaned/cleaned_highvolume_dataset/cleaned_fhvhv.csv"
        df2_path = "resources/data/cleaned/cleaned_taxi_zone_dataset/cleaned_taxiZone.csv"
        df1 = spark.read.csv(df1_path, header=True, inferSchema=True)
        df2 = spark.read.csv(df2_path, header=True, inferSchema=True)

        # Renaming columns according to a mapping
        # This assumes that the same renaming logic applies to both joins
        rename_mappings = {
            'Shape_Leng': 'ShapeLength',
            'Shape_Area': 'ShapeArea',
            'LocationID': 'LocationID',
            'zone': 'zone',
            'borough': 'borough'
        }

        # Alias df2 for join operations
        df2_alias = df2.alias("df2")

        # First Join: Join df1 with df2 on PULocationID
        df1 = df1.join(df2_alias, df1["PULocationID"] == df2_alias["LocationID"], "left")

        for original, new in rename_mappings.items():
            df1 = df1.withColumnRenamed(original, f"PU_{new}")

        #Drop redundant column 'PU_LocationID'
        df1 = df1.drop("PU_LocationID")
        # Second Join: Join df1 with df2 on DOLocationID
        df1 = df1.join(df2_alias, df1["DOLocationID"] == df2_alias["LocationID"], "left")

        for original, new in rename_mappings.items():
            df1 = df1.withColumnRenamed(original, f"DO_{new}")

        #Drop redundant column 'DO_LocationID'
        df1 = df1.drop("DO_LocationID")

        # Output HDFS path
        hdfs_output_dir = "hdfs://localhost:9000/tmp/hadoop-yuvrajpatadia/dfs/data"
        hdfs_output_file = f"{hdfs_output_dir}/merged.parquet"

        # Delete existing HDFS directory
        # delete_hdfs_dir(hdfs_output_dir)

        # start HDFS service
        stop_and_start_hadoop_services()

        # Write the merged DataFrame to HDFS
        df1.coalesce(1).write.parquet(hdfs_output_file, mode="overwrite")
        logger.info(f"Merged data written to {hdfs_output_file}")

        # Delete the intermediate 'converted.csv' file if exists
        # File paths to check and delete if they exist
        file_paths = [
            'resources/data/converted/converted_fhvhv.csv',
            'resources/data/cleaned/cleaned_taxi_zone_dataset/cleaned_taxiZone.csv',
            'resources/data/cleaned/cleaned_highvolume_dataset/cleaned_fhvhv.csv'
        ]

        # Function to delete a file if it exists
        def delete_file_if_exists(file_path):
            if os.path.exists(file_path):
                os.remove(file_path)

        # Deleting the files
        [delete_file_if_exists(path) for path in file_paths]

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == '__main__':
    main()
