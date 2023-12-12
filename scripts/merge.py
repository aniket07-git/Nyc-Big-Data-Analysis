from dotenv import load_dotenv
import os
import glob
from start_hdfs_services import stop_and_start_hadoop_services
import logging
from pyspark.sql import SparkSession

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delete_csv_files_in_directory(directory_path):
    try:
        # Find all .csv files in the directory
        csv_files = glob.glob(os.path.join(directory_path, '*.csv'))
        
        # Delete each found .csv file
        for file in csv_files:
            os.remove(file)
            logger.info(f"Deleted file: {file}")

    except Exception as e:
        logger.error(f"Failed to delete .csv files in {directory_path}: {e}")
def main():
    spark = SparkSession.builder.appName("MergeCSV").getOrCreate()

    try:
        # Reading the data into DataFrames with appropriate error handling
        df1_path = "resources/data/cleaned/cleaned_highvolume_dataset/*.csv"
        df2_path = "resources/data/cleaned/cleaned_taxi_zone_dataset/*.csv"
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
        hdfs_output_dir = os.getenv('HDFS_OUTPUT_DIR')
        hdfs_output_file = f"{hdfs_output_dir}/merged.parquet"

        # start HDFS service
        stop_and_start_hadoop_services()

        # Write the merged DataFrame to HDFS
        df1.repartition(16)
        df1.write.parquet(hdfs_output_dir, mode="overwrite")
        logger.info(f"Merged data written to {hdfs_output_file}")

        # Directories to check and delete .csv files
        directories = [
            'resources/data/converted',
            'resources/data/cleaned/cleaned_taxi_zone_dataset',
            'resources/data/cleaned/cleaned_highvolume_dataset'
        ]

        # Deleting .csv files in each directory
        for directory in directories:
            delete_csv_files_in_directory(directory)

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == '__main__':
    main()
