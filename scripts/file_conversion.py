import logging
from pyspark.sql import SparkSession

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def read_parquet_file(spark, file_path):
    try:
        df = spark.read.parquet(file_path)
        return df
    except Exception as e:
        logger.error(f"Error reading parquet file: {e}")
        raise

def write_to_csv(df, target_dir):
    try:
        # Write in parallel, repartition if necessary
        df = df.repartition(16)
        df.write.option("header", "true").mode("overwrite").csv(target_dir)
    except Exception as e:
        logger.error(f"Error writing CSV file: {e}")
        raise

def main():
    spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()
    
    try:
        parquet_file_path = "resources/data/raw/fhvhv.parquet"
        output_dir = "resources/data/converted/"
        df = read_parquet_file(spark, parquet_file_path)
        # Write the DataFrame to a CSV file
        write_to_csv(df, output_dir)
        
    except Exception as e:
        # Log an error if any part of the main process fails
        logger.error(f"Job failed: {e}")
    finally:
        # Stop the SparkSession to free up resources
        spark.stop()

if __name__ == "__main__":
    # Run the main function if the script is executed as the main program
    main()
