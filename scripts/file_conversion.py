from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ParquetToCSV") \
    .getOrCreate()

# Load the Parquet file into a DataFrame
parquetFile = spark.read.parquet("resources/data/raw/fhv_tripdata_2023-01.parquet")

# Coalesce to a single partition
parquetFile = parquetFile.coalesce(1)

# Save the DataFrame as a CSV file
parquetFile.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("resources/data/converted/")

# Stop the SparkSession when done
spark.stop()
