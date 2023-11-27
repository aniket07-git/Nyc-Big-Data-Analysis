from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from file_rename import rename_spark_output_csv

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ParquetToCSV") \
    .getOrCreate()

# Load the Parquet file into a DataFrame
parquetFile = spark.read.parquet("resources/data/raw/fhvhv.parquet")

# Coalesce to a single partition
parquetFile = parquetFile.coalesce(1)

# Save the DataFrame as a CSV file
parquetFile.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("resources/data/converted/")

source_dir = "resources/data/converted/"
new_name = "converted_fhvhv.csv"
rename_spark_output_csv(source_dir, new_name)

# Stop the SparkSession when done
spark.stop()
