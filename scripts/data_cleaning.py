from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql.functions import col
from pyspark import SparkContext
import os

# Initialize a Spark session
spark = SparkSession.builder.appName("TaxiZonesDataCleaning").getOrCreate()

# Load the data
df = spark.read.csv("resources/data/raw/taxi_zones.csv", header=True, inferSchema=True)

# Remove duplicate rows if any
df_cleaned = df.dropDuplicates()

# Renaming columns for clarity
df_cleaned = df_cleaned.withColumnRenamed("Shape_Leng", "Length") \
                       .withColumnRenamed("Shape_Area", "Area") \
                       .withColumnRenamed("the_geom", "Geometry")

# Type Conversion
df_cleaned = df_cleaned.withColumn("OBJECTID", col("OBJECTID").cast(IntegerType())) \
                       .withColumn("LocationID", col("LocationID").cast(IntegerType())) \
                       .withColumn("Length", col("Length").cast(DoubleType())) \
                       .withColumn("Area", col("Area").cast(DoubleType()))

# Fill missing values with defaults
default_values = {
    "Length": 0.0,
    "Area": 0.0,
    "Geometry": "Unknown",
    "zone": "Unknown",
    "LocationID": 0,
    "borough": "Unknown",
    "OBJECTID": 0
}
df_cleaned = df_cleaned.fillna(default_values)

# Coalesce to a single partition and save the cleaned data to a single CSV file
output_path = "resources/data/cleaned/taxi_zones.csv"
# Check if the output path already exists
fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
# Coalesce to a single partition and write the data
df_cleaned.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
