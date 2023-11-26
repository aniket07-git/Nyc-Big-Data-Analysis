from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, FloatType    
from pyspark.sql.functions import col
from pyspark import SparkContext
from pyspark.sql.functions import when
from pyspark.sql.functions import to_timestamp, unix_timestamp
import os

# Initialize a Spark session
spark = SparkSession.builder.appName("TaxiZonesDataCleaning").getOrCreate()

# Load the data
df = spark.read.csv("resources/data/raw/taxi_zones.csv", header=True, inferSchema=True)
df1 = spark.read.csv("resources/data/converted/part-00000-435ffd69-809f-4ecd-8787-ede4a075ce72-c000.csv", header=True, inferSchema=True)

# Handling Missing Values
for column in df1.columns:
    if isinstance(df1.schema[column].dataType, StringType):
        df1 = df1.withColumn(column, when(col(column).isNull(), "N/A").otherwise(col(column)))
    else:
        df1 = df1.withColumn(column, when(col(column).isNull(), None).otherwise(col(column)))

# Date and Time Conversion
df1 = df1.withColumn("pickup_datetime", to_timestamp("pickup_datetime"))
df1 = df1.withColumn("dropOff_datetime", to_timestamp("dropOff_datetime"))

# Data Validation
df1 = df1.filter(df1.pickup_datetime < df1.dropOff_datetime)

# Create a new column called duration
df1 = df1.withColumn("duration", 
                   (unix_timestamp("dropOff_datetime") - unix_timestamp("pickup_datetime")) / 60)

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
output_path = "resources/data/cleaned/cleaned_taxi_zone_dataset"
# Coalesce to a single partition and save the cleaned data to a single CSV file
output_path1 = "resources/data/cleaned/cleaned_highvolume_dataset"
# Check if the output path already exists
fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
# Coalesce to a single partition and write the data
df_cleaned.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
df1.coalesce(1).write.csv(output_path1, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
