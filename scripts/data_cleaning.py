from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType    
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import to_timestamp, unix_timestamp
from file_rename import rename_spark_output_csv
import os

# Initialize a Spark session
spark = SparkSession.builder.appName("TaxiZonesDataCleaning").getOrCreate()

# Load the data
taxiZoneDf = spark.read.csv("resources/data/raw/taxi_zones.csv", header=True, inferSchema=True)
fhvhvDf = spark.read.csv("resources/data/converted/converted_fhvhv.csv", header=True, inferSchema=True)

# Handling Missing Values
for column in fhvhvDf.columns:
    if isinstance(fhvhvDf.schema[column].dataType, StringType):
        fhvhvDf = fhvhvDf.withColumn(column, when(col(column).isNull(), "N/A").otherwise(col(column)))
    else:
        fhvhvDf = fhvhvDf.withColumn(column, when(col(column).isNull(), None).otherwise(col(column)))

# Date and Time Conversion
fhvhvDf = fhvhvDf.withColumn("pickup_datetime", to_timestamp("pickup_datetime"))
fhvhvDf = fhvhvDf.withColumn("dropOff_datetime", to_timestamp("dropOff_datetime"))

# Data Validation
fhvhvDf = fhvhvDf.filter(fhvhvDf.pickup_datetime < fhvhvDf.dropOff_datetime)

# Create a new column called duration
fhvhvDf = fhvhvDf.withColumn("duration", 
                   (unix_timestamp("dropOff_datetime") - unix_timestamp("pickup_datetime")) / 60)

# Remove duplicate rows if any
taxiZoneDf_cleaned = taxiZoneDf.dropDuplicates()

# Renaming columns for clarity
taxiZoneDf_cleaned = taxiZoneDf_cleaned.withColumnRenamed("Shape_Leng", "Length") \
                       .withColumnRenamed("Shape_Area", "Area") \
                       .withColumnRenamed("the_geom", "Geometry")

# Type Conversion
taxiZoneDf_cleaned = taxiZoneDf_cleaned.withColumn("OBJECTID", col("OBJECTID").cast(IntegerType())) \
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
taxiZoneDf_cleaned = taxiZoneDf_cleaned.fillna(default_values)

# Coalesce to a single partition and save the cleaned data to a single CSV file
taxiZoneCleanPath = "resources/data/cleaned/cleaned_taxi_zone_dataset"
# Coalesce to a single partition and save the cleaned data to a single CSV file
fhvhvCleanPath = "resources/data/cleaned/cleaned_highvolume_dataset"
# Check if the output path already exists
fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
# Coalesce to a single partition and write the data
taxiZoneDf_cleaned.coalesce(1).write.csv(taxiZoneCleanPath, header=True, mode="overwrite")
fhvhvDf.coalesce(1).write.csv(fhvhvCleanPath, header=True, mode="overwrite")
# Rename files
taxiZoneDf_cleaned_source_dir = "resources/data/cleaned/cleaned_taxi_zone_dataset/"
taxiZoneDf_cleaned_new_name = "cleaned_taxiZone.csv"
rename_spark_output_csv(taxiZoneDf_cleaned_source_dir, taxiZoneDf_cleaned_new_name)
fhvhvDf_source_dir = "resources/data/cleaned/cleaned_highvolume_dataset/"
fhvhvDf_new_name = "cleaned_fhvhv.csv"
rename_spark_output_csv(fhvhvDf_source_dir, fhvhvDf_new_name)
# Stop the Spark session
spark.stop()
# Delete the intermediate 'converted.csv' file if exists
file_path = 'resources/data/converted/converted_fhvhv.csv'  # Replace with the actual path of 'converted.csv'
if os.path.exists(file_path):
    os.remove(file_path)