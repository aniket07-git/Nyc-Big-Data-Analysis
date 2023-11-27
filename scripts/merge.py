from pyspark.sql import SparkSession
from file_rename import rename_spark_output_csv

# Initialize Spark session
spark = SparkSession.builder.appName("MergeCSV").getOrCreate()

# Read the CSV files into DataFrames
df1 = spark.read.csv("resources/data/cleaned/cleaned_highvolume_dataset/cleaned_fhvhv.csv", header=True, inferSchema=True)
df2 = spark.read.csv("resources/data/cleaned/cleaned_taxi_zone_dataset/cleaned_taxiZone.csv", header=True, inferSchema=True)

# Alias df2 for join operations
df2_alias = df2.alias("df2")

# First Join: Join df1 with df2 on PULocationID
df1 = df1.join(df2_alias, df1["PULocationID"] == df2_alias["LocationID"], "left")

# Rename and select columns after the first join
for column in ["OBJECTID", "LocationID", "Length", "Geometry", "Area", "zone", "borough"]:
    df1 = df1.withColumnRenamed(column, "PU_" + column)

# Drop redundant column 'LocationID' from df2
df1 = df1.drop("PU_LocationID")
df1 = df1.drop("PU_OBJECTID")

# Second Join: Join df1 with df2 on DOLocationID
df1 = df1.join(df2_alias, df1["DOLocationID"] == df2_alias["LocationID"], "left")

# Rename and select columns after the second join
for column in ["OBJECTID", "LocationID", "Length", "Geometry", "Area", "zone", "borough"]:
    df1 = df1.withColumnRenamed(column, "DO_" + column)

# Drop redundant column 'LocationID' from df2
df1 = df1.drop("DO_LocationID")
df1 = df1.drop("DO_OBJECTID")

df1.show()

# Write the result to a CSV file
merged_source_dir = "resources/data/merged"
df1.write.csv(merged_source_dir, header=True, mode="overwrite")
# Rename files
merged_new_name = "merged_data.csv"
rename_spark_output_csv(merged_source_dir, merged_new_name)

# Stop the Spark session
spark.stop()
