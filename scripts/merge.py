from pyspark.sql import SparkSession
from start_hdfs_services import stop_and_start_hadoop_services

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
df1 = df1.drop("PU_Geometry")

# Second Join: Join df1 with df2 on DOLocationID
df1 = df1.join(df2_alias, df1["DOLocationID"] == df2_alias["LocationID"], "left")

# Rename and select columns after the second join
for column in ["OBJECTID", "LocationID", "Length", "Geometry", "Area", "zone", "borough"]:
    df1 = df1.withColumnRenamed(column, "DO_" + column)

# Drop redundant column 'LocationID' from df2
df1 = df1.drop("DO_LocationID")
df1 = df1.drop("DO_OBJECTID")
df1 = df1.drop("DO_Geometry")

# Output HDFS Path
hdfs_output_dir = "hdfs://localhost:9000/tmp/hadoop-yuvrajpatadia/dfs/data"
hdfs_output_file = hdfs_output_dir + "/merged.csv"

# Create a new directory in HDFS
# subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_output_dir])
stop_and_start_hadoop_services()
# Write the merged DataFrame to the new HDFS directory
df1.coalesce(1).write.csv(hdfs_output_file, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
