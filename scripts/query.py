from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, sum as spark_sum

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkSQLonHDFSData") \
    .getOrCreate()

# Read the data from HDFS into a DataFrame
hdfs_input_dir = "hdfs://localhost:9000/tmp/hadoop-yuvrajpatadia/dfs/data/merged.csv"
df1 = spark.read.csv(hdfs_input_dir, header=True, inferSchema=True)

# Create a temporary view from the DataFrame
df1.createOrReplaceTempView("merged_data")

# Execute the SQL queries for Pickup and Dropoff counts
df1_pu = spark.sql('''
    SELECT PULocationID AS LocationID, count(*) AS PUcount
    FROM merged_data
    GROUP BY PULocationID
''')

df1_do = spark.sql('''
    SELECT DOLocationID AS LocationID, count(*) AS DOcount
    FROM merged_data
    GROUP BY DOLocationID
''')

# Perform an outer join on LocationID
df_q1 = df1_pu.join(df1_do, "LocationID", "outer").fillna(0)

# Calculate the total count
df_q1 = df_q1.withColumn("TOTALcount", col("PUcount") + col("DOcount"))

# Find the top 3 pickup and dropoff locations
PUtop3 = df_q1.orderBy(col("PUcount").desc()).limit(3)
DOtop3 = df_q1.orderBy(col("DOcount").desc()).limit(3)

# Show the top 3 pickup and dropoff locations
PUtop3.show()
DOtop3.show()

# Collect data to the driver node
PUtop3_local = PUtop3.toPandas()
DOtop3_local = DOtop3.toPandas()

# Stop the Spark session
spark.stop()
