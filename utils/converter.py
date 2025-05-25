from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Read and Write Parquet").getOrCreate()

# Read a file into a DataFrame
df = spark.read.csv("path_to_file.csv")

# Write the DataFrame to Parquet format
df.write.parquet("output_path")

# Stop the Spark session
spark.stop()
