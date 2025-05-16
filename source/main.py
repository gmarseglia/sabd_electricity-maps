from pyspark.sql import SparkSession
from tabulate import tabulate

from custom_formatter import QUERY_1_COLUMNS
from query1 import query1
from query2 import query2

# ITALY_HOURLY_FILE = "../dataset/combined/combined_dataset-italy_hourly.csv"
# SWEDEN_HOURLY_FILE = "../dataset/combined/combined_dataset-sweden_hourly.csv"
HDFS_PREFIX = "hdfs://master:54310"
ITALY_HOURLY_FILE = f"{HDFS_PREFIX}/dataset/combined/combined_dataset-italy_hourly.csv"
SWEDEN_HOURLY_FILE = f"{HDFS_PREFIX}/dataset/combined/combined_dataset-sweden_hourly.csv"

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("SABD - Electricy Maps") \
        .getOrCreate()

    sc = spark.sparkContext

    # query2(spark, ITALY_HOURLY_FILE)

    result1 = query1(sc, italy_file=ITALY_HOURLY_FILE, sweden_file=SWEDEN_HOURLY_FILE)
    
    # result1.collect()

    result1.toDF(QUERY_1_COLUMNS) \
        .coalesce(1) \
        .write \
        .mode('overwrite') \
        .csv(f'{HDFS_PREFIX}/results/query_1', header=True)

    # print(tabulate(result1.collect(),
    #                headers=QUERY_1_COLUMNS,
    #                tablefmt="psql"))

    # spark.stop()
