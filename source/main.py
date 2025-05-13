from pyspark.sql import SparkSession
from tabulate import tabulate

from source.query1 import query1

ITALY_HOURLY_FILE = "../dataset/combined/combined_dataset-italy_hourly.csv"
SWEDEN_HOURLY_FILE = "../dataset/combined/combined_dataset-sweden_hourly.csv"

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Pyspark hello world") \
        .getOrCreate()

    sc = spark.sparkContext

    result1 = query1(sc, italy_file=ITALY_HOURLY_FILE, sweden_file=SWEDEN_HOURLY_FILE).collect()

    print(tabulate(result1,
                   headers=["Country, Year",
                            "Avg CO2 Intensity", "Min CO2 Intensity", "Max CO2 Intensity",
                            "Avg C02 Free", "Min C02 Free", "Max C02 Free"],
                   tablefmt="psql"))