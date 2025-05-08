from pyspark.sql import SparkSession
from tabulate import tabulate

HOURLY_FILE = "../dataset/combined/combined_hourly_dataset.csv"
MONTHLY_FILE = "../dataset/combined/combined_monthly_dataset.csv"
YEARLY_FILE = "../dataset/combined/combined_yearly_dataset.csv"

def get_country(x):
    return x.split(",")[1]


def get_year(x):
    return x.split(",")[0].split("-")[0]


def get_month(x):
    return x.split(",")[0].split("-")[1]


def get_co2_intensity(x) -> float:
    return float(x.split(',')[4])


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Pyspark hello world") \
        .getOrCreate()

    sc = spark.sparkContext

    hourly_lines = sc.textFile(HOURLY_FILE)
    monthly_lines = sc.textFile(MONTHLY_FILE)
    yearly_lines = sc.textFile(YEARLY_FILE)

    print(f"Hourly lines: {hourly_lines.count()}")

    exit(0)

    # Query 2.2
    by_c02_intensity = monthly_lines \
        .filter(lambda x: get_country(x) == "Italy") \
        .map(lambda x: (get_year(x), get_month(x), x)) \
        .map(lambda x: (get_co2_intensity(x[2]), x[0], x[1], x[2])) \
        .cache()

    best_5 = by_c02_intensity.takeOrdered(5, lambda x: x[0])
    worst_5 = by_c02_intensity.takeOrdered(5, lambda x: -x[0])

    print(tabulate(best_5, headers=["CO2 Intensity", "Year", "Month", "Line"], tablefmt='psql'))
    print(tabulate(worst_5, headers=["CO2 Intensity", "Year", "Month", "Line"], tablefmt='psql'))
