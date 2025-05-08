from pyspark.sql import SparkSession
from tabulate import tabulate
from itertools import chain

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
    return float(x.split(',')[5])


def get_c02_free(x) -> float:
    return float(x.split(',')[6])


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Pyspark hello world") \
        .getOrCreate()

    sc = spark.sparkContext

    hourly_lines = sc.textFile(HOURLY_FILE)

    queries_base = hourly_lines \
        .map(lambda x: ((get_country(x), get_year(x), get_month(x)), (get_co2_intensity(x), get_c02_free(x), 1)))

    # Query 1.1: Average "CO2 Intensity" and "CO2 Free" by year ad by country
    query_1_base = queries_base \
        .map(lambda x: ((x[0][0], x[0][1]), x[1])) \
        .cache()

    avg_by_country = query_1_base \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
        .map(lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2])))

    min_by_country = query_1_base \
        .reduceByKey(lambda x, y: (min(x[0], y[0]), min(x[1], y[1])))

    max_by_country = query_1_base \
        .reduceByKey(lambda x, y: (max(x[0], y[0]), max(x[1], y[1])))

    query_1 = avg_by_country \
        .join(min_by_country) \
        .join(max_by_country) \
        .coalesce(1) \
        .map(lambda x:
             (x[0], x[1][0][0][0], x[1][0][1][0], x[1][1][0], x[1][0][0][1], x[1][0][1][1], x[1][1][1])
             ) \
        .sortBy(lambda x: x[0], True)

    print(tabulate(query_1.collect(),
                   headers=["Country, Year",
                            "Avg CO2 Intensity", "Min CO2 Intensity", "Max CO2 Intensity",
                            "Avg C02 Free", "Min C02 Free", "Max C02 Free"],
                   tablefmt="psql"))

# Query 2.2
# by_c02_intensity = hourly_lines \
#     .filter(lambda x: get_country(x) == "Italy") \
#     .map(lambda x: (get_year(x), get_month(x), x)) \
#     .map(lambda x: (get_co2_intensity(x[2]), x[0], x[1], x[2])) \
#     .cache()
#
# best_5 = by_c02_intensity.takeOrdered(5, lambda x: x[0])
# worst_5 = by_c02_intensity.takeOrdered(5, lambda x: -x[0])
#
# print(tabulate(best_5, headers=["CO2 Intensity", "Year", "Month", "Line"], tablefmt='psql'))
# print(tabulate(worst_5, headers=["CO2 Intensity", "Year", "Month", "Line"], tablefmt='psql'))
