from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

from custom_formatter import *

def query1(spark: SparkSession, italy_file: str, sweden_file: str, api: str):
    if api == "default" or api == "rdd":
        return query_1_rdd(spark.sparkContext, italy_file, sweden_file)

    if api == "df":
        return query_1_df(spark, italy_file, sweden_file)


def query_1_rdd(sc: SparkContext, italy_file: str, sweden_file: str):
    italy_rdd = sc.textFile(italy_file)
    sweden_rdd = sc.textFile(sweden_file)

    hourly_rdd = italy_rdd.union(sweden_rdd)

    queries_base = hourly_rdd.map(
        lambda x: (
            (get_country(x), get_year(x), get_month(x)),
            (get_co2_intensity(x), get_c02_free(x), 1),
        )
    )

    # Query 1.1: Average "CO2 Intensity" and "CO2 Free" by year ad by country
    query_1_base = queries_base.map(lambda x: ((x[0][0], x[0][1]), x[1])).cache()

    avg_by_country = query_1_base.reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])
    ).map(lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2])))

    min_by_country = query_1_base.reduceByKey(
        lambda x, y: (min(x[0], y[0]), min(x[1], y[1]))
    )

    max_by_country = query_1_base.reduceByKey(
        lambda x, y: (max(x[0], y[0]), max(x[1], y[1]))
    )

    query_1 = (
        avg_by_country.join(min_by_country)
        .join(max_by_country)
        .map(
            lambda x: (
                x[0][0],
                x[0][1],
                x[1][0][0][0],
                x[1][0][1][0],
                x[1][1][0],
                x[1][0][0][1],
                x[1][0][1][1],
                x[1][1][1],
            )
        )
        .sortBy(lambda x: x[0] + x[1], True)
    )

    return query_1

def query_1_df(spark: SparkSession, italy_file: str, sweden_file: str):
    raise NotImplementedError
