# Query 2.2
from pyspark import SparkContext
from pyspark.sql.connect.session import SparkSession
from pyspark.sql import functions as F
from tabulate import tabulate

from custom_formatter import *


def query2(spark: SparkSession, italy_file: str, api: str):
    if api == "default" or api == "df":
        return query2_df(spark, italy_file)

    if api == "rdd":
        return query2_rdd(spark.sparkContext, italy_file)


def query2_df(spark: SparkSession, italy_file: str):
    df = spark.read.csv(italy_file, header=False, inferSchema=True).toDF(
        *COLUMN_NAMES_RAW
    )

    df = (
        df.withColumn("Year", F.split(F.col("Datetime"), "-").getItem(0))
        .withColumn("Month", F.split(F.col("Datetime"), "-").getItem(1))
        .select(*COLUMN_NAMES_DF_2)
    )

    df_avg = (
        df.groupBy("Year", "Month")
        .agg(
            F.avg("CO2_intensity_direct").alias("avg_CO2_intensity_direct"),
            F.avg("Carbon_free_energy_percent").alias("avg_carbon_free_energy"),
        )
        .cache()
    )

    # df_sorted_by_direct = df_avg.sort(F.desc("avg_CO2_intensity_direct"))
    # df_sorted_by_free = df_avg.sort(F.desc("avg_carbon_free_energy"))

    df_by_direct = df_avg.orderBy(F.col("avg_CO2_intensity_direct").desc()).cache()
    df_by_direct_top = df_by_direct.head(5)
    df_by_direct_bottom = df_by_direct.tail(5)

    print(tabulate(df_by_direct_top, headers=QUERY_2_COLUMNS, tablefmt="grid"))
    print(tabulate(df_by_direct_bottom, headers=QUERY_2_COLUMNS, tablefmt="grid"))

    df_by_free = df_avg.orderBy(F.col("avg_carbon_free_energy").desc()).cache()
    df_by_free_top = df_by_free.head(5)
    df_by_free_bottom = df_by_free.tail(5)

    print(tabulate(df_by_free_top, headers=QUERY_2_COLUMNS, tablefmt="grid"))
    print(tabulate(df_by_free_bottom, headers=QUERY_2_COLUMNS, tablefmt="grid"))

    return df_by_direct, df_by_free


def query2_rdd(sc: SparkContext, italy_file: str):
    raise NotImplementedError
    # italy_rdd = sc.textFile(italy_file)
    #
    # base = italy_rdd \
    #     .map(lambda x: ((get_country(x), get_year(x), get_month(x)), (get_co2_intensity(x), get_c02_free(x), 1)))
    #
    # avg = base \
    #     .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
    #     .map(lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2]))) \
    #     .cache()
    #
    #
    # print(avg.take(10))
    #
    # best_5_co2_intensity = avg.takeOrdered(5, lambda x : x[1][0])
    #
    # by_c02_intensity = hourly_lines \
    #     .filter(lambda x: get_country(x) == "Italy") \
    #     .map(lambda x: (get_year(x), get_month(x), x)) \
    #     .map(lambda x: (get_co2_intensity(x[2]), x[0], x[1], x[2])) \
    #     .cache()
    #
    # best_5 = by_c02_intensity.takeOrdered(5, lambda x: x[0])
    # worst_5 = by_c02_intensity.takeOrdered(5, lambda x: -x[0])
    #
    # return
