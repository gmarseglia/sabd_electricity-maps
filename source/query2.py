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
            F.avg("CO2_intensity_direct").alias(QUERY_2_COLUMNS[1]),
            F.avg("Carbon_free_energy_percent").alias(QUERY_2_COLUMNS[2]),
        )
        .cache()
    )

    df_avg = df_avg.withColumn(
        "date", F.concat(F.col("Year"), F.lit("_"), F.col("Month"))
    ).select(*QUERY_2_COLUMNS)

    df_by_direct = df_avg.orderBy(F.col(QUERY_2_COLUMNS[1]).desc()).cache()

    df_by_free = df_avg.orderBy(F.col(QUERY_2_COLUMNS[2]).desc()).cache()

    return df_by_direct, df_by_free


def query2_rdd(sc: SparkContext, italy_file: str):

    italy_rdd = sc.textFile(italy_file)

    base = italy_rdd.map(
        lambda x: (
            (get_country(x), get_year(x), get_month(x)),
            (get_co2_intensity(x), get_c02_free(x), 1),
        )
    )

    avg = base.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])).map(
        lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2]))
    )

    avg = (
        avg.map(lambda x: (x[0][0], x[0][1], x[0][2], x[1][0], x[1][1]))
        .map(lambda x: (x[1] + "_" + x[2], x[3], x[4]))
        .cache()
    )

    rdd_by_direct = avg.sortBy(lambda x: x[1], ascending=False).cache()

    rdd_by_free = avg.sortBy(lambda x: x[2], ascending=False).cache()

    return rdd_by_direct, rdd_by_free
