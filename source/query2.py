# Query 2.2
from pyspark import SparkContext
from pyspark.sql.connect.session import SparkSession
from pyspark.sql import functions as F
from tabulate import tabulate

from custom_formatter import *


def query2(spark: SparkSession, italy_file: str, api: str):
    if api == "rdd":
        return query2_rdd(spark, italy_file)
    if api == "df":
        return query2_df(spark, italy_file)
    raise Exception("API not supported")


def query2_df(spark: SparkSession, italy_file: str):
    if italy_file.endswith(".csv"):
        df = spark.read.csv(italy_file, header=False, inferSchema=True).toDF(
            *COLUMN_NAMES_RAW
        )
    elif italy_file.endswith(".parquet"):
        df = spark.read.parquet(italy_file)
    else:
        raise Exception("Invalid file format for DF API implementation: {italy_file}")

    df = (
        df.withColumn("Year", F.split(F.col("Datetime"), "-").getItem(0))
        .withColumn("Month", F.split(F.col("Datetime"), "-").getItem(1))
        .select(*COLUMN_NAMES_DF_2)
    )

    df_avg = df.groupBy("Year", "Month").agg(
        F.avg("CO2_intensity_direct").alias(QUERY_2_COLUMNS[1]),
        F.avg("Carbon_free_energy_percent").alias(QUERY_2_COLUMNS[2]),
    )

    df_avg = (
        df_avg.withColumn("date", F.concat(F.col("Year"), F.lit("_"), F.col("Month")))
        .select(*QUERY_2_COLUMNS)
        .cache()
    )

    df_by_carbon_intensity = df_avg.orderBy(F.col(QUERY_2_COLUMNS[1]).desc()).cache()
    df_by_carbon_intensity_top = df_by_carbon_intensity.limit(5)
    df_by_carbon_intensity_bottom = df_by_carbon_intensity.orderBy(
        F.col(QUERY_2_COLUMNS[1]).asc()
    ).limit(5)

    df_by_cfe = df_avg.orderBy(F.col(QUERY_2_COLUMNS[2]).desc()).cache()
    df_by_cfe_top = df_by_cfe.limit(5)
    df_by_cfe_bottom = df_by_cfe.orderBy(F.col(QUERY_2_COLUMNS[2]).asc()).limit(5)

    return (
        df_by_carbon_intensity,
        df_by_carbon_intensity_top,
        df_by_carbon_intensity_bottom,
        df_by_cfe,
        df_by_cfe_top,
        df_by_cfe_bottom,
    )


def query2_rdd(spark: SparkSession, italy_file: str):
    if italy_file.endswith(".csv"):
        sc = spark.sparkContext
        italy_rdd = sc.textFile(italy_file)
    else:
        raise Exception(f"Invalid file format for RDD API implementation: {italy_file}")

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

    rdd_by_carbon_intensity = avg.sortBy(lambda x: x[1], ascending=False).cache()
    rdd_by_carbon_intensity_top = (
        rdd_by_carbon_intensity.zipWithIndex()
        .filter(lambda x: x[1] < 5)
        .map(lambda x: x[0])
    )
    rdd_by_carbon_intensity_bottom = (
        rdd_by_carbon_intensity.sortBy(lambda x: x[1], ascending=True)
        .zipWithIndex()
        .filter(lambda x: x[1] < 5)
        .map(lambda x: x[0])
    )

    rdd_by_cfe = avg.sortBy(lambda x: x[2], ascending=False).cache()
    rdd_by_cfe_top = (
        rdd_by_cfe.zipWithIndex().filter(lambda x: x[1] < 5).map(lambda x: x[0])
    )
    rdd_by_cfe_bottom = (
        rdd_by_cfe.sortBy(lambda x: x[2], ascending=True)
        .zipWithIndex()
        .filter(lambda x: x[1] < 5)
        .map(lambda x: x[0])
    )

    return (
        rdd_by_carbon_intensity,
        rdd_by_carbon_intensity_top,
        rdd_by_carbon_intensity_bottom,
        rdd_by_cfe,
        rdd_by_cfe_top,
        rdd_by_cfe_bottom,
    )
