# Query 2.2
from pyspark import SparkContext
from pyspark.sql.connect.session import SparkSession
from pyspark.sql import functions as F
from tabulate import tabulate

from custom_formatter import *


def query2(
    spark: SparkSession,
    italy_file: str,
    api: str,
    use_cache: bool = True,
    debug: bool = False,
):
    if api == "rdd":
        return query2_rdd(spark, italy_file, use_cache, debug)
    if api == "df":
        return query2_df(spark, italy_file, use_cache, debug)
    if api == "sql":
        return query2_sql(spark, italy_file, debug)
    raise Exception("API not supported")


def query2_rdd(
    spark: SparkSession, italy_file: str, use_cache: bool = True, debug: bool = False
):
    if italy_file.endswith(".csv"):
        sc = spark.sparkContext
        raw_italy = sc.textFile(italy_file)
    else:
        raise Exception(f"Invalid file format for RDD API implementation: {italy_file}")

    base = raw_italy.map(
        lambda x: (
            (get_year(x), get_month(x)),
            (get_co2_intensity(x), get_c02_free(x), 1),
        )
    )
    print_debug(base, "base", debug)

    all = (
        base.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
        .map(lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2])))
        .map(lambda x: (x[0][0] + "_" + x[0][1], x[1][0], x[1][1]))
    )

    if use_cache:
        all = all.cache()
    print_debug(all, "avg", debug)

    total_count = all.count()

    by_date = all.sortBy(lambda x: x[0], ascending=True)

    by_carbon_intensity = all.sortBy(lambda x: x[1], ascending=False).zipWithIndex()
    if use_cache:
        by_carbon_intensity = by_carbon_intensity.cache()

    by_carbon_intensity_top = by_carbon_intensity.filter(lambda x: x[1] < 5).map(
        lambda x: x[0]
    )

    by_carbon_intensity_bottom = (
        by_carbon_intensity.filter(lambda x: x[1] >= total_count - 5)
        .map(lambda x: x[0])
        .sortBy(lambda x: x[1], ascending=True)
    )

    by_cfe = all.sortBy(lambda x: x[2], ascending=False).zipWithIndex()
    if use_cache:
        by_cfe = by_cfe.cache()

    by_cfe_top = by_cfe.filter(lambda x: x[1] < 5).map(lambda x: x[0])

    by_cfe_bottom = (
        by_cfe.filter(lambda x: x[1] >= total_count - 5)
        .map(lambda x: x[0])
        .sortBy(lambda x: x[2], ascending=True)
    )

    return (
        by_date,
        by_carbon_intensity_top,
        by_carbon_intensity_bottom,
        by_cfe_top,
        by_cfe_bottom,
    )


def query2_df(
    spark: SparkSession, italy_file: str, use_cache: bool = True, debug: bool = False
):
    if italy_file.endswith(".csv"):
        raw_italy = spark.read.csv(italy_file, header=False, inferSchema=True).toDF(
            *COLUMN_NAMES_RAW
        )
    elif italy_file.endswith(".parquet"):
        raw_italy = spark.read.parquet(italy_file)
    elif italy_file.endswith(".avro"):
        raw_italy = spark.read.format("avro").load(italy_file)
    else:
        raise Exception("Invalid file format for DF API implementation: {italy_file}")

    base = (
        raw_italy.withColumn("Year", F.split(F.col("Datetime"), "-").getItem(0))
        .withColumn("Month", F.split(F.col("Datetime"), "-").getItem(1))
        .select(*COLUMN_NAMES_DF_2)
    )
    print_debug(base, "base", debug)

    all = (
        base.groupBy("Year", "Month")
        .agg(
            F.avg("CO2_intensity_direct").alias(QUERY_2_COLUMNS[1]),
            F.avg("Carbon_free_energy_percent").alias(QUERY_2_COLUMNS[2]),
        )
        .withColumn("date", F.concat(F.col("Year"), F.lit("_"), F.col("Month")))
        .select(*QUERY_2_COLUMNS)
    )
    if use_cache:
        all = all.cache()
    print_debug(all, "all", debug)

    by_date = all.orderBy(F.col(QUERY_2_COLUMNS[0]).asc())

    by_carbon_intensity_top = all.orderBy(F.col(QUERY_2_COLUMNS[1]).desc()).limit(5)

    by_carbon_intensity_bottom = all.orderBy(F.col(QUERY_2_COLUMNS[1]).asc()).limit(5)

    by_cfe_top = all.orderBy(F.col(QUERY_2_COLUMNS[2]).desc()).limit(5)

    by_cfe_bottom = all.orderBy(F.col(QUERY_2_COLUMNS[2]).asc()).limit(5)

    return (
        by_date,
        by_carbon_intensity_top,
        by_carbon_intensity_bottom,
        by_cfe_top,
        by_cfe_bottom,
    )


def query2_sql(spark: SparkSession, italy_file: str, debug: bool = False):
    # Read data
    if italy_file.endswith(".csv"):
        italy_df = spark.read.csv(italy_file, header=False, inferSchema=True).toDF(
            *COLUMN_NAMES_RAW
        )
    elif italy_file.endswith(".parquet"):
        italy_df = spark.read.parquet(italy_file)
    elif italy_file.endswith(".avro"):
        italy_df = spark.read.format("avro").load(italy_file)
    else:
        raise Exception("Invalid file format: {italy_file} and {sweden_file}")

    italy_df.createOrReplaceTempView("carbon_data")

    spark.sql(
        """
        CREATE OR REPLACE TEMPORARY VIEW base AS
        SELECT
            date_format(Datetime, "yyyy_MM") AS date,
            AVG(CO2_intensity_direct) AS `carbon-intensity`, AVG(Carbon_free_energy_percent) AS cfe
        FROM carbon_data
        GROUP BY date
        ORDER BY date
        """
    )

    by_date = spark.sql(
        """
        SELECT 
            *
        FROM base
        """
    )

    by_carbon_intensity_top = spark.sql(
        """
        SELECT
            *
        FROM base
        ORDER BY `carbon-intensity` DESC
        LIMIT 5
        """
    )

    by_carbon_intensity_bottom = spark.sql(
        """
        SELECT
            *
        FROM base
        ORDER BY `carbon-intensity` ASC
        LIMIT 5
        """
    )

    by_cfe_top = spark.sql(
        """
        SELECT
            *
        FROM base
        ORDER BY cfe DESC
        LIMIT 5
        """
    )

    by_cfe_bottom = spark.sql(
        """
        SELECT
            *
        FROM base
        ORDER BY cfe ASC
        LIMIT 5
        """
    )

    return (
        by_date,
        by_carbon_intensity_top,
        by_carbon_intensity_bottom,
        by_cfe_top,
        by_cfe_bottom,
    )
