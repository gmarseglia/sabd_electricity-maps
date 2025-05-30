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
        italy_raw = sc.textFile(italy_file)
    else:
        raise Exception(f"Invalid file format for RDD API implementation: {italy_file}")

    base = italy_raw.map(
        lambda x: (
            (get_year(x), get_month(x)),
            (get_co2_intensity(x), get_c02_free(x), 1),
        )
    )
    print_debug(base, "base", debug)

    avg = (
        base.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
        .map(lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2])))
        .map(lambda x: (x[0][0] + "_" + x[0][1], x[1][0], x[1][1]))
    )

    if use_cache:
        avg = avg.cache()
    print_debug(avg, "avg", debug)

    total_count = avg.count()

    by_carbon_intensity = avg.sortBy(lambda x: x[1], ascending=False)
    if use_cache:
        by_carbon_intensity = by_carbon_intensity.cache()

    by_carbon_intensity_top = (
        by_carbon_intensity.zipWithIndex()
        .filter(lambda x: x[1] < 5)
        .map(lambda x: x[0])
    )

    rdd_by_carbon_intensity_bottom = (
        by_carbon_intensity.zipWithIndex()
        .filter(lambda x: x[1] >= total_count - 5)
        .map(lambda x: x[0])
        .sortBy(lambda x: x[1], ascending=True)
    )

    rdd_by_cfe = avg.sortBy(lambda x: x[2], ascending=False)
    if use_cache:
        rdd_by_cfe = rdd_by_cfe.cache()

    rdd_by_cfe_top = (
        rdd_by_cfe.zipWithIndex().filter(lambda x: x[1] < 5).map(lambda x: x[0])
    )

    rdd_by_cfe_bottom = (
        rdd_by_cfe.zipWithIndex()
        .filter(lambda x: x[1] >= total_count - 5)
        .map(lambda x: x[0])
        .sortBy(lambda x: x[2], ascending=True)
    )

    return (
        by_carbon_intensity,
        by_carbon_intensity_top,
        rdd_by_carbon_intensity_bottom,
        rdd_by_cfe,
        rdd_by_cfe_top,
        rdd_by_cfe_bottom,
    )


def query2_df(spark: SparkSession, italy_file: str, use_cache: bool = True):
    if italy_file.endswith(".csv"):
        df = spark.read.csv(italy_file, header=False, inferSchema=True).toDF(
            *COLUMN_NAMES_RAW
        )
    elif italy_file.endswith(".parquet"):
        df = spark.read.parquet(italy_file)
    elif italy_file.endswith(".avro"):
        italy_df = spark.read.format("avro").load(italy_file)
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

    df_avg = df_avg.withColumn(
        "date", F.concat(F.col("Year"), F.lit("_"), F.col("Month"))
    ).select(*QUERY_2_COLUMNS)
    if use_cache:
        df_avg = df_avg.cache()

    df_by_carbon_intensity = df_avg.orderBy(F.col(QUERY_2_COLUMNS[1]).desc())
    if use_cache:
        df_by_carbon_intensity = df_by_carbon_intensity.cache()
    df_by_carbon_intensity_top = df_by_carbon_intensity.limit(5)
    df_by_carbon_intensity_bottom = df_by_carbon_intensity.orderBy(
        F.col(QUERY_2_COLUMNS[1]).asc()
    ).limit(5)

    df_by_cfe = df_avg.orderBy(F.col(QUERY_2_COLUMNS[2]).desc())
    if use_cache:
        df_by_cfe = df_by_cfe.cache()
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


def query2_sql(spark: SparkSession, italy_file: str):
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

    by_carbon_intensity = spark.sql(
        """
        SELECT
            date, `carbon-intensity`, cfe
        FROM base
        ORDER BY `carbon-intensity` DESC
        """
    )

    by_carbon_intensity_top = spark.sql(
        """
        SELECT
            date, `carbon-intensity`, cfe
        FROM base
        ORDER BY `carbon-intensity` DESC
        LIMIT 5
        """
    )

    by_carbon_intensity_bottom = spark.sql(
        """
        SELECT
            date, `carbon-intensity`, cfe
        FROM base
        ORDER BY `carbon-intensity` ASC
        LIMIT 5
        """
    )

    by_cfe = spark.sql(
        """
        SELECT
            date, `carbon-intensity`, cfe
        FROM base
        ORDER BY cfe DESC
        """
    )

    by_cfe_top = spark.sql(
        """
        SELECT
            date, `carbon-intensity`, cfe
        FROM base
        ORDER BY cfe DESC
        LIMIT 5
        """
    )

    by_cfe_bottom = spark.sql(
        """
        SELECT
            date, `carbon-intensity`, cfe
        FROM base
        ORDER BY cfe ASC
        LIMIT 5
        """
    )

    return (
        by_carbon_intensity,
        by_carbon_intensity_top,
        by_carbon_intensity_bottom,
        by_cfe,
        by_cfe_top,
        by_cfe_bottom,
    )
