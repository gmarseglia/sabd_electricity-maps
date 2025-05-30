from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, split
from tabulate import tabulate

from custom_formatter import *


def print_debug(data, name, debug):
    if debug:
        print(f"{name}: {data.take(1)}")


def query1(
    spark: SparkSession,
    italy_file: str,
    sweden_file: str,
    api: str,
    use_cache: bool = True,
    debug: bool = False,
):
    if api == "default" or api == "rdd":
        return query_1_rdd(spark, italy_file, sweden_file, use_cache, debug)

    if api == "df":
        return query_1_df(spark, italy_file, sweden_file, use_cache, debug)

    if api == "sql":
        return query_1_sql(spark, italy_file, sweden_file)

    raise Exception("API not supported")


def query_1_rdd(
    spark: SparkSession,
    italy_file: str,
    sweden_file: str,
    use_cache: bool = True,
    debug: bool = False,
):
    # Read data
    if italy_file.endswith(".csv") and sweden_file.endswith(".csv"):
        sc = spark.sparkContext
        raw_italy = sc.textFile(italy_file)
        raw_sweden = sc.textFile(sweden_file)
    else:
        raise Exception(
            f"Invalid file format for RDD API implementation: {italy_file} and {sweden_file}"
        )

    # Combine data
    combined = raw_italy.union(raw_sweden)
    print_debug(combined, "combined", debug)

    # Query 1.1: Average "CO2 Intensity" and "CO2 Free" by year ad by country
    base = combined.map(
        lambda x: (
            (get_country(x), get_year(x)),
            (get_co2_intensity(x), get_c02_free(x), 1),
        )
    )
    if use_cache:
        base = base.cache()
    print_debug(base, "base", debug)

    avg_by_country = base.reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])
    ).map(lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2])))
    print_debug(avg_by_country, "avg_by_country", debug)

    min_by_country = base.reduceByKey(lambda x, y: (min(x[0], y[0]), min(x[1], y[1])))
    print_debug(min_by_country, "min_by_country", debug)

    max_by_country = base.reduceByKey(lambda x, y: (max(x[0], y[0]), max(x[1], y[1])))
    print_debug(max_by_country, "max_by_country", debug)

    result = (
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
        # Use project format
        .map(
            lambda x: (
                x[1],
                shorten_country(str(x[0])),
                x[2],
                x[3],
                x[4],
                x[5],
                x[6],
                x[7],
            )
        )
    )
    print_debug(result, "result", debug)

    return result


def query_1_df(
    spark: SparkSession,
    italy_file: str,
    sweden_file: str,
    use_cache: bool = True,
    debug: bool = False,
):
    # Read data
    if italy_file.endswith(".csv") and sweden_file.endswith(".csv"):
        raw_italy = spark.read.csv(italy_file, header=False, inferSchema=True).toDF(
            *COLUMN_NAMES_RAW
        )
        raw_sweden = spark.read.csv(sweden_file, header=False, inferSchema=True).toDF(
            *COLUMN_NAMES_RAW
        )
    elif italy_file.endswith(".parquet") and sweden_file.endswith(".parquet"):
        raw_italy = spark.read.parquet(italy_file)
        raw_sweden = spark.read.parquet(sweden_file)
    elif italy_file.endswith(".avro") and sweden_file.endswith(".avro"):
        raw_italy = spark.read.format("avro").load(italy_file)
        raw_sweden = spark.read.format("avro").load(sweden_file)
    else:
        raise Exception("Invalid file format: {italy_file} and {sweden_file}")

    # Combine data
    combined = raw_italy.union(raw_sweden)
    print_debug(combined, "combined", debug)

    base = combined.withColumn("Year", split(col("Datetime"), "-").getItem(0)).select(
        *COLUMN_NAMES_DF_1
    )
    if use_cache:
        base = base.cache()
    print_debug(base, "base", debug)

    # Query 1.1: Average "CO2 Intensity" and "CO2 Free" by year ad by country
    result = base.groupBy("Country", "Year").agg(
        F.avg("CO2_intensity_direct").alias(QUERY_1_COLUMNS[2]),
        F.min("CO2_intensity_direct").alias(QUERY_1_COLUMNS[3]),
        F.max("CO2_intensity_direct").alias(QUERY_1_COLUMNS[4]),
        F.avg("Carbon_free_energy_percent").alias(QUERY_1_COLUMNS[5]),
        F.min("Carbon_free_energy_percent").alias(QUERY_1_COLUMNS[6]),
        F.max("Carbon_free_energy_percent").alias(QUERY_1_COLUMNS[7]),
    )

    # Reformat
    udf_shorten_country = F.udf(shorten_country, "string")
    result = (
        result.withColumn("country", udf_shorten_country(result["Country"]))
        .orderBy("country", "Year")
        .withColumnRenamed("Year", "date")
        .select(*QUERY_1_COLUMNS)
    )
    print_debug(result, "result", debug)

    return result


def query_1_sql(spark: SparkSession, italy_file: str, sweden_file: str):
    # Read data
    if italy_file.endswith(".csv") and sweden_file.endswith(".csv"):
        italy_df = spark.read.csv(italy_file, header=False, inferSchema=True).toDF(
            *COLUMN_NAMES_RAW
        )
        sweden_df = spark.read.csv(sweden_file, header=False, inferSchema=True).toDF(
            *COLUMN_NAMES_RAW
        )
    elif italy_file.endswith(".parquet") and sweden_file.endswith(".parquet"):
        italy_df = spark.read.parquet(italy_file)
        sweden_df = spark.read.parquet(sweden_file)
    elif italy_file.endswith(".avro") and sweden_file.endswith(".avro"):
        italy_df = spark.read.format("avro").load(italy_file)
        sweden_df = spark.read.format("avro").load(sweden_file)
    else:
        raise Exception("Invalid file format: {italy_file} and {sweden_file}")

    italy_df.union(sweden_df).createOrReplaceTempView("carbon_data")

    result = spark.sql(
        """
        SELECT
            YEAR(Datetime) AS year, Country AS country,
            AVG(CO2_intensity_direct) AS `carbon-mean`, MIN(CO2_intensity_direct) AS `carbon-min`, MAX(CO2_intensity_direct) AS `carbon-max`,
            AVG(Carbon_free_energy_percent) AS `cfe-mean`,  MIN(Carbon_free_energy_percent) AS `cfe-min`, MAX(Carbon_free_energy_percent) AS `cfe-max`
        FROM carbon_data
        GROUP BY country, year
        ORDER BY country, year
        """
    )

    return result
