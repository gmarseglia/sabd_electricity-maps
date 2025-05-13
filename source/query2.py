# Query 2.2
from audioop import avgpp

from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import col, split, avg, desc
from tabulate import tabulate

from source.formatter import *

def query2(spark: SparkSession, italy_file: str):
    df = spark.read.csv(italy_file, header=False, inferSchema=True).toDF(*COLUMN_NAMES_RAW)

    df.show(5)

    df = df.withColumn('Year', split(col("Datetime"), "-").getItem(0)) \
        .withColumn('Month', split(col("Datetime"), "-").getItem(1)) \
        .cache()

    df = df.select(*COLUMN_NAMES_DF)
    df.show(5)

    df_avg = df.groupBy('Year', 'Month').agg(
        avg('CO2_intensity_direct').alias('avg_CO2_intensity_direct'),
        avg('Carbon_free_energy_percent').alias('avg_carbon_free_energy'),
    )

    # df_avg.show()

    df_sorted_by_direct = df_avg.sort(desc('avg_CO2_intensity_direct'))
    highest_5_by_direct = df_sorted_by_direct.head(5)
    lowest_5_by_direct = df_sorted_by_direct.tail(5)

    df_sorted_by_free = df_avg.sort(desc('avg_carbon_free_energy'))
    highest_5_by_free = df_sorted_by_free.head(5)
    lowest_5_by_free = df_sorted_by_free.tail(5)

    print(tabulate(highest_5_by_direct, headers=df_avg.columns, tablefmt='psql'))

    return

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

    # by_c02_intensity = hourly_lines \
    #     .filter(lambda x: get_country(x) == "Italy") \
    #     .map(lambda x: (get_year(x), get_month(x), x)) \
    #     .map(lambda x: (get_co2_intensity(x[2]), x[0], x[1], x[2])) \
    #     .cache()
    #
    # best_5 = by_c02_intensity.takeOrdered(5, lambda x: x[0])
    # worst_5 = by_c02_intensity.takeOrdered(5, lambda x: -x[0])

    return
