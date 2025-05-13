# Query 2.2
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.dataframe import DataFrame

from source.formatter import *


def query2(spark: SparkSession, italy_file: str):
    df = spark.read.csv(italy_file, header=False, inferSchema=True).toDF(*COLUMN_NAMES)

    df.show()

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
