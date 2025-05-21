from pyspark.sql import SparkSession
from tabulate import tabulate

from custom_formatter import QUERY_1_COLUMNS
from query1 import query1
from query2 import query2

import argparse

if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--mode', type=str, default='local')
    arg_parser.add_argument('--q1', type=bool, default=True)
    arg_parser.add_argument('--q2', type=bool, default=True)
    arg_parser.add_argument('--collect', type=bool, default=True)
    arg_parser.add_argument('--save', type=bool, default=True)
    args = arg_parser.parse_args()

    if args.mode == 'local':
        PREFIX = '..'
    elif args.mode == 'hdfs':
        PREFIX = 'hdfs://master:54310'

    ITALY_HOURLY_FILE = f"{PREFIX}/dataset/combined/combined_dataset-italy_hourly.csv"
    SWEDEN_HOURLY_FILE = f"{PREFIX}/dataset/combined/combined_dataset-sweden_hourly.csv"

    if not args.q1 and not args.q2:
        raise Exception("At least one query must be selected")

    spark = SparkSession \
        .builder \
        .appName("SABD - Electricy Maps") \
        .getOrCreate()

    sc = spark.sparkContext

    sc.setLogLevel("WARN")

    if args.q1:
        result1 = query1(sc, italy_file=ITALY_HOURLY_FILE,
                         sweden_file=SWEDEN_HOURLY_FILE)

        if args.collect:
            print(tabulate(result1.collect(), headers=QUERY_1_COLUMNS))

        if args.save:
            result1.toDF(QUERY_1_COLUMNS) \
                .coalesce(1) \
                .write \
                .mode('overwrite') \
                .csv(f'{PREFIX}/results/query_1', header=True)

    if args.q2:
        result21, result22 = query2(spark, ITALY_HOURLY_FILE)

        if args.collect:
            print(tabulate(result21.collect(), headers=QUERY_1_COLUMNS))
            print(tabulate(result22.collect(), headers=QUERY_1_COLUMNS))

        if args.save:
            result21.write.mode('overwrite').csv(
                f'{PREFIX}/results/query_2-by_direct-no_coalesce', header=True)
            result22.write.mode('overwrite').csv(
                f'{PREFIX}/results/query_2-by_free-no_coalesce', header=True)

    spark.stop()
