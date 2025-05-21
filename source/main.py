from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql import SparkSession
from tabulate import tabulate

from custom_formatter import QUERY_1_COLUMNS, QUERY_2_COLUMNS
from query1 import query1
from query2 import query2

import argparse

if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--mode", type=str, default="local")
    arg_parser.add_argument("--q1", action="store_true")
    arg_parser.add_argument("--q2", action="store_true")
    arg_parser.add_argument("--collect", action="store_true")
    arg_parser.add_argument("--save-hdfs", dest="save_hdfs", action="store_true")
    arg_parser.add_argument("--save-influx", dest="save_influx", action="store_true")
    args = arg_parser.parse_args()

    if args.mode == "local":
        PREFIX = ".."
        INFLUX_HOST = "localhost"
    elif args.mode == "hdfs":
        PREFIX = "hdfs://master:54310"
        INFLUX_HOST = "influxdb"

    ITALY_HOURLY_FILE = f"{PREFIX}/dataset/combined/combined_dataset-italy_hourly.csv"
    SWEDEN_HOURLY_FILE = f"{PREFIX}/dataset/combined/combined_dataset-sweden_hourly.csv"

    if not args.q1 and not args.q2:
        raise Exception("At least one query must be selected")

    spark = SparkSession.builder.appName("SABD - Electricy Maps").getOrCreate()

    sc = spark.sparkContext

    # sc.setLogLevel("WARN")

    if args.save_influx:
        bucket = "mybucket"
        org = "myorg"
        token = "mytoken"
        url = f"http://{INFLUX_HOST}:8086"
        client = InfluxDBClient(url=url, token=token, org=org)
        write_api = client.write_api(write_options=SYNCHRONOUS)

    if args.q1:
        result1 = query1(
            sc, italy_file=ITALY_HOURLY_FILE, sweden_file=SWEDEN_HOURLY_FILE
        )

        if args.collect:
            print(tabulate(result1.collect(), headers=QUERY_1_COLUMNS, tablefmt="grid"))

        if args.save_hdfs:
            result1.toDF(QUERY_1_COLUMNS).coalesce(1).write.mode("overwrite").csv(
                f"{PREFIX}/results/query_1", header=True
            )

        if args.save_influx:
            for row in result1.collect():
                point = (
                    Point("query_1")
                    .tag("country", row[0])
                    .field("avg_co2_intensity", row[2])
                    .field("min_co2_intensity", row[3])
                    .field("max_co2_intensity", row[4])
                    .field("avg_c02_free", row[5])
                    .field("min_c02_free", row[6])
                    .field("max_c02_free", row[7])
                    .time(datetime.strptime(row[1], "%Y"))
                )
                write_api.write(bucket=bucket, org=org, record=point)

    if args.q2:
        result21, result22 = query2(spark, ITALY_HOURLY_FILE)

        if args.collect:
            print(
                tabulate(result21.collect(), headers=QUERY_2_COLUMNS, tablefmt="grid")
            )
            print(
                tabulate(result22.collect(), headers=QUERY_2_COLUMNS, tablefmt="grid")
            )

        if args.save_hdfs:
            result21.write.mode("overwrite").csv(
                f"{PREFIX}/results/query_2-by_direct-no_coalesce", header=True
            )
            result22.write.mode("overwrite").csv(
                f"{PREFIX}/results/query_2-by_free-no_coalesce", header=True
            )

        if args.save_influx:
            for row in result21.collect(): 
                point = (
                    Point("query_2")
                    .field("co2_intensity", row['avg_CO2_intensity_direct'])
                    .field("avg_c02_free", row['avg_carbon_free_energy'])
                    .time(datetime.strptime(f"{row[0]}-{row[1]}", "%Y-%m"))
                )
                write_api.write(bucket=bucket, org=org, record=point)

    spark.stop()
