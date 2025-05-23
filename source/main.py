from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql import SparkSession
from tabulate import tabulate

from custom_formatter import QUERY_1_COLUMNS, QUERY_2_COLUMNS
from query1 import query1
from query2 import query2

import time
import argparse

if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--mode", type=str, default="local")
    arg_parser.add_argument("--api", type=str, default="default")
    arg_parser.add_argument("--q1", action="store_true")
    arg_parser.add_argument("--q2", action="store_true")
    arg_parser.add_argument("--collect", action="store_true")
    arg_parser.add_argument("--save-hdfs", dest="save_hdfs", action="store_true")
    arg_parser.add_argument("--save-influx", dest="save_influx", action="store_true")
    arg_parser.add_argument("--timed", action="store_true")
    arg_parser.add_argument("--debug", action="store_true")
    args = arg_parser.parse_args()

    if args.mode == "local":
        PREFIX = ".."
        INFLUX_HOST = "localhost"
    elif args.mode == "composed":
        PREFIX = "hdfs://master:54310"
        INFLUX_HOST = "influxdb"

    ITALY_HOURLY_FILE = f"{PREFIX}/dataset/combined/combined_dataset-italy_hourly.csv"
    SWEDEN_HOURLY_FILE = f"{PREFIX}/dataset/combined/combined_dataset-sweden_hourly.csv"

    if not args.q1 and not args.q2:
        raise Exception("At least one query must be selected")

    if args.timed:
        t_q1 = {}
        t_q2 = {}

    """
    Spark setup
    """
    spark = SparkSession.builder.appName("SABD - Electricy Maps").getOrCreate()
    sc = spark.sparkContext
    # sc.setLogLevel("WARN")

    """
    InfluxDB setup
    """
    if args.save_influx:
        bucket = "mybucket"
        org = "myorg"
        token = "mytoken"
        url = f"http://{INFLUX_HOST}:8086"
        client = InfluxDBClient(url=url, token=token, org=org)
        write_api = client.write_api(write_options=SYNCHRONOUS)

    """
    Query 1
    """
    if args.q1:
        """
        Build query
        """
        result1 = query1(
            spark, italy_file=ITALY_HOURLY_FILE, sweden_file=SWEDEN_HOURLY_FILE, api=args.api
        )

        if args.timed:
            t_q1["query_start"] = time.perf_counter()
            result1.collect()
            t_q1["query_end"] = time.perf_counter()
            t_q1["query_duration"] = round(t_q1["query_end"] - t_q1["query_start"], 3)
            print(f"Query 1 took {t_q1['query_duration']} seconds")

        """
        Collect results
        """
        if args.collect:
            print(tabulate(result1.collect(), headers=QUERY_1_COLUMNS, tablefmt="grid"))

        """
        Save results to HDFS
        """
        if args.save_hdfs:
            t_q1["hdfs_start"] = time.perf_counter()
            result1.toDF(QUERY_1_COLUMNS).coalesce(1).write.mode("overwrite").csv(
                f"{PREFIX}/results/query_1", header=True
            )
            t_q1["hdfs_end"] = time.perf_counter()
            t_q1["hdfs_duration"] = round(t_q1["hdfs_end"] - t_q1["hdfs_start"], 3)
            print(f"HDFS took {t_q1['hdfs_duration']} seconds")

        """
        Save results to InfluxDB
        """
        if args.save_influx:
            t_q1["influx_start"] = time.perf_counter()
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
            t_q1["influx_end"] = time.perf_counter()
            t_q1["influx_duration"] = round(
                t_q1["influx_end"] - t_q1["influx_start"], 3
            )
            print(f"Influx took {t_q1['influx_duration']} seconds")

        """
        Save timed results to InfluxDB
        """
        if args.timed and args.save_influx:
            point = (
                Point("t_q1")
                .time(datetime.now(timezone.utc), write_precision=WritePrecision.MS)
                .tag("mode", args.mode)
            )
            point.field("query_duration", t_q1["query_duration"])
            if args.save_hdfs:
                point.field("hdfs_duration", t_q1["hdfs_duration"])
            point.field("influx_duration", t_q1["influx_duration"])
            write_api.write(bucket=bucket, org=org, record=point)

    """
    Query 2
    """
    if args.q2:
        result21, result22 = query2(spark, ITALY_HOURLY_FILE, args.api)

        if args.timed:
            t_q2["query_start"] = time.perf_counter()
            result21.collect()
            result22.collect()
            t_q2["query_end"] = time.perf_counter()
            t_q2["query_duration"] = round(t_q2["query_end"] - t_q2["query_start"], 3)
            print(f"Query 2 took {t_q2['query_duration']} seconds")

        if args.collect:
            print(
                tabulate(result21.collect(), headers=QUERY_2_COLUMNS, tablefmt="grid")
            )
            print(
                tabulate(result22.collect(), headers=QUERY_2_COLUMNS, tablefmt="grid")
            )

        if args.save_hdfs:
            t_q2["hdfs_start"] = time.perf_counter()
            result21.write.mode("overwrite").csv(
                f"{PREFIX}/results/query_2-by_direct-no_coalesce", header=True
            )
            result22.write.mode("overwrite").csv(
                f"{PREFIX}/results/query_2-by_free-no_coalesce", header=True
            )
            t_q2["hdfs_end"] = time.perf_counter()
            t_q2["hdfs_duration"] = round(t_q2["hdfs_end"] - t_q2["hdfs_start"], 3)
            print(f"HDFS took {t_q2['hdfs_duration']} seconds")

        if args.save_influx:
            t_q2["influx_start"] = time.perf_counter()
            for row in result21.collect():
                point = (
                    Point("query_2")
                    .field("co2_intensity", row["avg_CO2_intensity_direct"])
                    .field("avg_c02_free", row["avg_carbon_free_energy"])
                    .time(datetime.strptime(f"{row[0]}-{row[1]}", "%Y-%m"))
                )
                write_api.write(bucket=bucket, org=org, record=point)
            t_q2["influx_end"] = time.perf_counter()
            t_q2["influx_duration"] = round(
                t_q2["influx_end"] - t_q2["influx_start"], 3
            )
            print(f"Influx took {t_q2['influx_duration']} seconds")

        if args.timed and args.save_influx:
            point = (
                Point("t_q2")
                .time(datetime.now(timezone.utc), write_precision=WritePrecision.MS)
                .tag("mode", args.mode)
            )
            point.field("query_duration", t_q2["query_duration"])
            if args.save_hdfs:
                point.field("hdfs_duration", t_q2["hdfs_duration"])
            point.field("influx_duration", t_q2["influx_duration"])

            write_api.write(bucket=bucket, org=org, record=point)

    spark.stop()
