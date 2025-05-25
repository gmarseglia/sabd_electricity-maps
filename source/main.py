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
    arg_parser.add_argument("--api", type=str, default="rdd")
    arg_parser.add_argument("--q1", action="store_true")
    arg_parser.add_argument("--q2", action="store_true")
    arg_parser.add_argument("--collect", action="store_true")
    arg_parser.add_argument("--save-fs", dest="save_fs", action="store_true")
    arg_parser.add_argument("--save-influx", dest="save_influx", action="store_true")
    arg_parser.add_argument("--timed", action="store_true")
    arg_parser.add_argument("--debug", action="store_true")
    arg_parser.add_argument("--format", type=str, default="csv")
    args = arg_parser.parse_args()

    if args.mode == "local":
        PREFIX = ".."
        INFLUX_HOST = "localhost"
    elif args.mode == "composed":
        PREFIX = "hdfs://master:54310"
        INFLUX_HOST = "influxdb"

    if not args.q1 and not args.q2:
        raise Exception("At least one query must be selected")
    
    if not args.format == "csv" and not args.format == "parquet":
        raise Exception("At least one format must be selected")
    
    ITALY_HOURLY_FILE = f"{PREFIX}/dataset/combined/combined_dataset-italy_hourly.{args.format}"
    SWEDEN_HOURLY_FILE = f"{PREFIX}/dataset/combined/combined_dataset-sweden_hourly.{args.format}"

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
        t_q1["query_start"] = time.perf_counter()

        result1 = query1(
            spark,
            italy_file=ITALY_HOURLY_FILE,
            sweden_file=SWEDEN_HOURLY_FILE,
            api=args.api,
        )

        if args.timed:
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
        if args.save_fs:
            if args.timed:
                t_q1["fs_start"] = time.perf_counter()

            if args.api == "default" or args.api == "rdd":
                result1 = result1.toDF(QUERY_1_COLUMNS)

            result1.coalesce(1).write.mode("overwrite").csv(
                f"{PREFIX}/results/query_1/{args.api}-{args.format}", header=True
            )

            if args.timed:
                t_q1["fs_end"] = time.perf_counter()
                t_q1["fs_duration"] = round(t_q1["fs_end"] - t_q1["fs_start"], 3)
                print(f"FS took {t_q1['fs_duration']} seconds")

        """
        Save results to InfluxDB
        """
        if args.save_influx:
            if args.timed:
                t_q1["influx_start"] = time.perf_counter()
            for row in result1.collect():
                point = (
                    Point("query_1")
                    .tag(QUERY_1_COLUMNS[1], row[1])
                    .field(QUERY_1_COLUMNS[2], row[2])
                    .field(QUERY_1_COLUMNS[3], row[3])
                    .field(QUERY_1_COLUMNS[4], row[4])
                    .field(QUERY_1_COLUMNS[5], row[5])
                    .field(QUERY_1_COLUMNS[6], row[6])
                    .field(QUERY_1_COLUMNS[7], row[7])
                    .time(datetime.strptime(row[0], "%Y"))
                )
                write_api.write(bucket=bucket, org=org, record=point)
            if args.timed:
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
                .tag("api", args.api)
                .tag("format", args.format)
            )
            point.field("query_duration", t_q1["query_duration"])
            if args.save_fs:
                point.field("fs_duration", t_q1["fs_duration"])
            point.field("influx_duration", t_q1["influx_duration"])
            write_api.write(bucket=bucket, org=org, record=point)

    """
    Query 2
    """
    if args.q2:
        t_q2["query_start"] = time.perf_counter()

        result2 = {}
        (
            result2["by_carbon_intensity"],
            result2["by_carbon_intensity_top"],
            result2["by_carbon_intensity_bottom"],
            result2["by_cfe"],
            result2["by_cfe_top"],
            result2["by_cfe_bottom"],
        ) = query2(spark, ITALY_HOURLY_FILE, args.api)

        if args.timed:
            for result in result2.values():
                result.collect()
            t_q2["query_end"] = time.perf_counter()
            t_q2["query_duration"] = round(t_q2["query_end"] - t_q2["query_start"], 3)
            print(f"Query 2 took {t_q2['query_duration']} seconds")

        if args.collect:
            for key in result2.keys():
                print(f"Results for {key}")
                print(
                    tabulate(
                        result2[key].collect(),
                        headers=QUERY_2_COLUMNS,
                        tablefmt="grid",
                    )
                )

        if args.save_fs:
            if args.timed:
                t_q2["fs_start"] = time.perf_counter()

            if args.api == "rdd":
                for key in result2.keys():
                    result2[key] = result2[key].toDF(QUERY_2_COLUMNS)

            for key in result2.keys():
                result2[key].coalesce(1).write.mode("overwrite").csv(
                    f"{PREFIX}/results/query_2/{args.api}-{args.format}/{key}",
                    header=True,
                )

            if args.timed:
                t_q2["fs_end"] = time.perf_counter()
                t_q2["fs_duration"] = round(t_q2["fs_end"] - t_q2["fs_start"], 3)
                print(f"FS took {t_q2['fs_duration']} seconds")

        if args.save_influx:
            if args.timed:
                t_q2["influx_start"] = time.perf_counter()

            for row in result2["by_carbon_intensity"].collect():
                point = (
                    Point("query_2")
                    .field(QUERY_2_COLUMNS[1], row[1])
                    .field(QUERY_2_COLUMNS[2], row[2])
                    .time(datetime.strptime(row[0], "%Y_%m"))
                )
                write_api.write(bucket=bucket, org=org, record=point)

            if args.timed:
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
                .tag("api", args.api)
                .tag("format", args.format)
            )
            point.field("query_duration", t_q2["query_duration"])
            if args.save_fs:
                point.field("fs_duration", t_q2["fs_duration"])
            point.field("influx_duration", t_q2["influx_duration"])

            write_api.write(bucket=bucket, org=org, record=point)

    spark.stop()
