from datetime import datetime, timezone
from math import inf
import time
from hdfs import InsecureClient
import argparse


from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from tabulate import tabulate

from custom_formatter import (
    get_c02_free,
    get_co2_intensity,
    get_country,
    get_year,
    shorten_country,
)

ITALY_HOURLY_FILE = "/dataset/combined/combined_dataset-italy_hourly.csv"
SWEDEN_HOURLY_FILE = "/dataset/combined/combined_dataset-sweden_hourly.csv"
Q1_HEADER = "date,country,carbon-mean,carbon-min,carbon-max,cfe-mean,cfe-min,cfe-max\n"


def read_file(path, mode):
    if mode == "local":
        path = ".." + path
        return fs_read(path)
    elif mode == "composed":
        return hdfs_read(path)


def hdfs_read(path):
    with client.read(path, encoding="utf-8") as reader:
        yield from reader


def fs_read(path):
    with open(path, "r") as file:
        yield from file


def q1_result_to_line(results, key):
    return f"{key.split('_')[0]},{key.split('_')[1]},{results['means'][key][0]},{results['mins'][key][0]},{results['maxs'][key][0]},{results['means'][key][1]},{results['mins'][key][1]},{results['maxs'][key][1]}\n"


def q1(path, results):
    """
    Compute mean, max and mix of carbon-intensity and cfe
    Aggregation by country and year
    """
    counts = results["counts"]
    means = results["means"]
    maxs = results["maxs"]
    mins = results["mins"]
    for line in read_file(path, args.mode):
        # print(line.strip())
        country = shorten_country(get_country(line))
        year = get_year(line)
        key = year + "_" + country
        carbon_intensity = get_co2_intensity(line)
        cfe = get_c02_free(line)

        if key not in counts:
            counts[key] = 0
            means[key] = [0, 0]
            maxs[key] = [0, 0]
            mins[key] = [inf, inf]

        counts[key] += 1
        means[key][0] = means[key][0] + (carbon_intensity - means[key][0]) / counts[key]
        means[key][1] = means[key][1] + (cfe - means[key][1]) / counts[key]
        maxs[key][0] = max(maxs[key][0], carbon_intensity)
        maxs[key][1] = max(maxs[key][1], cfe)
        mins[key][0] = min(mins[key][0], carbon_intensity)
        mins[key][1] = min(mins[key][1], cfe)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="local")
    parser.add_argument("--q1", action="store_true")
    parser.add_argument("--timed", action="store_true")
    parser.add_argument("--save-influx", dest="save_influx", action="store_true")
    args = parser.parse_args()

    if args.mode == "local":
        INFLUX_HOST = "localhost"
    if args.mode == "composed":
        # Connect to HDFS - replace with your Namenode URL and port
        client = InsecureClient("http://master:9870", user="spark")
        INFLUX_HOST = "influxdb"

    t_q1 = {}
    t = {"1": t_q1}

    if args.q1:
        results = {
            "counts": {},
            "means": {},
            "maxs": {},
            "mins": {},
        }

        t_q1["query_start"] = time.perf_counter()

        q1(ITALY_HOURLY_FILE, results)
        q1(SWEDEN_HOURLY_FILE, results)

        t_q1["query_end"] = time.perf_counter()

        if args.timed:
            t_q1["query_duration"] = round(t_q1["query_end"] - t_q1["query_start"], 3)
            print(f"Query 1 took {t_q1['query_duration']} seconds")

        if args.mode == "local":
            with open("../results/query_1/baseline/results.csv", "w") as file:
                file.write(Q1_HEADER)
                for key in results["counts"].keys():
                    file.write(q1_result_to_line(results, key))
        elif args.mode == "composed":
            client.makedirs("/results/query_1/baseline")
            with client.write(
                "/results/query_1/baseline/results.csv", overwrite=True
            ) as file:
                # Ensure content is written in bytes
                file.write(Q1_HEADER.encode("utf-8"))
                for key in results["counts"].keys():
                    file.write(q1_result_to_line(results, key).encode("utf-8"))

    if args.timed and args.save_influx:
        bucket = "mybucket"
        org = "myorg"
        token = "mytoken"
        url = f"http://{INFLUX_HOST}:8086"
        client = InfluxDBClient(url=url, token=token, org=org)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        point = (
            Point("t_q1")
            .time(datetime.now(timezone.utc), write_precision=WritePrecision.MS)
            .tag("mode", args.mode)
            .tag("api", "baseline")
            .field("query_duration", t_q1["query_duration"])
        )
        write_api.write(bucket=bucket, org=org, record=point)
