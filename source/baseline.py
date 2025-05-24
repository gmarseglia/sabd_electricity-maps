from datetime import datetime, timezone
from math import inf
import os
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
    get_month,
    get_year,
    shorten_country,
)

ITALY_HOURLY_FILE = "/dataset/combined/combined_dataset-italy_hourly.csv"
SWEDEN_HOURLY_FILE = "/dataset/combined/combined_dataset-sweden_hourly.csv"
Q1_HEADER = "date,country,carbon-mean,carbon-min,carbon-max,cfe-mean,cfe-min,cfe-max\n"
Q2_HEADER = "date,carbon-intensity,cfe\n"


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


def q2_result_to_line(results, key):
    return f"{key},{results['means'][key][0]},{results['means'][key][1]}\n"


def q2(path, results):
    """
    Compute mean of carbon-intensity and cfe and sort
    Aggregation by country, year and month
    """
    counts = results["counts"]
    means = results["means"]
    for line in read_file(path, args.mode):
        # print(line.strip())
        year = get_year(line)
        month = get_month(line)
        key = year + "_" + month
        carbon_intensity = get_co2_intensity(line)
        cfe = get_c02_free(line)

        if key not in counts:
            counts[key] = 0
            means[key] = [0, 0]

        counts[key] += 1
        means[key][0] = means[key][0] + (carbon_intensity - means[key][0]) / counts[key]
        means[key][1] = means[key][1] + (cfe - means[key][1]) / counts[key]

    # Sort by means
    keys_by_carbon_intensity = sorted(
        means.keys(), key=lambda k: means[k][0], reverse=True
    )
    keys_by_cfe = sorted(means.keys(), key=lambda k: means[k][1], reverse=True)

    results["sorted_by_carbon_intensity"] = keys_by_carbon_intensity
    results["sorted_by_cfe"] = keys_by_cfe


def open_file_by_mode(path, mode):
    if mode == "local":
        return open(".." + path, "w")
    elif mode == "composed":
        return client.write(path, overwrite=True)


def write_file_by_mode(file, content, mode):
    if mode == "local":
        file.write(content)
    elif mode == "composed":
        file.write(content.encode("utf-8"))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="local")
    parser.add_argument("--q1", action="store_true")
    parser.add_argument("--q2", action="store_true")
    parser.add_argument("--timed", action="store_true")
    parser.add_argument("--save-fs", dest="save_fs", action="store_true")
    parser.add_argument("--save-influx", dest="save_influx", action="store_true")
    args = parser.parse_args()

    if args.mode == "local":
        INFLUX_HOST = "localhost"
    if args.mode == "composed":
        # Connect to HDFS - replace with your Namenode URL and port
        client = InsecureClient("http://master:9870", user="spark")
        INFLUX_HOST = "influxdb"

    t_q1 = {}
    t_q2 = {}

    if args.q1:
        results_q1 = {
            "counts": {},
            "means": {},
            "maxs": {},
            "mins": {},
        }

        t_q1["query_start"] = time.perf_counter()

        q1(ITALY_HOURLY_FILE, results_q1)
        q1(SWEDEN_HOURLY_FILE, results_q1)

        t_q1["query_end"] = time.perf_counter()

        if args.timed:
            t_q1["query_duration"] = round(t_q1["query_end"] - t_q1["query_start"], 3)
            print(f"Query 1 took {t_q1['query_duration']} seconds")

        if args.save_fs:
            if args.mode == "local":
                os.makedirs("../results/query_1/baseline", exist_ok=True)
            elif args.mode == "composed":
                client.makedirs("/results/query_1/baseline")

            with open_file_by_mode(
                "/results/query_1/baseline/results.csv", args.mode
            ) as file:
                write_file_by_mode(file, Q1_HEADER, args.mode)
                for key in results_q1["counts"].keys():
                    write_file_by_mode(
                        file, q1_result_to_line(results_q1, key), args.mode
                    )

    if args.q2:
        results_q2 = {
            "counts": {},
            "means": {},
            "sorted_by_carbon_intensity": [],
            "sorted_by_cfe": [],
        }

        t_q2["query_start"] = time.perf_counter()

        q2(ITALY_HOURLY_FILE, results_q2)

        t_q2["query_end"] = time.perf_counter()

        if args.timed:
            t_q2["query_duration"] = round(t_q2["query_end"] - t_q2["query_start"], 3)
            print(f"Query 2 took {t_q2['query_duration']} seconds")

        if args.save_fs:
            if args.mode == "local":
                os.makedirs("../results/query_2/baseline", exist_ok=True)
            elif args.mode == "composed":
                client.makedirs("/results/query_2/baseline")

            with open_file_by_mode(
                "/results/query_2/baseline/by_carbon_intensity-all.csv", args.mode
            ) as file:
                write_file_by_mode(file, Q2_HEADER, args.mode)
                for key in results_q2["sorted_by_carbon_intensity"]:
                    write_file_by_mode(
                        file, q2_result_to_line(results_q2, key), args.mode
                    )

            with open_file_by_mode(
                "/results/query_2/baseline/by_carbon_intensity-top.csv", args.mode
            ) as file:
                write_file_by_mode(file, Q2_HEADER, args.mode)
                for key in results_q2["sorted_by_carbon_intensity"][:5]:
                    write_file_by_mode(
                        file, q2_result_to_line(results_q2, key), args.mode
                    )

            with open_file_by_mode(
                "/results/query_2/baseline/by_carbon_intensity-bottom.csv", args.mode
            ) as file:
                write_file_by_mode(file, Q2_HEADER, args.mode)
                for key in results_q2["sorted_by_carbon_intensity"][-5:]:
                    write_file_by_mode(
                        file, q2_result_to_line(results_q2, key), args.mode
                    )

            with open_file_by_mode(
                "/results/query_2/baseline/by_cfe-all.csv", args.mode
            ) as file:
                write_file_by_mode(file, Q2_HEADER, args.mode)
                for key in results_q2["sorted_by_cfe"]:
                    write_file_by_mode(
                        file, q2_result_to_line(results_q2, key), args.mode
                    )

            with open_file_by_mode(
                "/results/query_2/baseline/by_cfe-top.csv", args.mode
            ) as file:
                write_file_by_mode(file, Q2_HEADER, args.mode)
                for key in results_q2["sorted_by_cfe"][:5]:
                    write_file_by_mode(
                        file, q2_result_to_line(results_q2, key), args.mode
                    )

            with open_file_by_mode(
                "/results/query_2/baseline/by_cfe-bottom.csv", args.mode
            ) as file:
                write_file_by_mode(file, Q2_HEADER, args.mode)
                for key in results_q2["sorted_by_cfe"][-5:]:
                    write_file_by_mode(
                        file, q2_result_to_line(results_q2, key), args.mode
                    )

    if args.timed and args.save_influx:
        bucket = "mybucket"
        org = "myorg"
        token = "mytoken"
        url = f"http://{INFLUX_HOST}:8086"
        client = InfluxDBClient(url=url, token=token, org=org)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        point = (
            Point("t_q2")
            .time(datetime.now(timezone.utc), write_precision=WritePrecision.MS)
            .tag("mode", args.mode)
            .tag("api", "baseline")
            .field("query_duration", t_q2["query_duration"])
        )
        write_api.write(bucket=bucket, org=org, record=point)
