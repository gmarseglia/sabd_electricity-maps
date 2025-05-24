from math import inf
from hdfs import InsecureClient
import argparse

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


def read_file(path, mode):
    if mode == "local":
        path = ".." + path
        return fs_read(path)
    elif mode == "composed":
        return hdfs_read(path)


def hdfs_read(path):
    # Connect to HDFS - replace with your Namenode URL and port
    client = InsecureClient("http://master:9870", user="spark")
    with client.read(path, encoding="utf-8") as reader:
        yield from reader


def fs_read(path):
    with open(path, "r") as file:
        yield from file


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
    args = parser.parse_args()

    if args.q1:
        results = {
            "counts": {},
            "means": {},
            "maxs": {},
            "mins": {},
        }

        q1(ITALY_HOURLY_FILE, results)
        q1(SWEDEN_HOURLY_FILE, results)

        with open("../results/query_1/baseline/results.csv", "w") as file:
            file.write(
                "date,country,carbon-mean,carbon-min,carbon-max,cfe-mean,cfe-min,cfe-max\n"
            )

            for key in results["counts"].keys():
                file.write(
                    f"{key.split('_')[0]},{key.split('_')[1]},{results['means'][key][0]},{results['mins'][key][0]},{results['maxs'][key][0]},{results['means'][key][1]},{results['mins'][key][1]},{results['maxs'][key][1]}\n"
                )
