import docker
import os
import zipfile
import time
from itertools import product

SOURCE_DIR = "/home/giuseppe/SABD/sabd_electricity-maps/source"
RESULTS_DIR = "/home/giuseppe/SABD/sabd_electricity-maps/results/experiments"

RUNS_FOR_EXPERIMENT = 1
EXPERIMENT_1 = list(
    product(["1", "2"], ["rdd", "df", "sql", "baseline"], ["csv"], [True])
)
EXPERIMENT_2 = list(product(["1", "2"], ["df", "sql"], ["parquet", "avro"], [True]))
EXPERIMENT_3 = list(product(["1", "2"], ["rdd", "df"], ["csv"], [False]))
EXPERIMENTS = EXPERIMENT_1 + EXPERIMENT_2 + EXPERIMENT_3


def create_cmd(
    query,
    api,
    format,
    cache,
    mode="composed",
):
    str_cache = " --no-cache" if not cache else ""
    short_str_cache = "-no-cache" if not cache else ""
    if api == "baseline":
        return (
            "python3" " source/baseline.py" f" --mode {mode}"
            # " --save-fs"
            " --save-influx" " --timed" f" --q{query}" f"{str_cache}",
            f"q{query}-baseline-csv{short_str_cache}",
        )
    else:
        return (
            "/opt/spark/bin/spark-submit"
            " --master spark://spark-master:7077"
            " --deploy-mode client"
            " --py-files source/source.zip"
            " source/main.py"
            f" --mode {mode}"
            # " --save-fs"
            " --save-influx"
            " --timed"
            f" --q{query}"
            f" --api {api}"
            f" --format {format}"
            f"{str_cache}",
            f"q{query}-{api}-{format}{short_str_cache}",
        )


def execute_cmd(
    cmd, short_cmd, container, write_result=False, recreate_zip=False, delete_zip=False
):
    zip_path = os.path.join(SOURCE_DIR, "source.zip")
    # Change to the source directory
    os.chdir(SOURCE_DIR)

    # Remove existing ZIP file if it exists
    if recreate_zip and os.path.exists(zip_path):
        os.remove(zip_path)

    # Create a new ZIP file with all contents of the directory
    if not os.path.exists(zip_path):
        with zipfile.ZipFile("source.zip", "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk("."):
                for file in files:
                    filepath = os.path.join(root, file)
                    zipf.write(filepath, arcname=filepath)

    print(f"Running command: {cmd}")

    return

    start = time.perf_counter()

    exec_result = container.exec_run(cmd)

    end = time.perf_counter()
    duration = round(end - start, 3)
    print(f"Execution took {pretty_duration(duration)}")

    if write_result:
        # Create the results directory
        os.makedirs(RESULTS_DIR, exist_ok=True)
        os.chdir(RESULTS_DIR)
        with open(f"result_{short_cmd}.txt", "w") as f:
            f.write(exec_result.output.decode("utf-8") + "\n")

    # Clean up
    if delete_zip:
        os.remove(zip_path)

    return duration


def pretty_duration(seconds):
    ms = f"{int(round(seconds % 1, 3) * 1000):03}"
    seconds = int(seconds)
    hours = f"{seconds // 3600:02}"
    minutes = f"{seconds // 60 % 60:02}"
    sseconds = f"{seconds % 60:02}"
    if seconds < 5:
        return f"{hours}:{minutes}:{sseconds}.{ms}"
    else:
        return f"{hours}:{minutes}:{sseconds}"


if __name__ == "__main__":
    docker_client = docker.from_env()
    spark_client_container = docker_client.containers.get("spark-client")

    total_runs = RUNS_FOR_EXPERIMENT * len(EXPERIMENTS)
    start = time.perf_counter()

    completed_runs = 0
    for n in range(RUNS_FOR_EXPERIMENT):
        e = 0
        for experiment in EXPERIMENTS:
            e += 1
            cmd, short_cmd = create_cmd(*experiment)
            short_cmd = f"{short_cmd}_{n}"

            print(
                f"Experiment {short_cmd}::{n}-{e}/{RUNS_FOR_EXPERIMENT}-{len(EXPERIMENTS)}"
            )

            duration = execute_cmd(
                cmd,
                short_cmd,
                spark_client_container,
                write_result=True,
                recreate_zip=False,
                delete_zip=False,
            )

            completed_runs += 1
            total_duration = round(time.perf_counter() - start, 3)
            estimated_total_duration = total_duration * total_runs / completed_runs
            eta = round(estimated_total_duration - total_duration, 3)

            print(
                f"Completed: {completed_runs}/{total_runs} in {pretty_duration(total_duration)}"
            )
            print(
                f"Remaining: {total_runs - completed_runs}/{total_runs} in {pretty_duration(eta)}"
            )
            print("-" * 80)
