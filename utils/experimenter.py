import docker
import os
import zipfile
import time
from itertools import product


def create_cmd(
    query,
    api,
    format,
    mode="composed",
):
    if api == "baseline":
        return (
            "python3"
            " source/baseline.py"
            f" --mode {mode}"
            " --save-fs"
            " --save-influx"
            " --timed"
            f" --q{query}"
        )
    else:
        return (
            "/opt/spark/bin/spark-submit"
            " --master spark://spark-master:7077"
            " --deploy-mode client"
            " --py-files source/source.zip"
            " source/main.py"
            f" --mode {mode}"
            " --save-fs"
            " --save-influx"
            " --timed"
            f" --q{query}"
            f" --api {api}"
            f" --format {format}"
        )


EXPERIMENT_1 = product(["1", "2"], ["baseline", "rdd", "df", "sql"], ["csv"])

SOURCE_DIR = "/home/giuseppe/SABD/sabd_electricity-maps/source"
RESULTS_DIR = "/home/giuseppe/SABD/sabd_electricity-maps/results/experiments"
zip_path = os.path.join(SOURCE_DIR, "source.zip")

# Change to the source directory
os.chdir(SOURCE_DIR)

# Remove existing ZIP file if it exists
if os.path.exists(zip_path):
    os.remove(zip_path)

# Create a new ZIP file with all contents of the directory
with zipfile.ZipFile("source.zip", "w", zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk("."):
        for file in files:
            filepath = os.path.join(root, file)
            zipf.write(filepath, arcname=filepath)

docker_client = docker.from_env()
spark_client = docker_client.containers.get("spark-client")

cmd = (
    "/opt/spark/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --deploy-mode client"
    " --py-files source/source.zip"
    " source/main.py"
    " --mode composed"
    " --save-fs"
    " --save-influx"
    " --timed"
    " --q1"
    " --api rdd"
    " --format csv"
)

short_cmd = cmd[176:].replace(" ", "_")
print(f"Running command: {short_cmd}")

start = time.perf_counter()

exec_result = spark_client.exec_run(cmd)

end = time.perf_counter()
print(f"Execution took {round(end - start, 3)} seconds")

os.makedirs(RESULTS_DIR, exist_ok=True)
os.chdir(RESULTS_DIR)
with open(f"results{short_cmd}.txt", "a") as f:
    f.write(exec_result.output.decode("utf-8") + "\n")

# Clean up
os.remove(zip_path)
