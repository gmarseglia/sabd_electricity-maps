from pyspark.sql import SparkSession
import os
import shutil

COLUMN_NAMES_RAW = [
    "Datetime",
    "Country",
    "Zone_name",
    "Zone_id",
    "CO2_intensity_direct",
    "CO2_intensity_lifecycle",
    "Carbon_free_energy_percent",
    "Renewable_energy_percent",
    "Data_source",
    "Data_estimated",
    "Data_estimation_method",
]


def list_csv_files(directory) -> list[str]:
    return [directory + f for f in os.listdir(directory) if f.endswith(".csv")]


# Create a Spark session
spark = (
    SparkSession.builder.appName("Read and Write Parquet")
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.5")
    .getOrCreate()
)

dataset_dir = "../dataset/combined/"
csv_files = list_csv_files(dataset_dir)

# for format in ["parquet", "avro"]:
for format in ["avro"]:
    for file in csv_files:
        print(f"Processing file: {file}")
        temp_dir = file.replace(".csv", "") + "/"

        shutil.rmtree(temp_dir, ignore_errors=True)

        # Read a file into a DataFrame
        df = spark.read.csv(file, header=False, inferSchema=True).toDF(
            *COLUMN_NAMES_RAW
        )

        # Write the DataFrame to Parquet format
        if format == "parquet":
            df.coalesce(1).write.mode("overwrite").parquet(temp_dir)
        elif format == "avro":
            os.makedirs(temp_dir, exist_ok=True)
            df.coalesce(1).write.mode("overwrite").format("avro").save(temp_dir)
        else:
            raise Exception("Invalid format")

        formatted_file = [
            temp_dir + f for f in os.listdir(temp_dir) if f.endswith(f".{format}")
        ][0]
        print(f"File {formatted_file} created in {temp_dir}")
        # Move the original csv file to a backup dir
        shutil.move(formatted_file, file.replace(".csv", f".{format}"))
        shutil.rmtree(temp_dir)

# Stop the Spark session
spark.stop()
