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
spark = SparkSession.builder.appName("Read and Write Parquet").getOrCreate()

dataset_dir = "../dataset/combined/"
csv_files = list_csv_files(dataset_dir)

for file in csv_files:
    print(f"Processing file: {file}")
    parquet_dir = file.replace(".csv", "") + "/"
    
    shutil.rmtree(parquet_dir)
    
    # Read a file into a DataFrame
    df = spark.read.csv(file, header=False, inferSchema=True).toDF(*COLUMN_NAMES_RAW)

    # Write the DataFrame to Parquet format
    df.coalesce(1).write.mode("overwrite").parquet(parquet_dir)

    parquet_file = [
        parquet_dir + f for f in os.listdir(parquet_dir) if f.endswith(".parquet")
    ][0]
    print(f"File {parquet_file} created in {parquet_dir}")
    # Move the original csv file to a backup dir
    shutil.move(parquet_file, file.replace(".csv", ".parquet"))
    shutil.rmtree(parquet_dir)

# Stop the Spark session
spark.stop()
