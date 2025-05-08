from pyspark.sql import SparkSession

MONTHLY_FILE = "../dataset/combined/combined_monthly_dataset.csv"
YEARLY_FILE = "../dataset/combined/combined_yearly_dataset.csv"

spark = (SparkSession
         .builder
         .appName("Pyspark hello world")
         .getOrCreate())

sc = spark.sparkContext

monthly_lines = sc.textFile(MONTHLY_FILE)
yearly_lines = sc.textFile(YEARLY_FILE)

italian_monthly_lines = monthly_lines.filter(lambda x: "IT" in x)

print(f"Number of lines for monthly: {monthly_lines.count()}\n"
      f"Number of lines for yearly: {yearly_lines.count()}\n"
      f"Number of lines for italian: {italian_monthly_lines.count()}")