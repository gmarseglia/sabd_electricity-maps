from hdfs import InsecureClient

# Connect to HDFS - replace with your Namenode URL and port
client = InsecureClient('http://master:9870', user='spark')

# Path of the file on HDFS
hdfs_path = '/dataset/combined/combined_dataset-italy_hourly.csv'

# Open the file and read line by line
i = 0
with client.read(hdfs_path, encoding='utf-8') as reader:
    for line in reader:
        print(line.strip())
        i += 1
        
print(f"Read {i} lines")
