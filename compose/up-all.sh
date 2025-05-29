#!/bin/bash

# Create the docker network
docker network create --driver bridge sabd-net

# Start HDFS
./up-hdfs.sh

# Start InfluxDB
./up-influxdb.sh

# Start Spark
./up-spark.sh

# Start Grafana
./up-grafana.sh
