#!/bin/bash

# Stop grafana
./down-grafana.sh

# Stop Spark
./down-spark.sh

# Stop InfluxDB
./down-influxdb.sh

# Stop HDFS 
./down-hdfs.sh

# Remove the docker network
docker network rm sabd-net