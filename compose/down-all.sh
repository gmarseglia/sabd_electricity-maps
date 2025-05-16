#!/bin/bash

# Stop Spark
./down-spark.sh

# Stop HDFS 
./down-hdfs.sh

# Remove the docker network
docker network rm sabd-net