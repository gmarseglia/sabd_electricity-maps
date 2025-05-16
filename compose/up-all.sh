#!/bin/bash

# Create the docker network
docker network create --driver bridge sabd-net

# Start HDFS
./up-hdfs.sh

# Start Spark
./up-spark.sh
