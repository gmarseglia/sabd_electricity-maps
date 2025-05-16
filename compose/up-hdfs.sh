#!/bin/bash

# Create the docker network
docker network create --driver bridge sabd-net

# Build the image
docker build -f hdfs/Dockerfile -t gm/hadoop hdfs/

# Start HDFS
docker-compose -f hdfs/docker-compose.yml up -d
# Format the namenode
docker exec -t master hdfs namenode -format
# Start DFS
docker exec -t master /usr/local/hadoop/sbin/start-dfs.sh
# Put the data in HDFS
docker exec -t master hdfs dfs -put /app/dataset/ /
docker exec -t master hdfs dfs -mkdir /results
docker exec -t master hdfs dfs -setfacl -m user:spark:rwx /results/
echo "HDFS Setup: Done"