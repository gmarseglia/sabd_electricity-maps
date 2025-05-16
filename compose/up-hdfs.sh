#!/bin/bash

# Create the docker network
docker network create --driver bridge sabd-net

# Start HDFS
docker-compose -f hdfs/docker-compose.yml up -d
# Format the namenode
docker exec -t master hdfs namenode -format
# Start DFS
docker exec -t master /usr/local/hadoop/sbin/start-dfs.sh
# Put the data in HDFS
docker exec -t master hdfs dfs -put /app/dataset/ /
echo "dataset uploaded to HDFS"