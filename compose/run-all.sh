#!/bin/bash
docker network create --driver bridge sabd-net
docker-compose -f hdfs/docker-compose.yml up -d
docker exec -it master hdfs namenode -format
docker exec -it master /usr/local/hadoop/sbin/start-dfs.sh
./run-spark.sh
