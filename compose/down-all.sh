#!/bin/bash
./down-spark.sh
docker-compose -f hdfs/docker-compose.yml down -t 2
docker network rm sabd-net