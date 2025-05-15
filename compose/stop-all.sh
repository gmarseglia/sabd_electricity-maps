#!/bin/bash
docker-compose -f spark/docker-compose.yml down
docker-compose -f hdfs/docker-compose.yml down
docker network rm sabd-net