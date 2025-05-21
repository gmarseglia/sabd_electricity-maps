#!/bin/bash

# Create the docker network
docker network create --driver bridge sabd-net

docker-compose -f influxdb/docker-compose.yml up -d