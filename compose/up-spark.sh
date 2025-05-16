#!/bin/bash

# Create the docker network
docker network create --driver bridge sabd-net

# Start Spark
docker-compose -f spark/docker-compose.yml up -d