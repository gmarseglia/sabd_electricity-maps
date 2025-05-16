#!/bin/bash

# Create the docker network
docker network create --driver bridge sabd-net

# Build the image
docker build -f spark/Dockerfile -t gm/spark spark/

# Start Spark
docker-compose -f spark/docker-compose.yml up -d