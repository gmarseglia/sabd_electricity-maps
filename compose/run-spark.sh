#!/bin/bash
docker-compose -f spark/docker-compose.yml up -d
docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077