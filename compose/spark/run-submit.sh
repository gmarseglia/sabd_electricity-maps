#!/bin/bash

# Compress the source code
SOURCE_DIR="/home/giuseppe/SABD/sabd_electricity-maps/source"
(cd ${SOURCE_DIR} && rm -f source.zip && zip -r source.zip *)

# Run the job
docker exec -t spark-client /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files source/source.zip \
    source/main.py --mode composed --q1 --save-influx --timed --api sql --format parquet


# Remove the source code
(cd ${SOURCE_DIR} && rm -f source.zip)
