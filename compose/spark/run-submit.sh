#!/bin/bash

# Compress the source code
source .env
# SOURCE_DIR="/home/giuseppe/SABD/sabd_electricity-maps/source"
(cd ${SOURCE_DIR} && rm -f source.zip && zip -r source.zip *)

# Run the job
docker exec -t spark-client /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files source/source.zip \
    source/main.py --mode composed --collect --no-cache --format csv --q1 --api rdd

docker exec -t spark-client /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files source/source.zip \
    source/main.py --mode composed --collect --no-cache --format csv --q1 --api df

docker exec -t spark-client /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files source/source.zip \
    source/main.py --mode composed --collect --no-cache --format csv --q1 --api sql  

docker exec -t spark-client /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files source/source.zip \
    source/main.py --mode composed --collect --no-cache --format csv --q2 --api rdd

docker exec -t spark-client /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files source/source.zip \
    source/main.py --mode composed --collect --no-cache --format csv --q2 --api df

docker exec -t spark-client /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files source/source.zip \
    source/main.py --mode composed --collect --no-cache --format csv --q2 --api sql

# Remove the source code
(cd ${SOURCE_DIR} && rm -f source.zip)
