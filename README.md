# sabd_elecrticity-maps

A full description of the project is in `Report\Relazione SABD 1 - 0350066.pdf`.

## Table of Contents
- [Data Ingestion](#data-ingestion)
- [Run locally](#run-locally)
- [Initialize cluster](#initialize-cluster)
- [Run in cluster](#run-in-cluster)
- [Run experiments](#run-experiments)

## Data Ingestion

1.  Ensure that the requirements in `requirements/data_ingestion_requirements.txt` are respected.

2.  Do the data gather phase:
```bash
cd utils/
python3 url_getter.py
./downloader.sh
./combiner.sh
python3 converter
```

## Run locally

1.  Ensure that the requirements in `requirements/run_local_requirements.txt` are respected.

2.  Run locally:
```bash
cd source/
python3 main.py --mode local --q1 --collect --save-fs
```

## Initialize cluster

1.  In `compose/spark/.env`, change `SOURCE_DIR`.

2.  In `compose/hdfs/.env`, change `DATA_DIR`.

3.  Create the cluster:
```bash
cd compose/
./up-all.sh
```

4.  Destory the cluster:
```bash
./down-all.sh
```

## Run in cluster

1.  Create the cluster.

2.  Either run `docker exec -it spark-client bash`, and then:
```bash
spark-client /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files source/source.zip \
    source/main.py --mode composed --collect --no-cache --format csv --q1 --api rdd
```

3. Or use the script `compose/spark/run-submit.sh`.

## Run experiments

1.  Ensure that the requirements in `requirements/experimenter_requirements.txt` are respected.

2.  Change `SOURCE_DIR` and `OUTPUT_DIR` in `utils/experimenter.py`.  

2.  Run the experimenter script:
```bash
cd utils/
python3 experimenter.py
```