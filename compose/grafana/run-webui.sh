#!/bin/bash
docker run -d --rm --name grafana -p 3000:3000 --network=sabd-net bitnami/grafana:latest
# docker run -d --name=grafana -p 3000:3000 --network=sabd-net grafana/grafana
# curl -i -L -X GET "http://master:9870/webhdfs/v1/results/query_2-by_free-no_coalesce/part-00000-97b886e7-7c2a-424f-958f-11a1b6ddd25b-c000.csv?op=OPEN"