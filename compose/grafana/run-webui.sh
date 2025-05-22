#!/bin/bash
docker run -d --rm --name grafana -p 3000:3000 -v grafana_data:/opt/bitnami/grafana --network=sabd-net bitnami/grafana:latest

# from(bucket: "mybucket")
#   |> range(start: 2021-01-01T00:00:00Z, stop: 2025-01-01T00:00:00Z)  // example fixed range: one day
#   |> filter(fn: (r) => r["_measurement"] == "query_2")
#   |> filter(fn: (r) => r["_field"] == "avg_c02_free" or r["_field"] == "co2_intensity")
#   |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
#   |> yield(name: "mean")
