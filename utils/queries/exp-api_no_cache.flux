import "array"

groupCols = ["_measurement", "api"]
keepCols = groupCols |> array.concat(v: ["mean", "stddev", "count"])
sortGroupCols = ["_measurement"]

base = from(bucket: "mybucket")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r.cache == "False" or r.api == "baseline" or r.api == "sql")  

meanData = base
  |> group(columns: groupCols)
  |> aggregateWindow(every: 1y, fn: mean, createEmpty: false)

stdDevData = base
  |> group(columns: groupCols)
  |> aggregateWindow(every: 1y, fn: stddev, createEmpty: false)

meanAndStdDevData = join(
    tables: {t1: meanData, t2: stdDevData},
    on: groupCols,
    method: "inner"
)
  |> rename(columns: {_value_t1: "mean", _value_t2: "stddev"})
  |> keep(columns: keepCols)

countData = base
  |> group(columns: groupCols)
  |> aggregateWindow(every: 1y, fn: count, createEmpty: false)
  |> rename(columns: {_value: "count"})
  |> keep(columns: keepCols)

finalData = join(
    tables: {t1: meanAndStdDevData, t2: countData},
    on: groupCols, // Specify the field to join on
    method: "inner" // "inner" join is the default
)
  |> group(columns: sortGroupCols)
  |> sort(columns: ["mean"], desc: false) 
  |> yield()