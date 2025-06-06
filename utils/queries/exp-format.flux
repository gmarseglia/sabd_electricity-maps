import "array"

groupCols = ["_measurement", "api", "format"]
keepCols = groupCols |> array.concat(v: ["mean", "stddev", "count"])
sortGroupCols = ["_measurement", "api"]

base = from(bucket: "mybucket")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r.api == "df" or r.api == "sql")  
  |> filter(fn: (r) => r.cache != "True")
  |> filter(fn: (r) => r.custom == "country")  

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
    on: groupCols,
    method: "inner"
)
  |> group(columns: sortGroupCols)
  |> sort(columns: ["mean"], desc: false) 
  |> yield()