---
title: Aggregations and Windows
nav_order: 9
---

# Aggregations and Windows

This page covers grouped aggregation (`group_by`, `rollup`, `cube`), the
[`GroupedData`](#groupeddata) interface, and window functions built with
[`Window` / `WindowSpec`](#window-functions). The API mirrors PySpark closely,
so if you know `DataFrame.groupBy(...).agg(...)` and `Window.partitionBy(...)`
you will feel at home.

All examples assume a session and the functions module are in scope:

```ruby
require "spark-connect"

spark = SparkConnect::SparkSession.builder.remote("sc://localhost:15002").get_or_create
F = SparkConnect::F  # alias for SparkConnect::Functions
```

A sample DataFrame used throughout this page:

```ruby
employees = spark.create_data_frame(
  [
    { dept: "eng",   name: "ana",  salary: 130, year: 2023 },
    { dept: "eng",   name: "ben",  salary: 110, year: 2023 },
    { dept: "eng",   name: "cy",   salary: 150, year: 2024 },
    { dept: "sales", name: "dot",  salary: 90,  year: 2023 },
    { dept: "sales", name: "evan", salary: 120, year: 2024 }
  ]
)
```

## Grouping

`DataFrame#group_by` (aliased `groupBy` / `groupby`) returns a
[`GroupedData`](#groupeddata). Columns may be given as names or `Column`
objects.

```ruby
employees.group_by("dept").count.show
# +-----+-----+
# | dept|count|
# +-----+-----+
# |  eng|    3|
# |sales|    2|
# +-----+-----+
```

`rollup` and `cube` produce multi-dimensional aggregates with subtotals. A
`rollup` of `(a, b)` yields groupings for `(a, b)`, `(a)`, and `()`; a `cube`
additionally yields `(b)`.

```ruby
employees.rollup("dept", "year").sum("salary").order_by("dept", "year").show
employees.cube("dept", "year").count.show
```

To aggregate the whole DataFrame with no grouping columns, call `DataFrame#agg`
directly (it is shorthand for `group_by.agg`):

```ruby
employees.agg(F.sum("salary").alias("total"), F.avg("salary").alias("avg")).show
```

## GroupedData

`GroupedData` exposes the standard aggregates. Each method returns a new
`DataFrame`.

### agg

`agg` accepts either a list of aggregate `Column`s or a single `Hash` mapping
column names to function names.

```ruby
# Column form (most flexible: supports aliases, expressions, multiple stats).
employees.group_by("dept").agg(
  F.avg("salary").alias("avg_salary"),
  F.max("salary").alias("max_salary"),
  F.count("*").alias("headcount")
).show

# Hash form: {column => function_name}.
employees.group_by("dept").agg("salary" => "max", "year" => "min").show
```

### count, sum, avg, max, min

`count` counts rows per group. `sum`, `avg` (aliased `mean`), `max`, and `min`
take one or more numeric column names; with no arguments they apply to every
numeric column.

```ruby
employees.group_by("dept").count.show
employees.group_by("dept").sum("salary").show
employees.group_by("dept").avg("salary").show   # avg and mean are equivalent
employees.group_by("dept").max("salary", "year").show
employees.group_by("dept").min("salary").show
```

### pivot

`pivot` rotates the values of a column into separate output columns. Supplying
the list of values explicitly is faster and deterministic because the client
does not have to scan for distinct values first.

```ruby
# Inferred pivot values.
employees.group_by("dept").pivot("year").sum("salary").show

# Explicit pivot values (recommended in production).
employees.group_by("dept").pivot("year", [2023, 2024]).sum("salary").show
# +-----+----+----+
# | dept|2023|2024|
# +-----+----+----+
# |  eng| 240| 150|
# |sales|  90| 120|
# +-----+----+----+
```

## Window functions

Window functions compute a value for each row over a related set of rows (a
"window") without collapsing them into one row per group. Build a `WindowSpec`
with the `SparkConnect::Window` factory and attach it to an analytic column with
`Column#over`.

`WindowSpec` is immutable: each of `partition_by`, `order_by`, `rows_between`,
and `range_between` returns a new spec, so you can chain them freely.

```ruby
w = SparkConnect::Window.partition_by("dept").order_by(F.col("salary").desc)
```

### Ranking functions

`row_number`, `rank`, and `dense_rank` are no-argument functions; call them and
attach a window with `over`.

```ruby
ranked = employees.select(
  F.col("dept"),
  F.col("name"),
  F.col("salary"),
  F.row_number.over(w).alias("row_num"),
  F.rank.over(w).alias("rank"),
  F.dense_rank.over(w).alias("dense_rank")
)
ranked.show
```

`percent_rank`, `cume_dist`, and `ntile(n)` are also available:

```ruby
employees.select(
  F.col("dept"),
  F.col("salary"),
  F.percent_rank.over(w).alias("pct_rank"),
  F.cume_dist.over(w).alias("cume_dist"),
  F.ntile(2).over(w).alias("half")
).show
```

### Analytic functions: lag and lead

`lag` and `lead` look at preceding or following rows in the window. Both take an
optional offset (default `1`) and an optional default value.

```ruby
ordered = SparkConnect::Window.partition_by("dept").order_by("year")

employees.select(
  F.col("dept"),
  F.col("year"),
  F.col("salary"),
  F.lag("salary", 1).over(ordered).alias("prev_salary"),
  F.lead("salary", 1, 0).over(ordered).alias("next_salary")
).show
```

### Frames: rows_between and range_between

A window frame restricts which rows in the partition are included relative to
the current row. `rows_between` counts physical rows; `range_between` compares
values of the ordering column. Use the boundary constants for unbounded edges
and the current row.

```ruby
include SparkConnect    # brings Window into scope; or qualify with SparkConnect::Window

running = Window
  .partition_by("dept")
  .order_by("year")
  .rows_between(Window::UNBOUNDED_PRECEDING, Window::CURRENT_ROW)

employees.select(
  F.col("dept"),
  F.col("year"),
  F.col("salary"),
  F.sum("salary").over(running).alias("running_total")
).show
```

The boundary constants are:

| Constant                     | Meaning                                  |
| ---------------------------- | ---------------------------------------- |
| `Window::UNBOUNDED_PRECEDING` | start of the partition                   |
| `Window::UNBOUNDED_FOLLOWING` | end of the partition                     |
| `Window::CURRENT_ROW`         | the current row (offset `0`)             |

Any integer is also valid as an offset, e.g. `rows_between(-1, 1)` for a
three-row sliding window. `range_between` uses the same boundaries but
interprets offsets as values of the ordering expression:

```ruby
salary_band = Window.partition_by("dept").order_by("salary").range_between(-20, 20)

employees.select(
  F.col("name"),
  F.col("salary"),
  F.count("*").over(salary_band).alias("peers_within_20")
).show
```

## See also

- [Functions](functions.html) for the full list of aggregate and analytic functions.
- [DataFrames](dataframes.html) for the underlying transformation API.
- [Catalog](catalog.html) for inspecting tables you aggregate over.
