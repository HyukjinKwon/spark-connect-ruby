# spark-connect examples

Runnable, self-contained Ruby scripts that exercise the **spark-connect** gem
against a live Spark Connect server. Each script connects, does some focused
work, prints results, and calls `spark.stop` at the end.

## Prerequisites

- Ruby >= 3.1 with the gem and its dependencies installed.
- A running Spark Connect server (Spark 4.1.x). The examples default to
  `sc://localhost:15002`.

## Running

Make the gem and tools available on your `PATH`, then run any script:

```sh
export PATH="/opt/homebrew/lib/ruby/gems/3.4.0/bin:$PATH"
ruby examples/01_quickstart.rb
```

Point the examples at a different server by setting `SPARK_REMOTE`:

```sh
SPARK_REMOTE="sc://my-host:15002" ruby examples/07_read_write.rb
```

Every script can be syntax-checked without a server:

```sh
ruby -c examples/08_local_data.rb
```

## Index

| Script | What it shows |
| ------ | ------------- |
| `01_quickstart.rb` | Connect, build a DataFrame with `range`, project columns, then `show` / `collect`. |
| `02_transformations.rb` | Core lazy DataFrame transformations (select, filter, withColumn, orderBy, ...). |
| `03_aggregations.rb` | `group_by`/`agg`, `rollup`/`cube`, and `pivot`. |
| `04_joins.rb` | Inner/left/right/outer/semi/anti joins and `cross_join`. |
| `05_window_functions.rb` | `row_number`/`rank`/`lag`/`lead` over `partition_by`/`order_by`. |
| `06_sql.rb` | `spark.sql` with named and positional parameters, and temp views. |
| `07_read_write.rb` | Write a DataFrame to parquet/json/csv in a `Dir.mktmpdir` temp dir and read each back. |
| `08_local_data.rb` | `create_data_frame` from arrays of hashes, arrays + column names, and an explicit `StructType`; `printSchema`; `to_arrow`. |
| `09_na_and_stats.rb` | `df.na.drop`/`fill`/`replace`; `df.stat.corr`/`approx_quantile`/`crosstab`; `describe` / `summary`. |
| `10_streaming.rb` | Structured Streaming: `rate` source into the memory sink, with progress and a query handle. |
| `11_pipeline.rb` | Declarative Pipelines: a dataflow graph of two chained materialized views. |
