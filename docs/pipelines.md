---
title: Declarative Pipelines
nav_order: 10
---

# Declarative Pipelines

`spark-connect` supports **Spark Declarative Pipelines** (SDP, Spark 4.1+): you
describe a dataflow graph of **outputs** (tables, materialized views, temporary
views, sinks) and the **flows** that populate them, then run it. Each flow is
defined by an ordinary {SparkConnect::DataFrame}, so you build pipeline logic
with the same API you use everywhere else.

{: .note }
> Flows are defined by relations (DataFrames). Python-style query-function
> evaluation and `foreach`/`foreachBatch` are not supported (they require UDFs).

## Building and running a pipeline

```ruby
F = SparkConnect::F

pipe = spark.pipeline   # creates a dataflow graph; pipe.graph_id is its id

# A materialized view from a batch query
src = spark.range(10).select(F.col("id"), (F.col("id") % 2).alias("p"))
pipe.create_materialized_view("evens", src.filter(F.col("p") == 0))

# A second MV that reads the first (pipe.read references a dataset in the graph)
pipe.create_materialized_view(
  "doubled", pipe.read("evens").select((F.col("id") * 2).alias("d"))
)

# Resolve the graph and run an update. Storage must be a URI.
events = pipe.start_run(storage: "file:///tmp/pipeline_storage", full_refresh_all: true)
events.each { |e| puts e.message }   # QUEUED -> PLANNING -> RUNNING -> COMPLETED ...

spark.read.table("doubled").show
```

## Outputs

| Method | Output type | Notes |
| ------ | ----------- | ----- |
| `create_table(name, df)` | streaming table | needs a streaming relation or a `once: true` flow |
| `create_materialized_view(name, df)` | materialized view | published to the catalog |
| `create_temporary_view(name, df)` | temporary view | not published |
| `create_sink(name, df, format:, options:)` | streaming sink | |

Table-like outputs accept `comment`, `format`, `partition_cols`,
`clustering_columns`, `table_properties`, and `schema` (a `StructType` or DDL
string).

## Flows

`create_*` defines an output and a flow that populates it in one call. You can
also define flows explicitly (e.g. multiple flows into one table, or a one-time
backfill):

```ruby
pipe.create_table("target")                          # output only
pipe.define_flow("backfill", batch_df, target: "target", once: true)
pipe.define_flow("incremental", stream_df, target: "target")
```

## SQL-defined graphs

```ruby
pipe.define_sql(<<~SQL)
  CREATE MATERIALIZED VIEW evens AS SELECT id FROM range(10) WHERE id % 2 = 0;
SQL
```

## Running

```ruby
pipe.start_run(
  storage: "file:///tmp/storage",  # required; an absolute URI
  full_refresh_all: true,          # reset and recompute everything
  full_refresh: ["evens"],         # or reset specific datasets
  refresh: ["doubled"],            # or update specific datasets
  dry: false                       # true to validate the graph without executing
)
pipe.drop   # drop the graph and stop attached flows
```

`start_run` blocks until the update completes and returns the
{SparkConnect::PipelineEvent}s emitted during the run (also yielded to a block
if given).
