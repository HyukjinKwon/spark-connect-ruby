---
title: Structured Streaming
nav_order: 9
---

# Structured Streaming

`spark-connect` supports Spark Structured Streaming: read from streaming
sources, build streaming DataFrames with the same transformation API, and write
to streaming sinks, managing the resulting queries.

{: .note }
> `foreach` / `foreachBatch` sinks and user-defined functions are **not**
> supported, because they rely on UDFs whose Spark Connect protobuf definitions
> are not yet finalized. Everything else (file/Kafka/console/memory sinks,
> triggers, output modes, watermarks, the query manager) works.

## Reading a stream

Use {SparkConnect::SparkSession#read_stream} (alias `readStream`):

```ruby
F = SparkConnect::F

stream = spark.read_stream
              .format("rate")
              .option("rowsPerSecond", 10)
              .load

stream.streaming?            #=> true
stream.schema.simple_string  #=> "struct<timestamp:timestamp,value:bigint>"
```

`format`/`option`/`options`/`schema`/`load`/`table` mirror the batch
{SparkConnect::DataFrameReader}, plus `csv`/`json`/`parquet`/`orc`/`text`
shortcuts. The returned DataFrame is a normal {SparkConnect::DataFrame}: apply
`select`, `filter`, `group_by`, `with_watermark`, and so on.

## Watermarks

```ruby
events.with_watermark("event_time", "10 minutes")
      .group_by(F.window(F.col("event_time"), "5 minutes"))
      .count
```

## Writing a stream

{SparkConnect::DataFrame#write_stream} (alias `writeStream`) returns a
{SparkConnect::DataStreamWriter}. Calling `start` (or `to_table`) launches the
query and returns a {SparkConnect::StreamingQuery}.

```ruby
query = stream
        .write_stream
        .format("memory")          # or "console", "parquet", "kafka", ...
        .query_name("rates")       # required for the memory sink
        .output_mode("append")     # "append" | "complete" | "update"
        .trigger(processing_time: "1 second")
        .start

query.id          #=> stable query id (survives checkpoint restarts)
query.run_id      #=> unique per start
query.active?     #=> true
```

### Triggers

| Call | Meaning |
| ---- | ------- |
| `trigger(processing_time: "10 seconds")` | micro-batch every interval |
| `trigger(once: true)` | process available data once, then stop |
| `trigger(available_now: true)` | process all available data (multiple batches), then stop |
| `trigger(continuous: "1 second")` | continuous processing with the given checkpoint interval |

### Sinks

```ruby
# Files (provide a checkpoint location)
stream.write_stream.format("parquet")
      .option("checkpointLocation", "/chk/out")
      .start("/data/out")

# A catalog table
stream.write_stream.format("parquet")
      .option("checkpointLocation", "/chk/tbl")
      .to_table("db.events")
```

## Inspecting and controlling a query

```ruby
query.status            #=> {"message"=>..., "isActive"=>true, ...}
query.recent_progress   #=> [ {parsed progress JSON}, ... ]
query.last_progress     #=> the most recent progress object
query.process_all_available
query.await_termination(10_000)   # block up to 10s; => terminated?
query.explain
query.exception         #=> error message if the query failed, else nil
query.stop
```

## Managing queries

{SparkConnect::SparkSession#streams} returns a
{SparkConnect::StreamingQueryManager}:

```ruby
spark.streams.active                 #=> [StreamingQuery, ...]
spark.streams.get(query.id)          #=> the query, or nil
spark.streams.await_any_termination(30_000)
spark.streams.reset_terminated
```
