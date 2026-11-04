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
> supported, because they run user code on the server, which a Ruby client
> cannot ship (both raise `NotImplementedError`). Everything else
> (file/Kafka/console/memory sinks, triggers including real-time mode, output
> modes, watermarks, the query manager, and query listeners) works.

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
{SparkConnect::DataFrameReader}, plus `csv`/`json`/`parquet`/`orc`/`text`/`xml`
shortcuts. The returned DataFrame is a normal {SparkConnect::DataFrame}: apply
`select`, `filter`, `group_by`, `with_watermark`, and so on.

```ruby
# Name a source so its checkpoint state survives adding/removing/reordering
# sources (Spark 4.2+; enable spark.sql.streaming.queryEvolution.enableSourceEvolution).
spark.read_stream.format("kafka").name("orders").option("subscribe", "orders").load

# Stream the row-level changes of a table (Spark 4.2+).
spark.read_stream.option("startingVersion", 5).changes("catalog.db.events")
```

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
| `trigger(real_time: "5 minutes")` | real-time mode with the given batch duration (Spark 4.2+, supported sources/sinks only) |

Exactly one trigger option may be given, like PySpark. `start` and `to_table`
also accept PySpark's keyword shortcuts:
`start(path, format: "parquet", output_mode: "append", partition_by: "day", query_name: "q", checkpointLocation: "/chk")`.
Use `cluster_by(...)` to cluster the output table.

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

## Query listeners

Register a {SparkConnect::StreamingQueryListener} (Spark 4.0+) to receive query
events on a background thread. The client registers one server-side listener
for the session and dispatches the events it streams back, like PySpark:

```ruby
class Monitor < SparkConnect::StreamingQueryListener
  def on_query_started(event)    = puts("started #{event.name} (#{event.id})")
  def on_query_progress(event)   = puts("batch #{event.progress['batchId']}")
  def on_query_idle(event)       = nil
  def on_query_terminated(event) = puts("ended #{event.id}: #{event.exception.inspect}")
end

monitor = Monitor.new
spark.streams.add_listener(monitor)
# ... run streaming queries ...
spark.streams.remove_listener(monitor)
```

Any object responding to some of these callbacks (or PySpark's camelCase
`onQueryStarted`, ...) works. `SparkSession#stop` removes the listeners.
