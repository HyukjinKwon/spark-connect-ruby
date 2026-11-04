---
title: Configuration & Errors
nav_order: 13
---

# Configuration, Observations & Errors

## Runtime configuration

`spark.conf` returns a {`SparkConnect::RuntimeConfig`} for getting and setting
Spark SQL runtime properties.

```ruby
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.get("spark.sql.shuffle.partitions")          #=> "8"
spark.conf.get("spark.sql.session.timeZone", "UTC")     # with a default
spark.conf.unset("spark.sql.shuffle.partitions")
spark.conf.get_all("spark.sql")                         # filtered by prefix -> Hash
spark.conf.modifiable?("spark.sql.shuffle.partitions")  #=> true (alias is_modifiable)
spark.conf.set_all({ "spark.sql.ansi.enabled" => true, "spark.sql.shuffle.partitions" => 8 })
```

Session-level options can also be supplied when building the session:

```ruby
SparkConnect::SparkSession.builder
  .remote("sc://localhost:15002")
  .config("spark.sql.shuffle.partitions", 16)
  .config("spark.sql.ansi.enabled" => true)    # or several at once
  .get_or_create
```

Like PySpark, builder options are sent in one request after connecting, errors
for options the session cannot change (static configurations) are ignored, and
`spark.remote`/`spark.master` are never sent. `get_or_create` applies the
options to an existing session too.

## Observations

An `Observation` collects named aggregate metrics while a DataFrame is
materialised - without a second pass over the data.

```ruby
obs = SparkConnect::Observation.new("metrics")
df.observe(obs, F.count(F.lit(1)).alias("rows"), F.max("id").alias("max_id")).collect
obs.get   #=> {"rows" => 1000, "max_id" => 999}
```

Metric values decode to Ruby values (`BigDecimal`, `Date`, `Time`, `Array`,
`Hash`, ...). If computing a metric failed on the server (reported by Spark
4.2+), `obs.get` raises the corresponding `SparkConnect::SparkConnectError`
(for example `AnalysisError`) with the server's `error_class` and `sql_state`.

## Error handling

All library errors descend from `SparkConnect::Error`:

| Class                              | Raised when |
| ---------------------------------- | ----------- |
| `SparkConnect::ConnectionError`    | a connection string is malformed |
| `SparkConnect::IllegalArgumentError` | an argument is invalid before any request |
| `SparkConnect::SparkConnectError`  | the server returns an error (base) |
| `SparkConnect::AnalysisError`      | analysis-time failure (e.g. unresolved column) |
| `SparkConnect::ParseError`         | SQL parse failure |
| `SparkConnect::RetriesExceededError` | transient failures exhausted the retry budget |

```ruby
begin
  spark.sql("SELECT * FROM does_not_exist").collect
rescue SparkConnect::AnalysisError => e
  warn "Analysis failed: #{e.message} (error_class=#{e.error_class})"
end
```

Server errors carry Spark's canonical `error_class` and `sql_state` when the
server provides them, plus the originating gRPC status code (`grpc_code`).

## Tags and interrupts

Tag the operations a session runs, then interrupt by tag, by operation id, or
all at once:

```ruby
spark.add_tag("nightly-etl")
# ... run queries; each is tagged "nightly-etl" ...
spark.get_tags                 #=> ["nightly-etl"]
spark.interrupt_tag("nightly-etl")   # cancel everything with that tag
spark.interrupt_all                  # cancel all running operations
spark.clear_tags
```

## New and cloned sessions

```ruby
other = spark.new_session      # a fresh server-side session: no configs, temp views, or functions
copy  = spark.clone_session    # Spark 4.1+: starts as a copy of this session's state
copy  = spark.clone_session("6f1b8a5e-2d3c-4b7a-9e0f-1a2b3c4d5e6f")  # with a chosen session id
```

`new_session` follows PySpark 4.3: the new session shares only the connection
settings. A cloned session starts with this session's SQL configurations,
temporary views, registered functions, and catalog state, and is independent
afterwards. Both are released by their own `stop`.

## Progress handlers

Register a callable to follow the stages of every query the session runs:

```ruby
spark.register_progress_handler do |stages:, inflight_tasks:, operation_id:, done:|
  done_tasks = stages.sum(&:num_completed_tasks)
  total = stages.sum(&:num_tasks)
  warn "#{operation_id}: #{done_tasks}/#{total} tasks#{' (finished)' if done}"
end
spark.range(10_000_000).agg(F.sum("id")).collect
spark.clear_progress_handlers   # or remove_progress_handler(handler)
```

Each `stages` entry is a `SparkConnect::StageInfo` with `stage_id`,
`num_tasks`, `num_completed_tasks`, `num_bytes_read`, and `done`. Handlers run
on every progress update that reports tasks and once more with `done: true`
when the query finishes; an exception raised by a handler is reported as a
warning and does not affect the query.

## Operation status

On Spark 4.2+ servers, the low-level client reports the state of the session's
operations (the `GetStatus` RPC):

```ruby
resp = spark.client.get_operation_statuses              # every operation of the session
resp = spark.client.get_operation_statuses([op_id])     # selected operations
resp.operation_statuses.map { |s| [s.operation_id, s.state] }
#=> [["2b1c...", :OPERATION_STATE_SUCCEEDED]]
```

## Retries and timeouts

The client automatically retries transient gRPC failures (`UNAVAILABLE`,
`DEADLINE_EXCEEDED`, `ABORTED`, `RESOURCE_EXHAUSTED`) with exponential backoff
and jitter. Tune the policy when constructing a client directly via
`SparkConnect::SparkConnectClient.new(channel_builder, max_retries:,
retry_base_delay:, max_retry_delay:)`.

Like the Spark 4.3 clients, each non-streaming RPC has a deadline: one hour for
`AnalyzePlan` and ten minutes for `Config`, `Interrupt`, `ReleaseSession`,
`CloneSession`, and `GetStatus`. Query execution itself (`ExecutePlan`) has no
deadline. Pass `rpc_deadlines:` to change them (values in seconds; `nil`
disables one):

```ruby
deadlines = SparkConnect::RpcDeadlines.defaults
deadlines.analyze_plan = 120
client = SparkConnect::SparkConnectClient.new(
  SparkConnect::ChannelBuilder.new("sc://localhost:15002"), rpc_deadlines: deadlines
)
spark = SparkConnect::SparkSession.new(client)
# SparkConnect::RpcDeadlines.disabled turns every deadline off.
```
