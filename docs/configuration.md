---
title: Configuration & Errors
nav_order: 12
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
spark.conf.modifiable?("spark.sql.shuffle.partitions")  #=> true
```

Session-level options can also be supplied when building the session:

```ruby
SparkConnect::SparkSession.builder
  .remote("sc://localhost:15002")
  .config("spark.sql.shuffle.partitions", 16)
  .get_or_create
```

## Observations

An `Observation` collects named aggregate metrics while a DataFrame is
materialised - without a second pass over the data.

```ruby
obs = SparkConnect::Observation.new("metrics")
df.observe(obs, F.count(F.lit(1)).alias("rows"), F.max("id").alias("max_id")).collect
obs.get   #=> {"rows" => 1000, "max_id" => 999}
```

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

## Retries

The client automatically retries transient gRPC failures (`UNAVAILABLE`,
`DEADLINE_EXCEEDED`, `ABORTED`, `RESOURCE_EXHAUSTED`) with exponential backoff
and jitter. Tune the policy when constructing a client directly via
`SparkConnect::SparkConnectClient.new(channel_builder, max_retries:,
retry_base_delay:, max_retry_delay:)`.
