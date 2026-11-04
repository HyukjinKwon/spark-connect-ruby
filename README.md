# spark-connect (Ruby)

[![CI](https://github.com/HyukjinKwon/spark-connect-ruby/actions/workflows/ci.yml/badge.svg)](https://github.com/HyukjinKwon/spark-connect-ruby/actions/workflows/ci.yml)
[![Gem Version](https://badge.fury.io/rb/spark-connect.svg)](https://rubygems.org/gems/spark-connect)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://hyukjinkwon.github.io/spark-connect-ruby/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

A pure-Ruby client for **[Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)** - the gRPC-based, decoupled client/server protocol for Apache Spark.

`spark-connect` lets you build and run Spark DataFrame queries from Ruby against a remote Spark cluster, with an API that closely mirrors PySpark. No JVM, no local Spark installation, no `spark-submit` - just a gRPC connection to a Spark Connect server.

```ruby
require "spark-connect"

spark = SparkConnect::SparkSession.builder
                                  .remote("sc://localhost:15002")
                                  .get_or_create

F = SparkConnect::F

spark.range(1, 1_000)
     .select(F.col("id"), (F.col("id") % 3).alias("bucket"))
     .group_by("bucket")
     .agg(F.count("*").alias("n"), F.sum("id").alias("total"))
     .order_by("bucket")
     .show

spark.stop
```

```
+------+---+------+
|bucket|  n| total|
+------+---+------+
|     0|333|166833|
|     1|333|166167|
|     2|333|166500|
+------+---+------+
```

## What it supports

`spark-connect` implements the Spark Connect **DataFrame**, **Column** and
**functions**, **SQL**, **Catalog**, **reader/writer**, **Structured
Streaming** (including query listeners), and **Declarative Pipelines** API of
PySpark 4.3: subqueries, lateral and nearest-by joins, `zip`, `merge_into`,
table-valued functions, `as_table` table arguments, caching with storage
levels, session cloning, progress handlers, operation status, the TIME,
spatial, and nanosecond timestamp types, and every built-in SQL function in
`pyspark.sql.functions`.

**Out of scope** are the parts of PySpark that run client-side code on the
server or depend on Python libraries, because Spark executes such code only in
Python (or JVM) workers and has no Ruby worker:

- user-defined functions and table functions (`udf`, `udtf`, `arrow_udtf`,
  pandas and Arrow UDFs; these raise `NotImplementedError`);
- the `foreach`/`foreachBatch` streaming sinks (raise `NotImplementedError`);
- `applyInPandas`/`applyInArrow`, `transformWithState(InPandas)`, cogrouped
  maps, `mapInPandas`/`mapInArrow`, and Python data sources;
- pandas interop (`toPandas`, `pandas_api`, `plot`) -- use `to_arrow` for a
  columnar `Arrow::Table`;
- the UDF profiler (`spark.profile`) and artifact upload (`addArtifacts`,
  `copyFromLocalToFs`), which exist to serve UDF code;
- the separate, experimental MLlib-over-Connect surface.

Results decode through Apache Arrow into ordered, name-addressable `Row`s.
Method names are snake_case (idiomatic Ruby) with camelCase aliases for the
common PySpark names (`groupBy`, `withColumn`, `orderBy`, `createDataFrame`,
...), so PySpark code ports almost verbatim.

## Requirements

- **Ruby >= 3.1**
- **Apache Arrow C++/GLib system libraries** (required by the `red-arrow` dependency):
  - macOS: `brew install apache-arrow apache-arrow-glib`
  - Ubuntu/Debian: install `libarrow-glib-dev` from the [Apache Arrow APT repository](https://arrow.apache.org/install/)
- A reachable **Spark Connect server**. This client is generated against the Spark Connect 4.3 protocol and supports **Apache Spark 3.5 and above**.

See the [installation guide](https://hyukjinkwon.github.io/spark-connect-ruby/installation.html) for details.

## Installation

```bash
gem install rubygems-requirements-system
gem install spark-connect
```

Or in a `Gemfile`:

```ruby
plugin "rubygems-requirements-system"
gem "spark-connect"
```

## Running a local Spark Connect server

```bash
# Download a Spark distribution (4.3.0 shown here; 3.5+ also works)
curl -fsSL https://archive.apache.org/dist/spark/spark-4.3.0/spark-4.3.0-bin-hadoop3.tgz | tar xz
cd spark-4.3.0-bin-hadoop3

# Start the Connect server (requires Java 17+).
# Spark 4.0.0+ bundles the Connect server, so no extra packages are needed.
./sbin/start-connect-server.sh
```

On **Spark 3.5.x** the Connect server is not bundled; pull it in with
`--packages "org.apache.spark:spark-connect_2.13:3.5.5"` (use a Scala 2.13 distribution).

The server listens on `sc://localhost:15002` by default.

## Connecting

Connection strings follow the standard Spark Connect grammar:

```ruby
# Plaintext, local
SparkConnect::SparkSession.builder.remote("sc://localhost:15002").get_or_create

# TLS + bearer token (token implies SSL)
SparkConnect::SparkSession.builder
  .remote("sc://spark.example.com:443/;token=#{ENV['SPARK_TOKEN']};user_id=alice")
  .get_or_create
```

Supported parameters: `token` (defaults to `$SPARK_CONNECT_AUTHENTICATE_TOKEN`), `user_id`, `user_agent`, `use_ssl`, `session_id`, and the gRPC keepalive settings `grpc_keepalive_enabled`, `grpc_keepalive_time_ms`, `grpc_keepalive_timeout_ms`, `grpc_keepalive_without_calls`. Any other parameter (e.g. `x-api-key=...`) is sent as a gRPC request header.

## A quick tour

```ruby
F = SparkConnect::F
T = SparkConnect::Types

# Build a DataFrame from local Ruby data
df = spark.create_data_frame([
  { "name" => "alice", "dept" => "eng", "salary" => 120 },
  { "name" => "bob",   "dept" => "eng", "salary" => 100 },
  { "name" => "carol", "dept" => "ops", "salary" => 110 },
])

# Transform and aggregate
df.where(F.col("salary") >= 105)
  .group_by("dept")
  .agg(F.avg("salary").alias("avg_salary"), F.count("*").alias("headcount"))
  .order_by(F.col("avg_salary").desc)
  .show

# Window functions
w = SparkConnect::Window.partition_by("dept").order_by(F.col("salary").desc)
df.with_column("rank", F.rank.over(w)).show

# Schemas
df.print_schema
df.schema.simple_string  #=> "struct<name:string,dept:string,salary:bigint>"

# SQL with parameters
spark.sql("SELECT * FROM VALUES (1), (2), (3) AS t(x) WHERE x > :min", { min: 1 }).show
```

## Documentation

Full documentation, including guides for every part of the API, lives at
**<https://hyukjinkwon.github.io/spark-connect-ruby/>**.

Runnable [`examples/`](examples/) cover quickstart, transformations, aggregations, joins, window functions, SQL, reading/writing, local data, and NA/stat helpers.

## Compatibility

The client is generated against the **Spark Connect 4.3** protocol and supports **Apache Spark 3.5 and above** (the Spark Connect wire protocol is backward compatible across these releases). APIs added in newer Spark releases (for example `DataFrame#zip` in 4.3 or `nearest_by_join` in 4.2) need a server of at least that version; see [Troubleshooting](https://hyukjinkwon.github.io/spark-connect-ruby/troubleshooting.html#version-compatibility) for the list. CI runs the integration specs against Spark 4.3.0, 4.2.0, 4.1.0, 4.0.0, and 3.5.5.

## Development

```bash
git clone https://github.com/HyukjinKwon/spark-connect-ruby
cd spark-connect-ruby
bundle install

bundle exec rake spec      # unit specs (no server required)
bundle exec rake rubocop   # lint
bundle exec rake yard      # API docs

# Integration specs against a live server
SPARK_REMOTE=sc://localhost:15002 bundle exec rspec spec/integration

# Regenerate the protobuf/gRPC stubs from the vendored .proto files
bin/generate-protos
```

See [CONTRIBUTING.md](CONTRIBUTING.md).
