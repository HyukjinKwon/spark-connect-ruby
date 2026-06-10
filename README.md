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

`spark-connect` covers the batch DataFrame and SQL surface defined by the Spark
Connect protocol: DataFrames and column expressions, the standard SQL function
library, Spark SQL with parameters, reading and writing (CSV/JSON/Parquet/ORC/
JDBC/tables), the catalog, runtime configuration, observations, a full typed
schema system, and Apache Arrow result decoding -- all over a resilient gRPC
client with TLS/bearer-token auth and automatic retries.

Method names are snake_case (idiomatic Ruby); the highest-traffic PySpark names
also have camelCase aliases (`groupBy`, `withColumn`, `orderBy`,
`createDataFrame`, ...) so PySpark snippets translate almost verbatim.

Structured Streaming and user-defined functions (UDFs) are defined by the Spark
Connect protocol but are **not yet implemented** by this client; they are on the
roadmap.

## Requirements

- **Ruby >= 3.1**
- **Apache Arrow C++/GLib system libraries** (required by the `red-arrow` dependency):
  - macOS: `brew install apache-arrow apache-arrow-glib`
  - Ubuntu/Debian: install `libarrow-glib-dev` from the [Apache Arrow APT repository](https://arrow.apache.org/install/)
- A reachable **Spark Connect server**. This client is generated against the Spark Connect 4.1 protocol and supports **Apache Spark 3.5 and above**.

See the [installation guide](https://hyukjinkwon.github.io/spark-connect-ruby/installation.html) for details.

## Installation

```bash
gem install spark-connect
```

Or in a `Gemfile`:

```ruby
gem "spark-connect"
```

## Running a local Spark Connect server

```bash
# Download a Spark distribution (4.1.0 shown here; 3.5+ also works)
curl -fsSL https://archive.apache.org/dist/spark/spark-4.1.0/spark-4.1.0-bin-hadoop3.tgz | tar xz
cd spark-4.1.0-bin-hadoop3

# Start the Connect server (requires Java 17+)
./sbin/start-connect-server.sh --jars "$(pwd)/jars/spark-connect_2.13-4.1.0.jar"
```

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

Supported parameters: `token`, `user_id`, `user_agent`, `use_ssl`, `session_id`, and any `x-*` custom gRPC headers.

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

The client is generated against the **Spark Connect 4.1** protocol and supports **Apache Spark 3.5 and above** (the Spark Connect wire protocol is backward compatible across these releases).

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
