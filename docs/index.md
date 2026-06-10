---
title: Overview
nav_order: 1
---

# spark-connect for Ruby

`spark-connect` is a production-ready, **pure-Ruby** client for
[Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html).
It talks to a Spark cluster over gRPC and exposes a DataFrame API that closely
mirrors PySpark, so if you have written PySpark you already know most of this gem.

- **Repository:** [HyukjinKwon/spark-connect-ruby](https://github.com/HyukjinKwon/spark-connect-ruby)
- **Gem:** [`spark-connect`](https://rubygems.org/gems/spark-connect)

## What is Spark Connect?

Classic Spark applications run your driver code inside the cluster's JVM. Spark
Connect splits that apart: your program is a thin **client** that builds an
unresolved logical plan and ships it to a remote **Spark Connect server** over
gRPC. The server plans, optimizes, and executes the query, then streams results
back as [Apache Arrow](https://arrow.apache.org/) batches.

Because the protocol is language-agnostic, a client can be written in any
language. This gem is that client for Ruby: there is no JVM, no Py4J, and no
Spark installation required on the client machine -- only a reachable Spark
Connect server (Spark 3.4+, tested against 3.5.x and 4.x).

## Feature highlights

- **DataFrame API** modeled on PySpark: `select`, `filter`/`where`, `join`,
  `group_by`/`agg`, `order_by`, `union`, `distinct`, window functions, and more.
- **Snake_case Ruby idiom** with **camelCase aliases** for high-traffic names
  (`groupBy`, `withColumn`, `orderBy`, `createDataFrame`, `selectExpr`, ...), so
  PySpark snippets translate almost verbatim.
- **Spark SQL** via `spark.sql(...)`, including named and positional parameters.
- **Rich functions library** under `SparkConnect::Functions` (aliased `SparkConnect::F`).
- **Typed schemas** under `SparkConnect::Types::*` and DDL string parsing.
- **Arrow-based decoding** of results into `Row` objects (or a columnar `Arrow::Table`).
- **Catalog, reader/writer, NA & stat helpers, observations, and window specs.**

## Install

```ruby
gem install spark-connect
```

See [Installation]({{ "/installation.html" | relative_url }}) for prerequisites
(Ruby >= 3.1 and the Apache Arrow GLib system libraries that `red-arrow` needs).

## Quickstart

```ruby
require "spark-connect"

F = SparkConnect::F

spark = SparkConnect::SparkSession.builder
                                  .remote("sc://localhost:15002")
                                  .app_name("quickstart")
                                  .get_or_create

df = spark.range(10)
         .select(F.col("id"), (F.col("id") * 2).alias("doubled"))
         .filter(F.col("id") % 2 == 0)

df.show
puts "rows: #{df.count}"
spark.stop
```

## Where to next

- [Installation]({{ "/installation.html" | relative_url }}) -- prerequisites,
  installing the gem, and starting a local Spark Connect server.
- [Getting started]({{ "/getting-started.html" | relative_url }}) -- connection
  strings, building a session, and your first DataFrames.
