# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Declarative Pipelines** (Spark 4.1+): `SparkSession#pipeline` returns a
  `Pipeline` (dataflow graph). Define outputs (`create_table`,
  `create_materialized_view`, `create_temporary_view`, `create_sink`) and flows
  (`define_flow`, `define_sql`), then `start_run` (with `full_refresh`/`refresh`/
  `dry`/`storage`) which streams `PipelineEvent`s; plus `read` and `drop`.
- **Structured Streaming**: `SparkSession#read_stream` (`DataStreamReader`),
  `DataFrame#write_stream` (`DataStreamWriter`), `StreamingQuery` (status,
  recent/last progress, await_termination, process_all_available, stop,
  exception, explain), and `SparkSession#streams` (`StreamingQueryManager`:
  active, get, await_any_termination, reset_terminated). Supports triggers
  (processing-time, once, available-now, continuous), output modes, and
  file/console/memory/Kafka sinks. (`foreach`/`foreachBatch` and UDFs remain
  unsupported pending finalized protobuf definitions.)
- `DataFrame#with_watermark` for event-time watermarks on streaming DataFrames.
- `DataFrame#repartition_by_range`, `DataFrame#checkpoint` /
  `DataFrame#local_checkpoint`, and `DataFrame#transform`.

## [0.1.0] - 2026-06-10

Initial release.

### Added

- `SparkConnect::SparkSession` and its fluent `Builder` (`remote`, `app_name`,
  `config`, `get_or_create`/`create`), plus `range`, `sql`, `table`, `read`,
  `create_data_frame`, `conf`, `catalog`, `version`, and `stop`.
- `SparkConnect::DataFrame` with the core transformation and action surface:
  `select`/`select_expr`, `filter`/`where`, `with_column(s)`,
  `with_column(s)_renamed`, `drop`, `distinct`/`drop_duplicates`,
  `order_by`/`sort`/`sort_within_partitions`, `limit`/`offset`,
  `group_by`/`rollup`/`cube`/`agg`, `join`/`cross_join`,
  `union`/`union_by_name`/`intersect`/`except`/`subtract`,
  `repartition`/`coalesce`, `sample`, `alias`, `hint`, `unpivot`, `to`/`to_df`,
  `collect`/`take`/`head`/`first`/`count`/`show`/`to_arrow`, and plan
  introspection (`schema`, `columns`, `dtypes`, `print_schema`, `explain`).
- `SparkConnect::Column` with Ruby operator overloads, aliasing, casting,
  sort ordering, predicates, `when`/`otherwise`, complex-type access, and `over`.
- `SparkConnect::Functions` (`F`): a broad PySpark-compatible function library
  including aggregate, math, string, date/time, collection, JSON, conditional,
  hashing, and higher-order (lambda) functions.
- `SparkConnect::Window`/`WindowSpec` for analytic window definitions.
- `SparkConnect::GroupedData`, `DataFrameNaFunctions` (`na`),
  `DataFrameStatFunctions` (`stat`), and `Observation`.
- `SparkConnect::DataFrameReader`, `DataFrameWriter`, and `DataFrameWriterV2`.
- `SparkConnect::Catalog` and `SparkConnect::RuntimeConfig`.
- `SparkConnect::Types`: a full Spark SQL type system with proto conversion,
  `simpleString`/DDL/JSON rendering, and schema trees.
- `SparkConnect::Row` with positional, by-name, and method-style access.
- Apache Arrow IPC result decoding and local-relation encoding via `red-arrow`.
- gRPC client (`SparkConnectClient`, `ChannelBuilder`) with `sc://` connection
  string parsing, TLS/bearer-token auth, and retry-with-backoff.
- A structured error hierarchy (`Error`, `ConnectionError`, `SparkConnectError`,
  `AnalysisError`, `ParseError`, ...).
- Vendored Spark Connect 4.1 protobuf/gRPC definitions and a regeneration script
  (`bin/generate-protos`).

[Unreleased]: https://github.com/HyukjinKwon/spark-connect-ruby/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/HyukjinKwon/spark-connect-ruby/releases/tag/v0.1.0
