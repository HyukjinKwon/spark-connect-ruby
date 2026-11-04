# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-11-04

Feature parity with the PySpark 4.3.0 Spark Connect client. Items marked with a
Spark version need a server of at least that version.

### Added

- **Spark Connect 4.3.0 protocol.** Regenerated the vendored protobuf/gRPC stubs
  from Spark v4.3.0 (`SparkConnect::SPARK_VERSION` is now `"4.3.0"`).
- **Every `pyspark.sql.functions` function** except the UDF family: 280 new
  functions (559 of PySpark's 562), including the HLL/KLL/Theta/Tuple sketch,
  bitmap, variant (with the 4.3 `variant_set`/`variant_insert`/`variant_delete`/
  `variant_array_append` family), vector (4.3), geospatial (`st_*`), TIME
  (`make_time`, `time_*`, `current_time`), nanosecond timestamp (4.3), XML/CSV,
  AES, URL, collation, `try_*`, interval, partition transform, `listagg`/
  `string_agg`, `count_min_sketch`, `hmac`, base32, `jaro_winkler_similarity`,
  `call_function`, `assert_true`/`raise_error`, and `broadcast` functions, plus
  the PySpark 4.3 parameters `instr(..., start, occurrence)`,
  `regexp_replace(..., position)`, and `max_by`/`min_by(..., k)`, an
  `array_sort` comparator, and PySpark's camelCase aliases (`bitwiseNOT`,
  `shiftLeft`, `toDegrees`, ...). `udf`, `udtf`, and `arrow_udtf` raise
  `NotImplementedError`.
- **DataFrame**: `zip` (4.3), `nearest_by_join` (4.2), `repartition_by_id` (4.1),
  `lateral_join`, `transpose`, `scalar`/`exists` subqueries, `as_table` table
  arguments (`TableArg`), `group_by(..., grouping_sets:)`, `merge_into`
  (`MergeIntoWriter`), `zip_with_index`, `tail`, `with_metadata`,
  `metadata_column`, `random_split`, `cache`/`persist`/`unpersist`/
  `storage_level`/`cached?` with `SparkConnect::StorageLevel`,
  `local_checkpoint(storage_level:)`, `register_temp_table`, `df[...]` by name,
  index, Column (filter), or Array (select), the NA/stat shortcuts (`dropna`,
  `fillna`, `replace`, `approx_quantile`, `corr`, `cov`, `crosstab`,
  `freq_items`, `sample_by`), and PySpark camelCase aliases (`isEmpty`,
  `sameSemantics`, `semanticHash`, `inputFiles`, ...).
- **Column**: `isin(df)` IN-subqueries (Spark 4.1+), `try_cast`,
  `with_field`, `drop_fields`, `outer`, `transform`, qualified stars
  (`F.col("t.*")`), and the `bitwiseAND`/`bitwiseOR`/`bitwiseXOR`/
  `eqNullSafe`/`isNaN`/`getItem`/`getField` aliases.
- **Session**: `empty_data_frame`, `clone_session` (4.1), `tvf`
  (table-valued functions), progress handlers
  (`register_progress_handler`/`remove_progress_handler`/
  `clear_progress_handlers`, reporting `StageInfo`s), `stopped?`, and the
  `SparkSession.active`/`get_active_session`/`default_session` lookups. The
  builder gained `channel_builder`, `config(hash)`, a block form that stops the
  session afterwards, `master` (no-op), and `enable_hive_support` (raises, as
  in PySpark).
- **Client**: `get_operation_statuses` (the 4.2 `GetStatus` RPC), per-RPC
  deadlines (`SparkConnect::RpcDeadlines`), and `close`/`closed?`.
- **Connection strings**: the `grpc_keepalive_enabled`,
  `grpc_keepalive_time_ms`, `grpc_keepalive_timeout_ms`, and
  `grpc_keepalive_without_calls` parameters (keepalive is on by default, like
  the 4.3 clients), a `SPARK_CONNECT_AUTHENTICATE_TOKEN` default for `token`,
  and 128 MiB gRPC message limits.
- **Catalog** (4.2): `drop_table`, `drop_view`, `create_database`,
  `drop_database`, `list_partitions`, `list_views`, `get_table_properties`,
  `get_create_table_string`, `truncate_table`, and `analyze_table`; plus
  `get_database`, `get_table`, `get_function`, `refresh_by_path`, name
  patterns for the `list_*` methods, `cache_table(storage_level)`, and
  camelCase aliases.
- **Reading and writing**: `read.changes` (4.2), `read.xml`, parsing a
  DataFrame of JSON/CSV/XML strings (`read.json(df)`; XML needs 4.2),
  partitioned `read.jdbc`; `write.cluster_by`, `write.xml`, `write.jdbc`,
  `with_schema_evolution` (4.2) on both writers, and `write_to(...).options`/
  `partitioned_by`/`cluster_by`.
- **Structured Streaming**: `read_stream.name` (named sources, 4.2),
  `read_stream.changes` (4.2), `read_stream.xml`, `write_stream.cluster_by`,
  `trigger(real_time: ...)` (4.2), keyword shortcuts on `start`/`to_table`,
  `StreamingQueryListener` with `streams.add_listener`/`remove_listener`, and
  camelCase aliases on `StreamingQuery`.
- **Declarative Pipelines**: `create_streaming_table` and
  `create_auto_cdc_flow` (4.2).
- **Types**: `TimeType` and `GeometryType`/`GeographyType` (4.1),
  `TimestampNTZNanosType`/`TimestampLTZNanosType` (4.3), and decoding of
  user-defined types (`UserDefinedType`); TIME values decode to `Arrow::Time`
  and nanosecond timestamps keep their nanoseconds.
- `RuntimeConfig#set_all` and the `is_modifiable`/`isModifiable` aliases.
- `Observation#get` raises the server's error when a metric failed (4.2).
- Spark 4.3.0 is now the first entry of the CI integration matrix.

### Changed

- **Function arguments follow PySpark's conventions exactly** (each function
  was checked against PySpark 4.3.0 expression for expression). Where PySpark
  takes a column, a String is a column name, so `F.date_add("d", "n")`,
  `F.add_months`, `F.date_sub`, `F.substring`, `F.slice`, `F.overlay`,
  `F.repeat`, `F.array_repeat`, `F.array_insert`, the `split` limit, and the
  `regexp_like`/`regexp_count`/`regexp_substr`/`regexp_extract_all` pattern
  now read columns where they used to send String literals; wrap literals in
  `F.lit`.
- **Client-side random seeds.** `F.rand`, `F.randn`, `F.uuid`, `F.shuffle` (and
  the new `randstr`, `uniform`, `count_min_sketch`), `DataFrame#sample`,
  `random_split`, and `sample_by` pick a seed on the client when none is given,
  so a DataFrame returns the same values on every action. On Spark 3.5
  servers, `F.uuid`, `F.shuffle`, and `F.mode` (which now sends its
  `deterministic` flag) need `F.expr` instead.
- `F.mean`, `F.pow`, and one-argument `F.log` send `avg`, `power`, and `ln`
  like PySpark, so their result columns are named accordingly; the base of
  `F.log(base, col)` is a literal.
- `F.lit(BigDecimal(...))` is tagged `decimal(10, 0)` like PySpark; the server
  widens it to fit the value (`1.5` becomes `decimal(10,1)` instead of
  `decimal(2,1)`).
- `df["x"]`, `df.col("x")`, `df.x`, `col_regex`, and `pivot("x")` refer to that
  DataFrame's column (bound to its plan id, like PySpark), which disambiguates
  self-joins. `group_by`/`rollup`/`cube` accept 1-based ordinals, and `join`
  accepts an Array of Column conditions (combined with AND) and rejects
  DataFrames from another session.
- `GroupedData`: `sum`/`avg`/`mean`/`max`/`min` without columns aggregate
  every numeric column; `agg` without expressions raises; `pivot` is only
  allowed directly after `group_by` (`NotImplementedError` otherwise).
- `DataFrameNaFunctions`: `drop`, `fill`, and `replace` validate their
  arguments like PySpark; `replace` needs a `value` unless `to_replace` is a
  Hash, and integer replacement values are sent as doubles.
- `StatFunctions#corr` raises unless the method is `"pearson"`.
- Reader/writer/stream `option(key, nil)` is ignored (it used to send `""`),
  and `DataFrameWriter#mode(nil)` is a no-op.
- Builder options are applied with one request that silently skips options the
  session cannot change (as PySpark does), and `spark.remote`/`spark.master`
  are never sent; `get_or_create` applies them to an existing session too, and
  `create` makes the new session active.
- `new_session` follows PySpark 4.3: always a fresh, empty server-side session
  against the same endpoint.
- `SparkSession#stop` also closes the gRPC channel and stops the listener bus,
  and is idempotent.
- Every connection-string parameter the client does not consume is now sent as
  gRPC request metadata (it used to be only `x-*` ones), and `session_id` must
  be a UUID (`ConnectionError` otherwise).
- Non-streaming RPCs have deadlines (1 hour for `AnalyzePlan`, 10 minutes for
  the others); `ExecutePlan` has none.

### Fixed

- `F.bitwise_not`/`F.bitwiseNOT` and `F.negate` sent function names the server
  does not resolve (`bitwise_not`, `negate`); they now send `~` and `negative`.
- `F.from_json`, `F.to_json`, and `F.schema_of_json` flattened their options
  into extra arguments instead of passing one map, so any options failed.
- `F.trim`, `F.ltrim`, and `F.rtrim` with a trim string sent their arguments in
  the wrong order.
- `StreamingQuery#exception` raised `NoMethodError` for a query without an
  exception.
- `DataFrameReader#schema(StructType)` lost non-nullable fields and metadata;
  the schema is now sent as JSON, like PySpark.
- `Observation#get` returned raw protobuf messages (decimal, array, map, and
  struct metrics) or epoch integers (date and timestamp metrics).
- `new_session` reused the `session_id` of the connection string, so the "new"
  session was the same server-side session.

## [0.3.0] - 2026-06-15

### Fixed

- **Correct `TimestampType` instants in `create_data_frame`.** Timestamp columns
  were shipped as zone-less Arrow timestamps, so the server interpreted the epoch
  micros as session-local wall-clock and shifted the value by the session time
  zone. They are now tagged UTC; `TimestampNTZType` remains zone-less.
- **`RuntimeConfig#get` with a non-String default no longer raises.** A non-String
  default (e.g. `conf.get(key, 8)`) was passed straight into a protobuf string
  field, raising `Google::Protobuf::TypeError`. The default is now coerced to a
  String, matching `#set`.
- **No duplicate rows when an execute stream is retried.** The result accumulator
  was created outside the retry loop, so a mid-stream gRPC failure replayed
  already-consumed Arrow batches and duplicated rows on retry. The accumulator is
  now reset per attempt.
- **`DataFrame#drop_duplicates_within_watermark` is now watermark-aware.** It was a
  plain alias of `#drop_duplicates` and never set the `within_watermark` flag, so
  it silently performed an ordinary deduplication. Added a `dropDuplicatesWithinWatermark`
  alias.
- **`SparkSession::Builder#app_name` is now applied.** `create` explicitly skipped
  `spark.app.name`, making `#app_name` a no-op; all builder options are now
  forwarded to the new session.
- Corrected misleading doc comments for `Functions#nanvl` and `DataFrame#except_all`.

## [0.2.0] - 2026-06-10

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
- Temporary views: `DataFrame#create_temp_view` /
  `create_or_replace_temp_view` / `create_global_temp_view` /
  `create_or_replace_global_temp_view`.
- `DataFrame#with_watermark`, `repartition_by_range`, `checkpoint` /
  `local_checkpoint`, `transform`, `col_regex`, `to_json`, and
  `each` / `to_local_iterator`.
- `Catalog#create_table` and `Catalog#create_external_table`.
- `SparkSession#new_session`, `interrupt_all` / `interrupt_tag` /
  `interrupt_operation`, and operation tags (`add_tag` / `remove_tag` /
  `get_tags` / `clear_tags`) propagated on every execution.
- Regenerated the vendored protobuf/gRPC stubs against Spark Connect **4.1.0**
  (adds `pipelines.proto`).
- `bin/console` (an IRB session with `spark`/`F`/`T`) and `bin/setup`.

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

[Unreleased]: https://github.com/HyukjinKwon/spark-connect-ruby/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/HyukjinKwon/spark-connect-ruby/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/HyukjinKwon/spark-connect-ruby/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/HyukjinKwon/spark-connect-ruby/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/HyukjinKwon/spark-connect-ruby/releases/tag/v0.1.0
