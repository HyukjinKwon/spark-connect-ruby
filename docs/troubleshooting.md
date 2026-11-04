---
title: Troubleshooting
nav_order: 14
---

# Troubleshooting

## Apache Arrow / red-arrow installation

`spark-connect` decodes results using the `red-arrow` gem, which binds to the
Apache Arrow C++/GLib system libraries. If you see a load error such as
`cannot load such file -- arrow` or a GObject introspection error, the system
libraries are missing or their version does not match the gem.

- macOS (Homebrew):
  ```bash
  brew install apache-arrow apache-arrow-glib
  ```
- Ubuntu/Debian: install `libarrow-glib-dev` from the
  [Apache Arrow APT repository](https://arrow.apache.org/install/).

The `red-arrow` gem version must match the **major version** of the installed
Arrow libraries. If they differ, install the matching gem version, for example:

```bash
SPARK_CONNECT_RED_ARROW_VERSION=22.0.0 bundle install
```

## gRPC connection problems

- `GRPC::Unavailable` / connection refused: confirm the Spark Connect server is
  running and reachable at the host and port in your `sc://` URL (default port
  `15002`).
- Hanging on the first request: the client retries transient failures with
  backoff. A wrong host will retry several times before giving up. Double-check
  the endpoint.
- TLS errors: a `token` parameter implies TLS. Use `use_ssl=false` for a
  plaintext local server, or `use_ssl=true` for a TLS endpoint without a token.

## Version compatibility

The client is generated against the Spark Connect 4.3 protocol and supports
Apache Spark 3.5 and above. If you connect to an older
server and a specific relation or function is rejected, that feature may not
exist server-side; check your Spark version with `spark.version`. The newer
APIs and the first Spark release whose server understands them:

| Spark | APIs |
| ----- | ---- |
| 4.3   | `DataFrame#zip`, nanosecond timestamp types, the `F.vector_*`, `F.variant_*` mutation, `F.hmac`, `F.to_base32`/`F.from_base32`, and `F.timestamp_nanos` functions |
| 4.2   | `nearest_by_join`, `read.changes`/`read_stream.changes`, `read_stream.name`, `trigger(real_time: ...)`, `with_schema_evolution`, the XML `read.xml(DataFrame)` form, the catalog DDL/maintenance methods (`drop_table`, `create_database`, `list_partitions`, `truncate_table`, ...), `get_operation_statuses`, Auto CDC flows, the `F.tuple_*`/`F.kll_merge_agg_*`/`F.time_*` functions |
| 4.1   | `clone_session`, `repartition_by_id`, the TIME and spatial (geometry/geography) types, Declarative Pipelines |
| 4.0   | subqueries (`scalar`/`exists`/`isin(df)`), `as_table`/`TableArg`, `lateral_join`, `transpose`, `tvf`, `merge_into`, `group_by(..., grouping_sets:)`, progress handlers, streaming query listeners |

On Spark 3.5 servers, `F.uuid`, `F.shuffle`, and `F.mode` need the Spark 4.0
argument forms these functions now send; use `F.expr("uuid()")` and friends
there.

## createDataFrame type errors

When building a DataFrame from local Ruby data without an explicit schema, the
schema is inferred from the first non-nil value in each column. If a column
mixes types or is entirely nil, pass an explicit schema (a `StructType` or a DDL
string) to avoid ambiguity.

## Getting help

Open an issue at
<https://github.com/HyukjinKwon/spark-connect-ruby/issues> with the Ruby
version, gem version, Spark server version, and a minimal reproduction.
