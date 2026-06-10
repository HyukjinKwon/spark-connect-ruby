---
title: Troubleshooting
nav_order: 12
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

The client is generated against the Spark Connect 4.1 protocol and supports
Apache Spark 3.5 and above. If you connect to an older
server and a specific relation or function is rejected, that feature may not
exist server-side; check your Spark version with `spark.version`.

## createDataFrame type errors

When building a DataFrame from local Ruby data without an explicit schema, the
schema is inferred from the first non-nil value in each column. If a column
mixes types or is entirely nil, pass an explicit schema (a `StructType` or a DDL
string) to avoid ambiguity.

## Getting help

Open an issue at
<https://github.com/HyukjinKwon/spark-connect-ruby/issues> with the Ruby
version, gem version, Spark server version, and a minimal reproduction.
