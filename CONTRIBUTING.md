# Contributing

Thanks for your interest in improving **spark-connect**! Contributions of all
kinds - bug reports, documentation, examples, and code - are welcome.

## Development setup

Prerequisites:

- Ruby >= 3.1
- Apache Arrow C++/GLib system libraries (for the `red-arrow` dependency):
  - macOS: `brew install apache-arrow apache-arrow-glib`
  - Ubuntu/Debian: `libarrow-glib-dev` from the [Apache Arrow APT repo](https://arrow.apache.org/install/)
- Java 17+ if you want to run a local Spark Connect server for integration tests

```bash
git clone https://github.com/HyukjinKwon/spark-connect-ruby
cd spark-connect-ruby
bundle install
```

## Tests and linting

```bash
bundle exec rake spec        # unit specs - no server required
bundle exec rake rubocop     # lint
bundle exec rake             # spec + rubocop (the default task)
```

Unit specs must **not** require a live server: they assert on the protobuf plan a
DataFrame builds, or run against the in-memory `FakeClient` in
`spec/spec_helper.rb`.

### Integration tests

Integration specs live in `spec/integration/` and only run when `SPARK_REMOTE`
is set:

```bash
# start a local server first (see the README), then:
SPARK_REMOTE=sc://localhost:15002 bundle exec rspec spec/integration
```

## Regenerating the protobuf stubs

The generated stubs under `lib/spark_connect/proto/` are committed so installing
the gem needs no protobuf compiler. Regenerate them only when bumping the
vendored Spark Connect protocol version:

```bash
gem install grpc-tools
bin/generate-protos            # uses the version in PROTO_VERSION
bin/generate-protos v4.0.0     # or pin an explicit Spark ref
```

## Conventions

- **snake_case is canonical.** Add a camelCase alias only for high-traffic
  PySpark names (`groupBy`, `withColumn`, `orderBy`, ...).
- Every file starts with `# frozen_string_literal: true`.
- Document public methods with [YARD](https://yardoc.org/) tags.
- Keep `rubocop` clean (`.rubocop.yml` is the source of truth).
- Mirror PySpark semantics where reasonable; note any intentional deviations.

## Pull requests

1. Fork and create a feature branch.
2. Add or update specs for your change; keep the suite green.
3. Run `bundle exec rake` (spec + rubocop) before pushing.
4. Update `CHANGELOG.md` under "Unreleased".
5. Open a PR with a clear description of the motivation and behaviour.

By contributing, you agree that your contributions are licensed under the
project's [Apache 2.0 license](LICENSE).
