---
title: Installation
nav_order: 2
---

# Installation

This page covers everything you need to get `spark-connect` running: the
prerequisites, installing the gem, verifying the install, and bringing up a
local Spark Connect server to connect to.

## Prerequisites

### Ruby >= 3.1

The gem requires Ruby 3.1 or newer (it is tested against 3.1, 3.2, 3.3, and
3.4). Check your version:

```ruby
ruby --version
```

## Installing the gem

### Directly

```bash
gem install rubygems-requirements-system
gem install spark-connect
```

### With Bundler

Add it to your `Gemfile`:

```ruby
plugin "rubygems-requirements-system"
gem "spark-connect"
```

Then install:

```bash
bundle install
```

## Verifying the install

A quick smoke test that the gem and its native Arrow dependency load:

```ruby
require "spark-connect"

puts SparkConnect::VERSION         # => the gem version, e.g. "0.1.0"
puts SparkConnect::SPARK_VERSION   # => the Spark Connect protocol line, e.g. "4.1.0"
```

If `require "spark-connect"` raises a load error mentioning Arrow or GLib, the
native libraries above are missing or do not match the `red-arrow` gem version.

## Starting a local Spark Connect server

To run anything beyond loading the gem you need a Spark Connect server to talk
to. You do **not** need Spark on your client app machine -- only wherever the
server runs.

1. **Download a Spark distribution** (3.4+; this client is tested against 3.5.x
   and 4.x). Pick a release from the
   [Spark downloads page](https://spark.apache.org/downloads.html), or fetch one
   directly:

   ```bash
   mkdir -p ~/spark
   curl -fsSL "https://archive.apache.org/dist/spark/spark-4.1.0/spark-4.1.0-bin-hadoop3.tgz" \
     | tar xz -C ~/spark --strip-components=1
   ```

2. **Start the Connect server.** The `sbin/start-connect-server.sh` script
   launches the gRPC endpoint, by default on port **15002**. Spark 4.0.0+
   bundles the Connect server, so no extra packages are needed:

   ```bash
   ~/spark/sbin/start-connect-server.sh \
     --conf spark.log.level=WARN
   ```

   On **Spark 3.5.x** the Connect server is not bundled; pull it in with
   `--packages "org.apache.spark:spark-connect_2.13:3.5.5"` (use a Scala 2.13
   distribution).

3. **Confirm it is listening** on the gRPC port:

   ```bash
   bash -c "</dev/tcp/localhost/15002" && echo "Spark Connect is up"
   ```

To stop it later:

```bash
~/spark/sbin/stop-connect-server.sh
```

You are now ready to connect. Continue to
[Getting started]({{ "/getting-started.html" | relative_url }}).
