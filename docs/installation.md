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

### Apache Arrow GLib system libraries

`spark-connect` decodes query results using
[`red-arrow`](https://rubygems.org/gems/red-arrow), which is a binding over the
**Apache Arrow C++ / GLib** libraries. Those native libraries must be installed
on your system **before** `bundle install`, and the installed Arrow version must
match the `red-arrow` gem version.

#### macOS (Homebrew)

```bash
brew install apache-arrow apache-arrow-glib
```

#### Ubuntu / Debian

These steps mirror the project's CI (`.github/workflows/ci.yml`): they add the
official Apache Arrow APT repository and then install the GLib development
packages.

```bash
sudo apt-get update
sudo apt-get install -y -V ca-certificates lsb-release wget
wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb"
sudo apt-get install -y -V "./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb"
sudo apt-get update
sudo apt-get install -y -V libarrow-glib-dev libparquet-glib-dev
```

> **Matching versions matters.** `red-arrow` must be the same major version as
> the Arrow GLib libraries it loads. If you install a specific Arrow version
> locally, pin `red-arrow` to match. The project's development `Gemfile`
> demonstrates this with a `SPARK_CONNECT_RED_ARROW_VERSION` override, for
> example `SPARK_CONNECT_RED_ARROW_VERSION=22.0.0`.

## Installing the gem

### Directly

```bash
gem install spark-connect
```

### With Bundler

Add it to your `Gemfile`:

```ruby
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
puts SparkConnect::SPARK_VERSION   # => the Spark Connect protocol line, e.g. "4.0.0"
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
   curl -fsSL "https://archive.apache.org/dist/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz" \
     | tar xz -C ~/spark --strip-components=1
   ```

2. **Start the Connect server.** The `sbin/start-connect-server.sh` script
   launches the gRPC endpoint, by default on port **15002**. Pass the matching
   Spark Connect package for your Spark version:

   ```bash
   ~/spark/sbin/start-connect-server.sh \
     --packages "org.apache.spark:spark-connect_2.13:4.0.0" \
     --conf spark.log.level=WARN
   ```

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
