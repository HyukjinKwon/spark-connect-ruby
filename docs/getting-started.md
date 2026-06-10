---
title: Getting started
nav_order: 3
---

# Getting started

This page assumes you have installed the gem and have a Spark Connect server
running (see [Installation]({{ "/installation.html" | relative_url }})). It
walks through connecting, building a session, creating DataFrames, and running
actions.

## Connecting with `sc://` URLs

You connect by giving the builder a Spark Connect connection string. The grammar
mirrors the official Spark Connect clients:

```
sc://host[:port][/;param=value;param=value...]
```

The host is required; the port defaults to **15002**. Parameters live after a
`/` and are separated by `;`. Recognized parameters:

| Parameter    | Meaning                                                              |
|--------------|----------------------------------------------------------------------|
| `token`      | Bearer token. Implies TLS and adds an `authorization` header.        |
| `user_id`    | The Spark user id.                                                   |
| `user_agent` | Client user agent (default `spark-connect-ruby/<version>`).          |
| `use_ssl`    | `true`/`false` -- force TLS on or off.                               |
| `session_id` | Reuse a specific server-side session id (UUID).                     |

Any parameter whose name starts with `x-` is forwarded verbatim as gRPC request
metadata.

Examples:

```ruby
# Plain local server, no TLS.
"sc://localhost:15002"

# A remote host with a bearer token (TLS is implied by the token).
"sc://spark.example.com:443/;token=abc123;user_id=alice"

# Force TLS on and set a custom user agent.
"sc://spark.example.com:15002/;use_ssl=true;user_agent=my-app/1.0"
```

## Building a session

`SparkSession` is the entry point. Build one with the fluent builder:

```ruby
require "spark-connect"

spark = SparkConnect::SparkSession.builder
                                  .remote("sc://localhost:15002")
                                  .app_name("getting-started")
                                  .config("spark.sql.shuffle.partitions", "8")
                                  .get_or_create
```

- `remote(url)` sets the connection string. If you omit it, the builder falls
  back to the `SPARK_REMOTE` environment variable, then to
  `sc://localhost:15002`.
- `app_name(name)` sets the application name.
- `config(key, value)` sets a runtime configuration option applied after
  connecting.
- `get_or_create` (alias `getOrCreate`) returns the active session if one
  exists, otherwise creates and activates a new one. Use `create` (alias
  `build`) to always make a fresh session.

You can inspect the server and session:

```ruby
spark.version     # => the Spark version reported by the server, e.g. "4.1.2"
spark.session_id  # => the client session id (UUID)
```

## `range`

The simplest DataFrame is an integer range with a single `id` column:

```ruby
spark.range(5).show
# +---+
# | id|
# +---+
# |  0|
# |  1|
# |  2|
# |  3|
# |  4|
# +---+
```

`range` accepts `range(end_)` or `range(start, end_, step = 1, num_partitions = nil)`:

```ruby
spark.range(10, 20, 2).show   # 10, 12, 14, 16, 18
```

## `sql`

Run Spark SQL and get back a lazy DataFrame:

```ruby
spark.sql("SELECT 1 AS a, 'hello' AS b").show
# +---+-----+
# |  a|    b|
# +---+-----+
# |  1|hello|
# +---+-----+
```

SQL queries support parameters. Pass a `Hash` for named parameters or an
`Array` for positional ones:

```ruby
# Named parameters (referenced as :name in the query).
spark.sql("SELECT * FROM range(100) WHERE id >= :lo AND id < :hi",
          { "lo" => 10, "hi" => 13 }).show

# Positional parameters (referenced as ? in the query).
spark.sql("SELECT * FROM range(100) WHERE id = ?", [42]).show
```

## `create_data_frame`

Build a DataFrame from local Ruby data. The method is `create_data_frame`,
aliased `create_dataframe` and `createDataFrame`. Data can be an array of
hashes, an array of arrays, or `Row` objects.

```ruby
# Array of hashes -- keys become column names, types are inferred.
people = spark.create_data_frame([
  { name: "Alice", age: 30 },
  { name: "Bob",   age: 25 },
])
people.show

# Array of arrays with an explicit column-name list.
nums = spark.createDataFrame([[1, "a"], [2, "b"]], ["n", "label"])
nums.show

# Or give a DDL schema string.
typed = spark.create_data_frame([[1, 2.5]], "id INT, score DOUBLE")
typed.print_schema
```

## A small transformation

Putting the DataFrame API together. `SparkConnect::F` is the functions module:

```ruby
F = SparkConnect::F

df = spark.create_data_frame([
  { dept: "eng",   salary: 100 },
  { dept: "eng",   salary: 120 },
  { dept: "sales", salary: 90 },
])

df.group_by("dept")
  .agg(F.sum("salary").alias("total"), F.avg("salary").alias("avg"))
  .order_by(F.col("total").desc)
  .show
```

## Actions: `show` and `collect`

DataFrames are lazy -- nothing runs on the server until you call an action.

`show` renders a formatted table to stdout:

```ruby
df = spark.range(3)
df.show                       # first 20 rows, truncated
df.show(5, truncate: false)   # first 5 rows, no truncation
df.show(5, vertical: true)    # one field per line
```

`collect` (alias `to_a`) executes the plan and returns an `Array` of `Row`
objects. A `Row` supports access by name and by position, and converts to a
`Hash`:

```ruby
rows = spark.range(3).collect
rows.each do |row|
  puts row["id"]   # by column name
  puts row[0]      # by position
end

spark.range(3).to_h_array  # => [{ "id" => 0 }, { "id" => 1 }, { "id" => 2 }]
```

Other common actions:

```ruby
spark.range(100).count        # => 100
spark.range(100).take(3)      # => first 3 Rows
spark.range(100).first        # => the first Row
spark.range(0).empty?         # => true
spark.range(5).to_arrow       # => an Arrow::Table (columnar)
```

## Stopping the session

When you are done, release the server-side session and stop the client:

```ruby
spark.stop
```

## Full example

```ruby
require "spark-connect"

F = SparkConnect::F

spark = SparkConnect::SparkSession.builder
                                  .remote("sc://localhost:15002")
                                  .app_name("getting-started")
                                  .get_or_create

begin
  df = spark.create_data_frame([
    { name: "Alice", dept: "eng",   salary: 100 },
    { name: "Bob",   dept: "eng",   salary: 120 },
    { name: "Carol", dept: "sales", salary: 90 },
  ])

  summary = df.group_by("dept")
              .agg(F.sum("salary").alias("total"))
              .order_by(F.col("total").desc)

  summary.show
  puts "departments: #{summary.count}"
ensure
  spark.stop
end
```

## Next steps

- Revisit the [Overview]({{ "/index.html" | relative_url }}) for the feature map.
- Review [Installation]({{ "/installation.html" | relative_url }}) if you need
  to bring up or upgrade a Spark Connect server.
