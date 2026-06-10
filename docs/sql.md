---
title: Spark SQL
nav_order: 6
---

# Spark SQL

`spark-connect` lets you run SQL against your Spark Connect server with
`spark.sql`, register DataFrames as temporary views, and freely mix SQL with the
DataFrame API. Every `spark.sql(...)` call returns a lazy
[`DataFrame`]({{ "/dataframe.html" | relative_url }}) -- nothing executes until
you call an action such as `show`, `collect`, or `count`.

If you have used PySpark's `spark.sql`, this is the same idea: snake_case method
names, and parameters passed as a Hash (named) or an Array (positional).

## Running a query

```ruby
require "spark-connect"

spark = SparkConnect::SparkSession.builder
                                  .remote("sc://localhost:15002")
                                  .get_or_create

df = spark.sql("SELECT id, id * 2 AS doubled FROM range(5)")
df.show
# +---+-------+
# | id|doubled|
# +---+-------+
# |  0|      0|
# |  1|      2|
# |  2|      4|
# |  3|      6|
# |  4|      8|
# +---+-------+
```

The method signature is `sql(query, args = nil)`. The optional second argument
binds parameters into the query and accepts either a **Hash** (named parameters)
or an **Array** (positional parameters).

## Parameterized queries

Parameter binding is the safe way to inject values into SQL: the values are sent
to the server as typed expressions rather than spliced into the query string, so
there is no string-escaping or SQL-injection risk.

### Named parameters (Hash)

Reference parameters in the query with a leading colon (`:name`) and pass a Hash.
Hash keys may be Strings or Symbols.

```ruby
df = spark.sql(
  "SELECT * FROM range(100) WHERE id BETWEEN :lo AND :hi",
  { lo: 10, hi: 20 }
)
df.count # => 11
```

The same parameter name can appear multiple times in the query:

```ruby
spark.sql(
  "SELECT :n AS n, :n * :n AS n_squared",
  { n: 9 }
).show
# +---+---------+
# |  n|n_squared|
# +---+---------+
# |  9|       81|
# +---+---------+
```

### Positional parameters (Array)

Use `?` placeholders and pass an Array; placeholders are filled left to right.

```ruby
df = spark.sql(
  "SELECT * FROM range(100) WHERE id BETWEEN ? AND ?",
  [10, 20]
)
df.count # => 11
```

### How values are mapped

Each argument is converted with `Column.to_col`, which wraps a literal value as
a Spark expression. That means the Ruby-to-Spark value mapping is exactly the one
documented in
[Types and schemas]({{ "/types-and-schemas.html" | relative_url }}) (Integer,
Float, String, `Time`, `Date`, `BigDecimal`, and so on). You can also pass a
`Column` directly:

```ruby
F = SparkConnect::F

spark.sql(
  "SELECT * FROM range(10) WHERE id > :threshold",
  { threshold: F.lit(5) }
).show
```

## Temporary views

To query a DataFrame by name from SQL, register it as a temporary view. Views are
created with a SQL statement, and the DataFrame is made available to the server
session via the catalog. The simplest portable approach is to drive the
registration through SQL and then read it back:

```ruby
people = spark.create_data_frame(
  [
    { "name" => "Alice", "age" => 30 },
    { "name" => "Bob",   "age" => 25 },
  ]
)

# Register the DataFrame's plan under a name the SQL engine can resolve.
people.create_or_replace_temp_view("people")

adults = spark.sql("SELECT name FROM people WHERE age >= 18 ORDER BY age")
adults.show
```

When you are done, drop the view through the
[Catalog]({{ "/catalog.html" | relative_url }}):

```ruby
spark.catalog.drop_temp_view("people")        # session-scoped temp views
spark.catalog.drop_global_temp_view("people") # cross-session global temp views
```

> Global temporary views live in the special `global_temp` database, so you
> reference them as `global_temp.<name>` from SQL.

## Mixing SQL and the DataFrame API

Because `spark.sql` returns a regular `DataFrame`, you can chain DataFrame
operations onto a SQL result, and vice versa. Use whichever expresses each step
most clearly.

```ruby
F = SparkConnect::F

# Start in SQL, continue in the DataFrame API.
df = spark.sql("SELECT id FROM range(1000)")
          .filter(F.col("id") % 3 == 0)
          .with_column("bucket", F.col("id") % 10)
          .group_by("bucket")
          .agg(F.count(F.lit(1)).alias("n"))
          .order_by("bucket")

df.show
```

You can also build a DataFrame first, register it, and finish in SQL:

```ruby
sales = spark.create_data_frame(
  [
    { "region" => "west", "amount" => 100 },
    { "region" => "west", "amount" => 250 },
    { "region" => "east", "amount" =>  90 },
  ]
)
sales.create_or_replace_temp_view("sales")

spark.sql(<<~SQL).show
  SELECT region, SUM(amount) AS total
  FROM sales
  GROUP BY region
  ORDER BY total DESC
SQL
```

### `select_expr` for inline SQL expressions

When you only need SQL *expressions* (not a full query), `select_expr` (aliased
from `selectExpr`, mirroring PySpark) keeps you in the DataFrame API:

```ruby
spark.range(5).select_expr("id", "id * id AS squared").show
```

## See also

- [Reading and writing]({{ "/reading-and-writing.html" | relative_url }}) --
  load external data, then query it with SQL.
- [Types and schemas]({{ "/types-and-schemas.html" | relative_url }}) -- how Ruby
  values map to Spark types when you bind SQL parameters.
