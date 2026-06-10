---
title: The DataFrame API
nav_order: 4
---

# The DataFrame API

A `DataFrame` is the central abstraction of `spark-connect`: a distributed,
lazily-evaluated collection of rows organised into named columns. If you know
PySpark's `pyspark.sql.DataFrame`, you already know this API - the method names
match, only the casing changes (Ruby snake_case, with camelCase aliases for the
most common operations).

```ruby
require "spark-connect"

F = SparkConnect::F

spark = SparkConnect::SparkSession.builder
                                  .remote("sc://localhost:15002")
                                  .get_or_create

df = spark.range(100)
df.filter(F.col("id") % 2 == 0)
  .select((F.col("id") * 10).alias("ten_x"))
  .order_by(F.col("ten_x").desc)
  .show(5)
```

## Laziness and immutability

Two properties govern everything below.

**Lazy.** A transformation never talks to the server. `select`, `filter`,
`join`, `with_column` and friends each build a new logical plan and return
immediately. Work is only sent to the Spark Connect server when you call an
*action* - `collect`, `count`, `show`, `take`, `to_arrow`, and so on. This lets
Spark see the whole pipeline at once and optimise it.

**Immutable.** A `DataFrame` is never modified in place. Every transformation
returns a *new* `DataFrame` wrapping a new plan; the original is untouched. This
makes it safe to branch a pipeline and reuse intermediate frames.

```ruby
base   = spark.range(10)
evens  = base.filter(F.col("id") % 2 == 0)   # new DataFrame
odds   = base.filter(F.col("id") % 2 == 1)   # base is unchanged
base.count   # still 10
```

Because transformations chain, the idiomatic style is a fluent pipeline. Each
link is cheap; nothing executes until the final `show`/`collect`.

## snake_case and camelCase

Method names are snake_case, the Ruby idiom. The highest-traffic PySpark names
*also* have camelCase aliases so PySpark code reads almost verbatim:
`groupBy`, `withColumn`, `withColumnRenamed`, `orderBy`, `dropDuplicates`,
`selectExpr`, `crossJoin`, `unionByName`, `printSchema`, `toDF`, and more. The
two spellings are identical methods:

```ruby
df.with_column("x2", F.col("id") * 2)   # Ruby style
df.withColumn("x2", F.col("id") * 2)    # PySpark style - same method
```

The rest of this guide uses snake_case; substitute the camelCase alias freely.

## Selecting columns

`select` projects a set of columns or expressions. Strings and symbols are
treated as column names; pass `Column` objects (see
[Columns and the F library](columns-and-functions.html)) for computed
expressions.

```ruby
df.select("id")
df.select(F.col("id"), (F.col("id") * 2).alias("doubled"))
df["id"]                       # index a single column off the DataFrame
df.select(F.col("*"))          # all columns
```

`select_expr` (alias `selectExpr`) takes raw SQL expression strings:

```ruby
df.select_expr("id", "id * 2 AS doubled", "id % 3 AS bucket")
```

## Filtering rows

`filter` keeps rows where a boolean condition holds. `where` is an alias. The
condition can be a `Column` or a SQL string.

```ruby
df.filter(F.col("id") > 50)
df.where(F.col("id").between(10, 20))
df.filter("id % 2 == 0")                       # SQL string condition
df.filter((F.col("id") > 10) & (F.col("id") < 90))
```

Use `&` (AND), `|` (OR) and `!`/`.not` (NOT) to combine conditions. Wrap each
comparison in parentheses - `&` binds tighter than the comparison operators in
Ruby.

## Adding and replacing columns

`with_column` (alias `withColumn`) adds a new column or replaces an existing one
by name:

```ruby
df.with_column("doubled", F.col("id") * 2)
  .with_column("id", F.col("id") + 1)          # replace existing "id"
```

`with_columns` (alias `withColumns`) adds several at once from a Hash:

```ruby
df.with_columns(
  "doubled" => F.col("id") * 2,
  "label"   => F.when(F.col("id") > 50, "big").otherwise("small")
)
```

## Renaming columns

`with_column_renamed` (alias `withColumnRenamed`) renames one column;
`with_columns_renamed` (alias `withColumnsRenamed`) renames several from a Hash
of `old => new`:

```ruby
df.with_column_renamed("id", "row_id")
df.with_columns_renamed("id" => "row_id", "doubled" => "x2")
```

`to_df` (alias `toDF`) renames *all* columns positionally:

```ruby
spark.range(3).select(F.col("id"), F.col("id") * 2).to_df("a", "b")
```

## Dropping columns

`drop` removes one or more columns, named either by string or `Column`:

```ruby
df.drop("doubled")
df.drop("a", "b")
df.drop(F.col("a"))
```

## Distinct and dropping duplicates

`distinct` returns the distinct rows. `drop_duplicates` (alias
`dropDuplicates`) does the same, but can be restricted to a subset of columns -
it keeps one row per distinct combination of those columns:

```ruby
df.distinct
df.drop_duplicates                       # all columns
df.drop_duplicates(["category"])         # one row per distinct category
```

## Ordering

`order_by` (aliases `sort`, `orderBy`) sorts globally. Pass column names for the
default ascending order, or `Column` sort expressions for control over
direction and null placement.

```ruby
df.order_by("id")
df.order_by(F.col("id").desc)
df.order_by(F.col("category").asc, F.col("id").desc_nulls_last)
```

`sort_within_partitions` (alias `sortWithinPartitions`) sorts each partition
locally without a global shuffle - useful before a partition-wise write.

```ruby
df.sort_within_partitions(F.col("id").asc)
```

## Limit and offset

`limit` keeps the first `n` rows; `offset` skips the first `n`. Combine them for
pagination (usually after an `order_by` so the result is deterministic).

```ruby
df.order_by("id").limit(10)             # first 10
df.order_by("id").offset(10).limit(10)  # rows 11-20
```

## Repartition and coalesce

`repartition` reshuffles the data into `num_partitions` partitions; pass columns
to hash-partition by them. `coalesce` *reduces* the partition count without a
full shuffle (it can only decrease partitions).

```ruby
df.repartition(8)                       # round-robin into 8 partitions
df.repartition(8, F.col("category"))    # hash-partition by category
df.coalesce(1)                          # merge down to 1 partition, no shuffle
```

## Joins

`join` joins with another `DataFrame`. The `on:` keyword is the join key(s) - a
column name, an array of names, or a boolean `Column` condition - and `how:`
selects the join type.

```ruby
left  = spark.create_data_frame([{ id: 1, name: "a" }, { id: 2, name: "b" }])
right = spark.create_data_frame([{ id: 1, score: 10 }, { id: 3, score: 30 }])

# Equi-join on a shared column name (the key appears once in the output):
left.join(right, on: "id")

# Join on a list of shared columns:
left.join(right, on: ["id"])

# Join on an explicit condition (both "id" columns survive):
left.join(right, on: left["id"] == right["id"])
```

The `how:` value accepts every Spark join type, with the usual spelling
variants:

| `how:` value | Meaning |
|---|---|
| `:inner` (default) | rows with a match on both sides |
| `:left`, `:left_outer`, `:leftouter` | all left rows; nulls where right is missing |
| `:right`, `:right_outer`, `:rightouter` | all right rows; nulls where left is missing |
| `:outer`, `:full`, `:full_outer`, `:fullouter` | all rows from both sides |
| `:semi`, `:left_semi`, `:leftsemi` | left rows that have a match (right columns dropped) |
| `:anti`, `:left_anti`, `:leftanti` | left rows with **no** match |
| `:cross` | Cartesian product |

```ruby
left.join(right, on: "id", how: :left)
left.join(right, on: "id", how: :full_outer)
left.join(right, on: "id", how: :left_semi)
left.join(right, on: "id", how: :anti)
```

`cross_join` (alias `crossJoin`) produces the Cartesian product directly,
without a condition:

```ruby
left.cross_join(right)   # every left row paired with every right row
```

## Set operations

`union` (aliases `union_all`, `unionAll`) stacks two DataFrames *by position*,
keeping duplicates. The two frames must have the same number of columns.

```ruby
a = spark.range(3)
b = spark.range(2)
a.union(b)               # 5 rows: 0,1,2,0,1
```

`union_by_name` (alias `unionByName`) matches columns by *name* rather than
position. Pass `allow_missing_columns: true` to tolerate differing schemas
(missing columns become null).

```ruby
a.union_by_name(b)
a.union_by_name(b, allow_missing_columns: true)
```

`intersect` returns distinct rows present in both frames; `intersect_all`
(alias `intersectAll`) keeps duplicates. `subtract` returns distinct rows in
this frame but not the other (Spark's `EXCEPT`); `except_all` (alias
`exceptAll`) is the duplicate-preserving variant.

```ruby
a.intersect(b)
a.intersect_all(b)
a.subtract(b)
a.except_all(b)
```

## Sampling

`sample` returns a random fraction of the rows. Set `with_replacement: true` to
allow the same row more than once, and pass a `seed:` for reproducibility.

```ruby
df.sample(0.1)                          # ~10% of rows
df.sample(0.5, seed: 42)                # reproducible
df.sample(0.2, with_replacement: true, seed: 7)
```

## Aliasing a DataFrame

`alias` (alias `as`) gives the frame a subquery name, which disambiguates
columns in self-joins and join conditions.

```ruby
l = df.alias("l")
r = df.alias("r")
l.join(r, on: F.col("l.id") == F.col("r.id") - 1)
```

## Hints

`hint` attaches a planner hint, e.g. to force a broadcast join. The convenience
function `F.broadcast(df)` is shorthand for `df.hint("broadcast")`.

```ruby
small = spark.range(10)
big   = spark.range(1_000_000)
big.join(small.hint("broadcast"), on: "id")
big.join(F.broadcast(small), on: "id")    # equivalent
```

## Unpivot (melt)

`unpivot` (alias `melt`) reshapes wide data to long: it keeps the `ids` columns
and folds the `values` columns into two columns - one holding the original
column name, one holding the value. Pass `nil` for `values` to unpivot all
non-id columns.

```ruby
wide = spark.create_data_frame([
  { id: 1, jan: 10, feb: 20 },
  { id: 2, jan: 30, feb: 40 }
])

wide.unpivot(["id"], ["jan", "feb"], "month", "amount")
# id | month | amount
#  1 | jan   | 10
#  1 | feb   | 20
#  2 | jan   | 30
#  2 | feb   | 40
```

## Actions

Actions trigger execution and return concrete Ruby values (or print output).

`collect` (alias `to_a`) returns **all** rows as an `Array` of `Row` objects. A
`Row` indexes by position or field name and converts to a Hash:

```ruby
rows = df.limit(3).collect
rows.first[0]          # by position
rows.first["id"]       # by name
rows.first.id          # by method (for identifier-safe names)
rows.first.to_h        # { "id" => 0 }
```

`take(n)` returns the first `n` rows as an Array. `head` returns the single
first `Row` when called with no argument, or the first `n` rows as an Array when
given a count. `first` returns the single first `Row` (or `nil`).

```ruby
df.take(5)             # Array of up to 5 Rows
df.head               # single Row
df.head(5)            # Array of up to 5 Rows
df.first              # single Row, or nil if empty
```

`count` returns the number of rows. `empty?` (alias `is_empty`) is a cheap check
for zero rows.

```ruby
df.count               # => 100
df.filter(F.col("id") < 0).empty?   # => true
```

`show` prints the first `n` rows (default 20) as a formatted table. Use
`truncate:` to control cell width (`true` = 20 chars, `false` = no truncation,
or an Integer width) and `vertical: true` for a record-per-block layout.
`show_string` returns that table as a `String` instead of printing.

```ruby
df.show                            # first 20 rows
df.show(5)                         # first 5
df.show(5, truncate: false)        # don't clip long values
df.show(5, vertical: true)         # one field per line
table = df.show_string(5)          # capture instead of print
```

`to_arrow` materialises the result as an Apache Arrow `Table` (columnar) for
zero-copy interop with the Ruby Arrow ecosystem. `to_h_array` returns every row
as a Hash.

```ruby
df.to_arrow             # Arrow::Table
df.to_h_array           # Array<Hash>
```

## Schema and metadata

These read the schema (a lightweight analyze call, not a full execution).

```ruby
df.schema               # Types::StructType
df.columns              # ["id", ...] column names
df.dtypes               # [["id", "bigint"], ...] name/type pairs
df.print_schema         # prints the schema tree (alias printSchema)
```

```ruby
df.print_schema
# root
#  |-- id: long (nullable = false)
```

## Explain

`explain` prints the query plan; `explain_string` returns it as a String. The
mode selects the level of detail: `:simple` (default), `:extended`, `:codegen`,
`:cost`, or `:formatted`.

```ruby
df.filter(F.col("id") > 5).explain
df.explain(:extended)
plan = df.explain_string(:formatted)
```

## Where to go next

- [Columns and the F library](columns-and-functions.html) - build the
  expressions you pass to `select`, `filter`, `with_column`, and the rest.
