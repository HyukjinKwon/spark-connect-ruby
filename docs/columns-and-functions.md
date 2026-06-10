---
title: Columns & Functions
nav_order: 5
---

# Columns & Functions

A {`SparkConnect::Column`} represents a column expression - a lazily-evaluated
computation over the columns of a DataFrame. The function library
`SparkConnect::Functions` (aliased `SparkConnect::F`) builds and combines them.

```ruby
F = SparkConnect::F
```

## Referencing columns

```ruby
F.col("age")          # by name
F.lit(42)             # a literal value
F.expr("age + 1")     # a SQL expression string
df["age"]             # indexing a DataFrame
F.col("*")            # all columns
```

`F.lit` accepts `nil`, booleans, integers, floats, strings, symbols, `Time`,
`Date`, `BigDecimal`, arrays, and hashes (maps), encoding each to the matching
Spark literal type.

## Operators

Columns override Ruby's operators, so expressions read naturally:

```ruby
F.col("price") * F.col("qty")
F.col("age") + 1
F.col("a") == F.col("b")          # equality -> boolean column
F.col("age") >= 18
(F.col("a") > 0) & (F.col("b") < 10)   # boolean AND
(F.col("x").is_null) | F.col("y").is_not_null
-F.col("delta")                   # negation
F.col("flag").eq_null_safe(nil)   # null-safe equality (<=>)
```

| Category   | Operators / methods |
| ---------- | ------------------- |
| Arithmetic | `+ - * / % **`, unary `-` |
| Comparison | `== != < <= > >=`, `eq_null_safe` |
| Boolean    | `& | !`, `not` |
| Predicates | `is_null`, `is_not_null`, `is_nan`, `between`, `isin`, `like`, `rlike`, `ilike`, `contains`, `startswith`, `endswith` |

## Aliasing, casting, and ordering

```ruby
(F.col("a") + F.col("b")).alias("sum")
F.col("amount").cast("decimal(10,2)")
F.col("amount").cast(SparkConnect::Types.double)
F.col("score").desc          # sort order (also asc, asc_nulls_last, ...)
```

## Conditionals

```ruby
F.when(F.col("score") >= 90, "A")
 .when(F.col("score") >= 80, "B")
 .otherwise("C")
 .alias("grade")
```

## Complex-type access

```ruby
F.col("tags")[0]            # array element / map value / struct field
F.col("address").get_field("city")
```

## The function library

All functions return a `Column`. Following PySpark, a **String** argument is a
**column name** for most functions (`F.sum("salary")`), but a **literal value**
for functions whose parameters are genuinely literal (regex patterns, date
formats, JSON paths, ...), e.g. `F.regexp_extract(F.col("s"), "\\d+", 0)`.

```ruby
# Aggregate
F.count("*"); F.count_distinct("a", "b"); F.sum("x"); F.avg("x")
F.max("x"); F.min("x"); F.collect_list("x"); F.approx_count_distinct("x")

# Math
F.abs("x"); F.sqrt("x"); F.round(F.col("x"), 2); F.pow("x", "y")

# String
F.upper("name"); F.length("name"); F.concat_ws("-", "a", "b")
F.substring("s", 1, 3); F.regexp_replace(F.col("s"), "a", "b"); F.trim("s")

# Date / time
F.current_date; F.year("d"); F.date_add("d", 7); F.date_format("ts", "yyyy-MM")

# Collections
F.size("arr"); F.array_contains("arr", 3); F.explode("arr"); F.sort_array("arr")
F.map_keys("m"); F.element_at("arr", 1)

# JSON
F.get_json_object(F.col("j"), "$.name"); F.from_json(F.col("j"), schema)

# Higher-order (lambdas)
F.transform("nums") { |x| x * 2 }
F.filter("nums") { |x| x > 0 }
F.aggregate("nums", F.lit(0), ->(acc, x) { acc + x })

# Hashing / misc
F.hash("a", "b"); F.coalesce("a", "b"); F.monotonically_increasing_id
```

Analytic functions are combined with a window via {`Column#over`} - see
[Aggregations & Windows](aggregations-and-windows.html).

```ruby
w = SparkConnect::Window.partition_by("dept").order_by(F.col("salary").desc)
F.rank.over(w)
F.lag("salary", 1).over(w)
```
