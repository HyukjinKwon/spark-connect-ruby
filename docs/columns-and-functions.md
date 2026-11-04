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
df["age"]             # a column bound to `df` (unambiguous in self-joins)
F.col("*")            # all columns
F.col("t.*")          # all columns of the aliased relation `t`
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
| Bitwise    | `bitwise_and`, `bitwise_or`, `bitwise_xor` (`bitwiseAND`, ...) |

`isin` also accepts a single-column DataFrame, building an `IN (subquery)`
(Spark 4.0+): `F.col("id").isin(other.select("id"))`.

## Aliasing, casting, and ordering

```ruby
(F.col("a") + F.col("b")).alias("sum")
F.col("amount").cast("decimal(10,2)")
F.col("amount").cast(SparkConnect::Types.double)
F.col("amount").try_cast("int")   # null instead of an error (Spark 4.0+)
F.col("score").desc          # sort order (also asc, asc_nulls_last, ...)
F.col("x").transform { |c| c * 2 }  # apply a custom function
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
F.col("address").with_field("zip", F.lit("00000"))   # add or replace a struct field
F.col("address").drop_fields("street", "unit")       # drop struct fields
```

## The function library

`SparkConnect::F` provides every function of PySpark 4.3's
`pyspark.sql.functions` except the UDF family (`udf`, `udtf`, `arrow_udtf`), and
builds exactly the expression PySpark builds for the same arguments, so PySpark
code ports verbatim. All functions return a `Column`.

Arguments follow PySpark's typing: where PySpark takes a `ColumnOrName`, a
**String** is a **column name** (`F.sum("salary")`, `F.regexp_like("s",
"pattern_col")`) and you wrap literals with `F.lit` (`F.regexp_like("s",
F.lit("a.*"))`); numbers and booleans are literals either way. Where PySpark
takes a plain Python value (`str`, `int`, ...), the argument is always a
**literal**, e.g. the pattern of `F.regexp_extract(F.col("s"), "\\d+", 0)` or the
format of `F.date_format("ts", "yyyy-MM")`.

`F.rand`, `F.randn`, `F.uuid`, `F.shuffle`, `F.randstr`, `F.uniform`, and
`F.count_min_sketch` pick their seed on the client when none is given (like
PySpark), so a DataFrame returns the same values on every action.

{: .note }
> On Spark 3.5 servers, `F.uuid`, `F.shuffle`, and `F.mode` need the Spark 4.0
> argument forms these functions now always send (a seed, or the
> `deterministic` flag); use `F.expr("uuid()")` and friends there.

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

# Null handling and safe arithmetic
F.nvl("a", "b"); F.nullif("a", "b"); F.try_divide("a", "b"); F.try_add("a", "b")

# CSV / XML / URLs / encryption
F.from_csv("line", "a INT, b STRING", { "sep" => ";" }); F.from_xml("x", schema)
F.parse_url("url", F.lit("HOST")); F.aes_encrypt("data", F.lit("0123456789abcdef"))

# Semi-structured data (VARIANT; mutation functions need Spark 4.3+)
F.parse_json("j"); F.variant_get("v", "$.a", "int"); F.variant_set("v", "$.b", F.lit(1))

# Sketches (HLL, KLL, Theta, Tuple)
F.hll_sketch_agg("x"); F.kll_sketch_agg_double("x"); F.theta_sketch_agg("x")

# Vectors (Spark 4.3+)
F.vector_cosine_similarity("a", "b"); F.vector_norm("v")

# Time windows and TIME values
F.window("ts", "10 minutes"); F.session_window("ts", "5 minutes"); F.time_trunc(F.lit("HOUR"), "t")

# Calling functions by name
F.call_function("my_catalog.my_db.my_fn", "a"); F.call_udf("registered_udf", "a")
```

Analytic functions are combined with a window via {`Column#over`} - see
[Aggregations & Windows](aggregations-and-windows.html).

```ruby
w = SparkConnect::Window.partition_by("dept").order_by(F.col("salary").desc)
F.rank.over(w)
F.lag("salary", 1).over(w)
```
