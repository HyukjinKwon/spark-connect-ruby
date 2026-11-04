---
title: Reading & Writing
nav_order: 7
---

# Reading & Writing Data

## Reading

`spark.read` returns a {`SparkConnect::DataFrameReader`}.

```ruby
# Generic
spark.read
     .format("csv")
     .option("header", true)
     .option("inferSchema", true)
     .load("data/people.csv")

# Format shortcuts
spark.read.json("events.json")
spark.read.parquet("/warehouse/sales")
spark.read.orc("/warehouse/orc")
spark.read.text("notes.txt")

# Tables and views
spark.read.table("default.people")
spark.table("default.people")    # shorthand on the session

# Explicit schema (StructType or DDL string)
schema = SparkConnect::Types.struct(
  SparkConnect::Types.field("id", SparkConnect::Types.long),
  SparkConnect::Types.field("name", SparkConnect::Types.string),
)
spark.read.schema(schema).json("people.json")
spark.read.schema("id BIGINT, name STRING").csv("people.csv")

# JDBC
spark.read.jdbc(
  "jdbc:postgresql://db/app",
  "public.users",
  { "user" => "ro", "password" => ENV["PGPASSWORD"], "driver" => "org.postgresql.Driver" },
)
# ... partitioned on a numeric column, or by explicit predicates
spark.read.jdbc(url, "users", props, column: "id", lower_bound: 0, upper_bound: 1_000_000, num_partitions: 8)
spark.read.jdbc(url, "users", props, predicates: ["region = 'EU'", "region = 'US'"])

# XML (set the row tag)
spark.read.option("rowTag", "book").xml("books.xml")

# Parse a DataFrame of JSON / CSV / XML strings (Spark 4.2+ for XML)
lines = spark.read.text("events.jsonl")
spark.read.schema("id BIGINT, kind STRING").json(lines)

# Row-level changes (Change Data Capture) of a table, Spark 4.2+; the table's
# catalog must support change logs.
spark.read.option("startingVersion", 10).changes("catalog.db.events")
```

Use `option` for a single setting and `options(hash)` for several (`nil`
values are ignored, like PySpark).

## Writing

`df.write` returns a {`SparkConnect::DataFrameWriter`}.

```ruby
df.write
  .format("parquet")
  .mode(:overwrite)            # :append, :overwrite, :ignore, :error
  .option("compression", "snappy")
  .partition_by("year", "month")
  .save("/warehouse/sales")

# Format shortcuts
df.write.mode(:append).parquet("/warehouse/sales")
df.write.json("/out/events")

# Tables
df.write.mode(:overwrite).save_as_table("default.sales")
df.write.insert_into("default.sales")

# Bucketing and sorting (table writes)
df.write.bucket_by(8, "user_id").sort_by("ts").save_as_table("events")

# Clustering, XML, JDBC
df.write.cluster_by("region", "day").save_as_table("events")
df.write.option("rowTag", "book").xml("/out/books")
df.write.jdbc("jdbc:postgresql://db/app", "public.events", :append, "user" => "etl")

# Automatic schema evolution (Spark 4.2+; the table must support it)
df.write.mode(:append).with_schema_evolution.save_as_table("catalog.db.events")
```

### Save modes

| Symbol                              | Behaviour |
| ----------------------------------- | --------- |
| `:append`                           | add to existing data |
| `:overwrite`                        | replace existing data |
| `:ignore`                           | no-op if data exists |
| `:error` / `:error_if_exists`       | raise if data exists (default) |

## The v2 (catalog) writer

`df.write_to(table)` returns a {`SparkConnect::DataFrameWriterV2`} for
DataSourceV2 catalogs.

```ruby
df.write_to("catalog.db.events").using("iceberg").create
df.write_to("catalog.db.events").append
df.write_to("catalog.db.events").partitioned_by(F.days("ts"), F.bucket(16, "id")).create_or_replace
df.write_to("catalog.db.events").cluster_by("region").options("k" => "v").create
df.write_to("catalog.db.events").with_schema_evolution.append   # Spark 4.2+
df.write_to("catalog.db.events").overwrite(F.col("day") == "2026-06-10")
```

`partitioned_by` (alias `partition_by`) takes columns or partition transforms
(`F.years`, `F.months`, `F.days`, `F.hours`, `F.bucket`).

## MERGE INTO

See [`DataFrame#merge_into`](dataframe.html#merge-into).
