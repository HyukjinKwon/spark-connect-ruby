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
```

Use `option` for a single setting and `options(hash)` for several.

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
df.write_to("catalog.db.events").partition_by("day").create_or_replace
df.write_to("catalog.db.events").overwrite(F.col("day") == "2026-06-10")
```
