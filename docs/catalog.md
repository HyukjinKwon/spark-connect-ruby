---
title: Catalog
nav_order: 12
---

# Catalog

The `Catalog` is the entry point for inspecting and managing metadata:
catalogs, databases, tables, columns, and functions, plus the query cache and
temporary views. Obtain it from `SparkSession#catalog`. The API mirrors
PySpark's `spark.catalog`.

Methods that list metadata return arrays of [`Row`](dataframes.html#rows);
predicate methods (`*_exists`, `cached?`) return booleans; and management
methods (`cache_table`, `set_current_database`, ...) return `nil`.

```ruby
require "spark-connect"

spark = SparkConnect::SparkSession.builder.remote("sc://localhost:15002").get_or_create
cat = spark.catalog
```

## Catalogs and databases

```ruby
cat.current_catalog              #=> "spark_catalog"
cat.list_catalogs.each { |c| puts c["name"] }
cat.set_current_catalog("spark_catalog")

cat.current_database             #=> "default"
cat.list_databases.each { |db| puts db["name"] }
cat.database_exists("default")   #=> true
cat.set_current_database("default")
```

Each `Row` carries the metadata fields the server returns (for example a
database row has `name`, `catalog`, `description`, and `locationUri`). Access
them by name:

```ruby
db = cat.list_databases.first
puts "#{db["name"]} -> #{db["locationUri"]}"
```

## Tables, columns, and functions

```ruby
# All tables/views in the current database, or in a named database.
cat.list_tables.each { |t| puts "#{t["name"]} (#{t["tableType"]})" }
cat.list_tables("default")

cat.table_exists("my_table")            #=> true / false
cat.table_exists("my_table", "default") #=> true / false

# Columns of a table.
cat.list_columns("my_table").each do |c|
  puts "#{c["name"]}: #{c["dataType"]} (nullable=#{c["nullable"]})"
end

# Registered functions.
cat.list_functions.each { |f| puts f["name"] }
cat.function_exists("explode")          #=> true
```

## Temporary views

Register a temporary view with a SQL `CREATE` statement, query it through
`SparkSession#sql` or `SparkSession#table`, then drop it through the catalog.
The `drop_*` methods return a boolean indicating whether a view was removed.

```ruby
# Create a session-local temp view.
spark.sql("CREATE OR REPLACE TEMPORARY VIEW nums AS SELECT * FROM range(5)")
spark.table("nums").show
spark.sql("SELECT id * id AS sq FROM nums").show

# Create a global temp view (lives in the global_temp database).
spark.sql("CREATE GLOBAL TEMPORARY VIEW global_nums AS SELECT * FROM range(5)")
spark.table("global_temp.global_nums").show

# Drop views through the catalog.
cat.drop_temp_view("nums")             #=> true
cat.drop_global_temp_view("global_nums") #=> true
```

> The catalog owns the *teardown* of views (`drop_temp_view`,
> `drop_global_temp_view`); creation and querying go through
> [`SparkSession#sql`](getting-started.html) and `SparkSession#table`.

## Cache management

Spark can cache a table or view in memory so repeated queries avoid recomputing
it.

```ruby
cat.cache_table("my_table")
cat.cached?("my_table")     #=> true

# ... run queries that benefit from the cache ...

cat.uncache_table("my_table")
cat.clear_cache             # drop every cached table at once
```

## Refresh and partition recovery

After data files change underneath a table, refresh its cached metadata. For
partitioned tables whose partitions were added out-of-band, recover them so the
catalog sees the new partition directories.

```ruby
cat.refresh_table("my_table")
cat.recover_partitions("my_partitioned_table")
```

## Creating and dropping objects (Spark 4.2+)

```ruby
cat.create_database("analytics", if_not_exists: true, properties: { "comment" => "BI tables" })
cat.drop_table("analytics.old_events", if_exists: true, purge: true)
cat.drop_view("analytics.v", if_exists: true)
cat.drop_database("analytics", if_exists: true, cascade: true)
```

## Inspecting and maintaining tables (Spark 4.2+)

```ruby
cat.list_views("analytics", "daily_*")         # Array<Row>, like list_tables
cat.list_partitions("analytics.events")        # Rows with a `partition` String, e.g. "day=2026-06-10"
cat.get_table_properties("analytics.events")   # => {"owner" => "etl", ...}
cat.get_create_table_string("analytics.events") # the SHOW CREATE TABLE DDL
cat.analyze_table("analytics.events", no_scan: true)
cat.truncate_table("analytics.scratch")
```

`get_database`, `get_table`, and `get_function` return a single `Row`, and the
listing methods accept a SQL `LIKE` pattern (`list_tables(db, "fact_*")`).
`refresh_by_path(path)` refreshes everything cached under a path.

## Method summary

| Method                                   | Returns        | Purpose                              |
| ---------------------------------------- | -------------- | ------------------------------------ |
| `current_catalog`                        | `String`       | active catalog name                  |
| `set_current_catalog(name)`              | `nil`          | switch catalog                       |
| `list_catalogs`                          | `Array<Row>`   | all catalogs                         |
| `current_database`                       | `String`       | active database name                 |
| `set_current_database(name)`             | `nil`          | switch database                      |
| `list_databases(pattern = nil)`          | `Array<Row>`   | databases                            |
| `get_database(db)`                       | `Row`          | one database                         |
| `database_exists(db)`                    | `Boolean`      | database presence                    |
| `create_database(db, if_not_exists:, properties:)` | `nil` | create a database (4.2+)         |
| `drop_database(db, if_exists:, cascade:)` | `nil`         | drop a database (4.2+)               |
| `list_tables(db = nil, pattern = nil)`   | `Array<Row>`   | tables and views                     |
| `list_views(db = nil, pattern = nil)`    | `Array<Row>`   | views only (4.2+)                    |
| `get_table(name)`                        | `Row`          | one table or view                    |
| `table_exists(name, db = nil)`           | `Boolean`      | table/view presence                  |
| `drop_table(name, if_exists:, purge:)`   | `nil`          | drop a table (4.2+)                  |
| `drop_view(name, if_exists:)`            | `nil`          | drop a view (4.2+)                   |
| `list_columns(table, db = nil)`          | `Array<Row>`   | a table's columns                    |
| `list_partitions(table)`                 | `Array<Row>`   | partitions (4.2+)                    |
| `get_table_properties(table)`            | `Hash`         | table properties (4.2+)              |
| `get_create_table_string(table, as_serde:)` | `String`    | `SHOW CREATE TABLE` (4.2+)           |
| `truncate_table(table)`                  | `nil`          | remove all rows (4.2+)               |
| `analyze_table(table, no_scan:)`         | `nil`          | compute statistics (4.2+)            |
| `list_functions(db = nil, pattern = nil)` | `Array<Row>`  | registered functions                 |
| `get_function(name)`                     | `Row`          | one function                         |
| `function_exists(name, db = nil)`        | `Boolean`      | function presence                    |
| `drop_temp_view(name)`                   | `Boolean`      | drop a session temp view             |
| `drop_global_temp_view(name)`            | `Boolean`      | drop a global temp view              |
| `cached?(table)`                         | `Boolean`      | is the table cached                  |
| `cache_table(table, storage_level = nil)` | `nil`         | cache (optionally at a storage level) |
| `uncache_table(table)`                   | `nil`          | remove from cache                    |
| `clear_cache`                            | `nil`          | clear all cached tables              |
| `refresh_table(table)`                   | `nil`          | refresh cached metadata              |
| `refresh_by_path(path)`                  | `nil`          | refresh everything under a path      |
| `recover_partitions(table)`              | `nil`          | rediscover partitions                |

## See also

- [Configuration](configuration.html) for `spark.conf` and error handling.
- [DataFrames](dataframes.html) for working with the data behind these tables.
