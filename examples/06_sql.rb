#!/usr/bin/env ruby
# frozen_string_literal: true

# 06_sql.rb
#
# Demonstrates spark.sql:
#   - plain SQL queries
#   - parameterized SQL with NAMED parameters (a Hash of name => value)
#   - parameterized SQL with POSITIONAL parameters (an Array bound to `?` marks)
#   - registering a temporary view (via CREATE OR REPLACE TEMP VIEW) and
#     querying it, then dropping it through the catalog
#
# Parameters are passed as the optional second argument to spark.sql: a Hash
# binds named markers (:name / `:name`), an Array binds positional `?` markers.
#
# Run with a live Spark Connect server:
#   export SPARK_REMOTE="sc://localhost:15002"
#   ruby examples/06_sql.rb

require "spark-connect"

spark = SparkConnect::SparkSession.builder
                                  .remote(ENV.fetch("SPARK_REMOTE", "sc://localhost:15002"))
                                  .app_name("06_sql")
                                  .get_or_create

# ---- Plain SQL ------------------------------------------------------------
puts "plain SQL (SELECT over a range):"
spark.sql("SELECT id, id * id AS squared FROM range(5)").show

# ---- Named parameters -----------------------------------------------------
# Named markers look like `:min_id`. The second argument is a Hash.
puts "named parameters (:min_id, :limit):"
spark.sql(
  "SELECT id FROM range(100) WHERE id >= :min_id ORDER BY id LIMIT :limit",
  { "min_id" => 10, "limit" => 3 }
).show

# ---- Positional parameters ------------------------------------------------
# Positional markers are `?`, filled in order from the Array.
puts "positional parameters (?, ?):"
spark.sql(
  "SELECT ? AS greeting, ? AS answer",
  ["hello", 42]
).show

# ---- Temporary views ------------------------------------------------------
# Build a DataFrame, register it as a temp view via SQL, then query the view.
people = spark.create_data_frame(
  [
    { "name" => "Alice", "age" => 30 },
    { "name" => "Bob",   "age" => 25 },
    { "name" => "Carol", "age" => 35 },
  ]
)

# Register the relation as a session-scoped temporary view. We round-trip the
# rows through an inline VALUES temp view so the view is independent of the
# original DataFrame plan.
spark.sql(<<~SQL)
  CREATE OR REPLACE TEMP VIEW people AS
  SELECT * FROM VALUES
    ('Alice', 30),
    ('Bob',   25),
    ('Carol', 35)
  AS t(name, age)
SQL

puts "input people DataFrame:"
people.show

puts "querying the 'people' temp view with a named parameter:"
spark.sql(
  "SELECT name, age FROM people WHERE age >= :min_age ORDER BY age",
  { "min_age" => 30 }
).show

# Clean up the temporary view through the catalog facade.
dropped = spark.catalog.drop_temp_view("people")
puts "dropped temp view 'people': #{dropped}"

spark.stop
