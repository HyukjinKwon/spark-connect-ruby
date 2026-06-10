#!/usr/bin/env ruby
# frozen_string_literal: true

# 04_joins.rb
#
# Demonstrates every join type supported by the spark-connect gem using two
# in-memory DataFrames built with create_data_frame:
#   - inner, left (outer), right (outer), full outer
#   - left semi, left anti
#   - cross join (cartesian product)
#
# Joins are expressed with DataFrame#join(other, on:, how:). The `on:` argument
# may be a list of shared column names (a "using" join) or a Column condition.
#
# Run with a live Spark Connect server:
#   export SPARK_REMOTE="sc://localhost:15002"
#   ruby examples/04_joins.rb

require "spark-connect"

F = SparkConnect::F

spark = SparkConnect::SparkSession.builder
                                  .remote(ENV.fetch("SPARK_REMOTE", "sc://localhost:15002"))
                                  .app_name("04_joins")
                                  .get_or_create

# Left frame: employees with a department id.
employees = spark.create_data_frame(
  [
    { "name" => "Alice",   "dept_id" => 1 },
    { "name" => "Bob",     "dept_id" => 2 },
    { "name" => "Carol",   "dept_id" => 2 },
    { "name" => "Dan",     "dept_id" => 4 }, # no matching department
  ]
)

# Right frame: departments. Department 3 has no employees.
departments = spark.create_data_frame(
  [
    { "dept_id" => 1, "dept_name" => "Engineering" },
    { "dept_id" => 2, "dept_name" => "Sales" },
    { "dept_id" => 3, "dept_name" => "Marketing" },
  ]
)

puts "employees:"
employees.show
puts "departments:"
departments.show

# ---- Inner join: only rows with a match on both sides --------------------
puts "INNER join on dept_id:"
employees.join(departments, on: "dept_id", how: :inner)
         .order_by("name")
         .show

# ---- Left (outer) join: keep all employees; nulls where no department -----
puts "LEFT OUTER join on dept_id:"
employees.join(departments, on: "dept_id", how: :left)
         .order_by("name")
         .show

# ---- Right (outer) join: keep all departments; nulls where no employee ----
puts "RIGHT OUTER join on dept_id:"
employees.join(departments, on: "dept_id", how: :right)
         .order_by("dept_id")
         .show

# ---- Full outer join: keep everything from both sides ---------------------
puts "FULL OUTER join on dept_id:"
employees.join(departments, on: "dept_id", how: :outer)
         .order_by("dept_id")
         .show

# ---- Left semi join: employees that have a matching department (no right cols)
puts "LEFT SEMI join on dept_id:"
employees.join(departments, on: "dept_id", how: :semi)
         .order_by("name")
         .show

# ---- Left anti join: employees with NO matching department ----------------
puts "LEFT ANTI join on dept_id:"
employees.join(departments, on: "dept_id", how: :anti)
         .order_by("name")
         .show

# ---- Cross join: cartesian product (every employee x every department) ----
# Using a Column condition join is also possible; here we show crossJoin.
puts "CROSS join (employees x departments) count:"
crossed = employees.crossJoin(departments)
puts crossed.count

# A join using an explicit Column condition rather than a shared column name.
puts "Explicit condition join (employees.dept_id == departments.dept_id):"
employees.alias("e")
         .join(departments.alias("d"), on: F.col("e.dept_id") == F.col("d.dept_id"), how: :inner)
         .select("e.name", "d.dept_name")
         .order_by("name")
         .show

spark.stop
