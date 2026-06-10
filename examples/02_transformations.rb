#!/usr/bin/env ruby
# frozen_string_literal: true

# 02_transformations.rb
# -----------------------------------------------------------------------------
# A tour of the core, narrow DataFrame transformations. All of these are lazy:
# nothing executes on the server until an action (show/collect/count) runs.
#
# What it shows:
#   - create_data_frame from local Ruby data
#   - select / filter (where)
#   - with_column (add) and with_column_renamed
#   - drop columns
#   - distinct
#   - order_by (sort)
#   - limit
#
# How to run:
#   export PATH="/opt/homebrew/lib/ruby/gems/3.4.0/bin:$PATH"
#   export SPARK_REMOTE="sc://localhost:15002"   # optional, this is the default
#   ruby examples/02_transformations.rb
# -----------------------------------------------------------------------------

require "spark-connect"

F = SparkConnect::F

remote = ENV.fetch("SPARK_REMOTE", "sc://localhost:15002")

spark = SparkConnect::SparkSession.builder
                                  .remote(remote)
                                  .app_name("02_transformations")
                                  .get_or_create

# Build a small DataFrame from in-memory rows. Each Hash key becomes a column.
people = spark.create_data_frame(
  [
    { name: "Alice",   dept: "eng",     salary: 120 },
    { name: "Bob",     dept: "eng",     salary: 95 },
    { name: "Carol",   dept: "sales",   salary: 80 },
    { name: "Dan",     dept: "sales",   salary: 80 },
    { name: "Eve",     dept: "support", salary: 70 }
  ]
)

puts "Source data:"
people.show

# select: keep only some columns (also accepts Column expressions).
puts "\nselect(name, salary):"
people.select("name", "salary").show

# filter / where: keep rows matching a boolean Column expression.
puts "\nfilter(salary >= 90):"
people.filter(F.col("salary") >= 90).show

# with_column: add (or replace) a column from an expression.
# with_column_renamed: rename an existing column.
puts "\nwith_column(bonus) + with_column_renamed(salary -> base_salary):"
people
  .with_column("bonus", F.col("salary") * 0.1)
  .with_column_renamed("salary", "base_salary")
  .show

# drop: remove one or more columns.
puts "\ndrop(dept):"
people.drop("dept").show

# distinct: deduplicate rows. Here we project to one column first.
puts "\nselect(salary).distinct:"
people.select("salary").distinct.order_by("salary").show

# order_by (alias: sort, orderBy): sort by columns; use .desc for descending.
puts "\norder_by(salary desc):"
people.order_by(F.col("salary").desc).show

# limit: cap the number of rows returned.
puts "\norder_by(name).limit(2):"
people.order_by("name").limit(2).show

spark.stop
puts "\nDone."
