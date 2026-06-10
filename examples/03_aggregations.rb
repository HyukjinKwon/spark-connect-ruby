#!/usr/bin/env ruby
# frozen_string_literal: true

# 03_aggregations.rb
# -----------------------------------------------------------------------------
# Grouped aggregations: group_by.agg with multiple aggregates, plus the
# rollup / cube hierarchical aggregations and a pivot table.
#
# What it shows:
#   - group_by(...).agg(...) with several aggregate Columns at once
#     (F.count, F.sum, F.avg, F.max via SparkConnect::Functions)
#   - rollup (subtotals + grand total)
#   - cube (all combinations of grouping columns)
#   - pivot (turn distinct values of a column into columns)
#
# How to run:
#   export PATH="/opt/homebrew/lib/ruby/gems/3.4.0/bin:$PATH"
#   export SPARK_REMOTE="sc://localhost:15002"   # optional, this is the default
#   ruby examples/03_aggregations.rb
# -----------------------------------------------------------------------------

require "spark-connect"

F = SparkConnect::F

remote = ENV.fetch("SPARK_REMOTE", "sc://localhost:15002")

spark = SparkConnect::SparkSession.builder
                                  .remote(remote)
                                  .app_name("03_aggregations")
                                  .get_or_create

# Sales facts: one row per sale, tagged by department and region.
sales = spark.create_data_frame(
  [
    { dept: "eng",     region: "west", amount: 100 },
    { dept: "eng",     region: "east", amount: 200 },
    { dept: "eng",     region: "west", amount: 150 },
    { dept: "sales",   region: "west", amount: 80 },
    { dept: "sales",   region: "east", amount: 120 },
    { dept: "support", region: "east", amount: 60 }
  ]
)

puts "Source data:"
sales.show

# group_by(...).agg(...): one group key, several aggregates in one pass.
# Each aggregate is a Column built from SparkConnect::Functions and aliased.
puts "\ngroup_by(dept).agg(count, sum, avg, max):"
sales
  .group_by("dept")
  .agg(
    F.count("*").alias("n"),
    F.sum("amount").alias("total"),
    F.avg("amount").alias("avg_amount"),
    F.max("amount").alias("max_amount")
  )
  .order_by("dept")
  .show

# rollup: hierarchical subtotals. Grouping by (dept, region) also yields the
# per-dept subtotal and the grand total (NULLs mark the rolled-up levels).
puts "\nrollup(dept, region).agg(sum):"
sales
  .rollup("dept", "region")
  .agg(F.sum("amount").alias("total"))
  .order_by("dept", "region")
  .show

# cube: like rollup but emits every combination of the grouping columns,
# including totals per region and the grand total.
puts "\ncube(dept, region).agg(sum):"
sales
  .cube("dept", "region")
  .agg(F.sum("amount").alias("total"))
  .order_by("dept", "region")
  .show

# pivot: group_by(dept), then pivot the region values into columns of sums.
# Passing the explicit value list keeps the output deterministic.
puts "\ngroup_by(dept).pivot(region).sum(amount):"
sales
  .group_by("dept")
  .pivot("region", ["east", "west"])
  .sum("amount")
  .order_by("dept")
  .show

spark.stop
puts "\nDone."
