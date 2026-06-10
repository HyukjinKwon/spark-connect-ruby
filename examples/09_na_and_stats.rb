#!/usr/bin/env ruby
# frozen_string_literal: true

# 09_na_and_stats.rb
#
# Demonstrates the missing-data helpers exposed by DataFrame#na
# (DataFrameNaFunctions) and the statistical helpers exposed by DataFrame#stat
# (DataFrameStatFunctions), plus describe / summary:
#
#   na:   drop, fill (scalar and per-column map), replace
#   stat: corr, approx_quantile, crosstab
#   df:   describe, summary
#
# Run with:
#   export PATH="/opt/homebrew/lib/ruby/gems/3.4.0/bin:$PATH"
#   ruby examples/09_na_and_stats.rb
#
# The endpoint comes from SPARK_REMOTE (defaults to sc://localhost:15002).

require "spark-connect"

spark = SparkConnect::SparkSession.builder
                                  .remote(ENV.fetch("SPARK_REMOTE", "sc://localhost:15002"))
                                  .app_name("09_na_and_stats")
                                  .get_or_create

begin
  # Data with some nulls so the na helpers have something to act on.
  rows = [
    { "name" => "Alice", "dept" => "eng",   "age" => 30,  "salary" => 100.0 },
    { "name" => "Bob",   "dept" => nil,     "age" => nil, "salary" => 80.0 },
    { "name" => "Carol", "dept" => "eng",   "age" => 41,  "salary" => 120.0 },
    { "name" => nil,     "dept" => "sales", "age" => 25,  "salary" => nil },
  ]
  df = spark.create_data_frame(rows)
  puts "== Original (with nulls) =="
  df.show

  # --- na.drop ----------------------------------------------------------------
  # Drop any row that has a null in any column.
  puts "== na.drop(how: :any) =="
  df.na.drop(how: :any).show

  # --- na.fill ----------------------------------------------------------------
  # Fill all string columns with a placeholder, then numeric columns via a map.
  puts "== na.fill('unknown', subset: ['name', 'dept']) =="
  df.na.fill("unknown", subset: %w[name dept]).show

  puts "== na.fill({ 'age' => 0, 'salary' => 0.0 }) =="
  df.na.fill({ "age" => 0, "salary" => 0.0 }).show

  # --- na.replace -------------------------------------------------------------
  # Replace the "eng" department label with "engineering".
  puts "== na.replace('eng', 'engineering', subset: ['dept']) =="
  df.na.replace("eng", "engineering", subset: ["dept"]).show

  # --- stat: build a clean numeric frame for the statistical helpers ----------
  nums = spark.create_data_frame([
    { "x" => 1.0, "y" => 2.0, "grp" => "a" },
    { "x" => 2.0, "y" => 4.1, "grp" => "a" },
    { "x" => 3.0, "y" => 6.2, "grp" => "b" },
    { "x" => 4.0, "y" => 7.9, "grp" => "b" },
    { "x" => 5.0, "y" => 10.0, "grp" => "a" },
  ])

  puts "== stat.corr('x', 'y') =="
  puts "Pearson correlation: #{nums.stat.corr('x', 'y')}"

  puts "== stat.approx_quantile('x', [0.25, 0.5, 0.75], 0.0) =="
  quartiles = nums.stat.approx_quantile("x", [0.25, 0.5, 0.75], 0.0)
  puts "Quartiles of x: #{quartiles.inspect}"

  puts "== stat.crosstab('grp', 'x') =="
  nums.stat.crosstab("grp", "x").show

  # --- describe / summary -----------------------------------------------------
  puts "== describe('x', 'y') =="
  nums.describe("x", "y").show

  puts "== summary (count, mean, min, 50%, max) =="
  nums.summary("count", "mean", "min", "50%", "max").show
ensure
  spark.stop
end
