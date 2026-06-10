#!/usr/bin/env ruby
# frozen_string_literal: true

# 05_window_functions.rb
#
# Demonstrates SQL window (analytic) functions over a WindowSpec built with
# SparkConnect::Window.partition_by(...).order_by(...):
#   - row_number : sequential row index within each partition
#   - rank       : ranking with gaps on ties
#   - dense_rank : ranking without gaps on ties
#   - lag / lead : value from the previous / next row in the ordered partition
#
# A window function Column is applied with Column#over(window_spec).
#
# Run with a live Spark Connect server:
#   export SPARK_REMOTE="sc://localhost:15002"
#   ruby examples/05_window_functions.rb

require "spark-connect"

F = SparkConnect::F
Window = SparkConnect::Window

spark = SparkConnect::SparkSession.builder
                                  .remote(ENV.fetch("SPARK_REMOTE", "sc://localhost:15002"))
                                  .app_name("05_window_functions")
                                  .get_or_create

# Sales rows partitioned by department, ordered by amount.
sales = spark.create_data_frame(
  [
    { "dept" => "eng",   "name" => "Alice", "amount" => 100 },
    { "dept" => "eng",   "name" => "Bob",   "amount" => 200 },
    { "dept" => "eng",   "name" => "Carol", "amount" => 200 },
    { "dept" => "eng",   "name" => "Dan",   "amount" => 300 },
    { "dept" => "sales", "name" => "Erin",  "amount" => 150 },
    { "dept" => "sales", "name" => "Frank", "amount" => 250 },
  ]
)

puts "input sales:"
sales.show

# Window: partition by department, order by amount descending so the biggest
# earner gets row_number 1 within each department.
w = Window.partition_by("dept").order_by(F.col("amount").desc)

puts "row_number / rank / dense_rank over (partition by dept order by amount desc):"
sales.with_column("row_number", F.row_number.over(w))
     .with_column("rank", F.rank.over(w))
     .with_column("dense_rank", F.dense_rank.over(w))
     .order_by("dept", F.col("amount").desc)
     .show

# Window for lag/lead: order ascending by amount within each department.
w_asc = Window.partition_by("dept").order_by("amount")

puts "lag / lead over (partition by dept order by amount asc):"
sales.with_column("prev_amount", F.lag("amount", 1).over(w_asc))
     .with_column("next_amount", F.lead("amount", 1).over(w_asc))
     .with_column("delta_from_prev", F.col("amount") - F.lag("amount", 1, 0).over(w_asc))
     .order_by("dept", "amount")
     .show

spark.stop
