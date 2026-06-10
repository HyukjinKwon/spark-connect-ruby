#!/usr/bin/env ruby
# frozen_string_literal: true

# 07_read_write.rb
#
# Demonstrates the DataFrameReader / DataFrameWriter round-trip: build a small
# DataFrame from local data, write it out to parquet, json, and csv inside a
# temporary directory, then read each format back and show the contents.
#
# All output paths live under a Dir.mktmpdir block so nothing is left behind
# on disk after the script exits.
#
# Run with:
#   export PATH="/opt/homebrew/lib/ruby/gems/3.4.0/bin:$PATH"
#   ruby examples/07_read_write.rb
#
# The endpoint comes from SPARK_REMOTE (defaults to sc://localhost:15002).

require "spark-connect"
require "tmpdir"

spark = SparkConnect::SparkSession.builder
                                  .remote(ENV.fetch("SPARK_REMOTE", "sc://localhost:15002"))
                                  .app_name("07_read_write")
                                  .get_or_create

begin
  # Source data: an array of hashes. Schema is inferred from the first row.
  people = [
    { "name" => "Alice", "age" => 30, "city" => "NYC" },
    { "name" => "Bob",   "age" => 25, "city" => "LA" },
    { "name" => "Carol", "age" => 41, "city" => "SF" },
  ]
  df = spark.create_data_frame(people)

  puts "== Source DataFrame =="
  df.show

  Dir.mktmpdir("scr-read-write") do |dir|
    parquet_path = File.join(dir, "people_parquet")
    json_path    = File.join(dir, "people_json")
    csv_path     = File.join(dir, "people_csv")

    # --- Write the three formats ---------------------------------------------
    # Parquet is the simplest: just save with the parquet shortcut.
    df.write.mode(:overwrite).parquet(parquet_path)

    # JSON, written via the explicit format(...).save(...) chain.
    df.write.format("json").mode(:overwrite).save(json_path)

    # CSV with a header row so it reads back with named columns.
    df.write.mode(:overwrite).option("header", true).csv(csv_path)

    puts "\nWrote parquet -> #{parquet_path}"
    puts "Wrote json    -> #{json_path}"
    puts "Wrote csv     -> #{csv_path}"

    # --- Read each format back ------------------------------------------------
    puts "\n== Parquet read back =="
    spark.read.parquet(parquet_path).order_by("age").show

    puts "== JSON read back =="
    spark.read.json(json_path).order_by("age").show

    # CSV needs header + inferSchema so ages come back as integers, not strings.
    puts "== CSV read back =="
    spark.read
         .option("header", true)
         .option("inferSchema", true)
         .csv(csv_path)
         .order_by("age")
         .show
  end
ensure
  spark.stop
end
