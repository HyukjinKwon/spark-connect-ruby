#!/usr/bin/env ruby
# frozen_string_literal: true

# Declarative Pipelines (Spark 4.1+): build a dataflow graph of two chained
# materialized views and run it.
#
# Run with a reachable Spark Connect server (Spark 4.1+):
#   SPARK_REMOTE=sc://localhost:15002 ruby examples/11_pipeline.rb

require "spark-connect"
require "tmpdir"

F = SparkConnect::F

spark = SparkConnect::SparkSession.builder
                                  .remote(ENV.fetch("SPARK_REMOTE", "sc://localhost:15002"))
                                  .app_name("pipeline-example")
                                  .get_or_create

pipe = spark.pipeline
puts "dataflow graph: #{pipe.graph_id}"

# First materialized view: the even ids from a range.
src = spark.range(10).select(F.col("id"), (F.col("id") % 2).alias("parity"))
pipe.create_materialized_view("evens", src.filter(F.col("parity") == 0))

# Second materialized view reads the first (pipe.read references a graph dataset).
pipe.create_materialized_view(
  "evens_doubled", pipe.read("evens").select((F.col("id") * 2).alias("doubled"))
)

Dir.mktmpdir("scr_pipeline_storage") do |dir|
  puts "running pipeline..."
  pipe.start_run(storage: "file://#{dir}", full_refresh_all: true) do |event|
    puts "  #{event.message}"
  end
end

puts "evens:         #{spark.read.table('evens').orderBy('id').collect.map { |r| r['id'] }.inspect}"
puts "evens_doubled: #{spark.read.table('evens_doubled').orderBy('doubled').collect.map { |r| r['doubled'] }.inspect}"

spark.stop
