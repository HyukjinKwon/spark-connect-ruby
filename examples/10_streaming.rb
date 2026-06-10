#!/usr/bin/env ruby
# frozen_string_literal: true

# Structured Streaming: read from the built-in `rate` source, run a streaming
# aggregation into the in-memory sink, inspect progress, then stop.
#
# Run with a reachable Spark Connect server:
#   SPARK_REMOTE=sc://localhost:15002 ruby examples/10_streaming.rb

require "spark-connect"

F = SparkConnect::F

spark = SparkConnect::SparkSession.builder
                                  .remote(ENV.fetch("SPARK_REMOTE", "sc://localhost:15002"))
                                  .app_name("streaming-example")
                                  .get_or_create

# A streaming DataFrame from the `rate` source (one row per tick).
stream = spark.read_stream.format("rate").option("rowsPerSecond", 10).load
puts "streaming? #{stream.streaming?}"
puts "schema:    #{stream.schema.simple_string}"

# Running count of rows per 2-second event-time window, into the memory sink.
windowed = stream
           .with_watermark("timestamp", "10 seconds")
           .group_by(F.window(F.col("timestamp"), "2 seconds"))
           .agg(F.count(F.lit(1)).alias("n"))

query = windowed.write_stream
                .format("memory")
                .query_name("rate_counts")
                .output_mode("update")
                .trigger(processing_time: "1 second")
                .start

puts "started query id=#{query.id} active=#{query.active?}"
sleep 4

puts "status: #{query.status}"
puts "active queries: #{spark.streams.active.map(&:id)}"
spark.sql("SELECT * FROM rate_counts ORDER BY window").show(truncate: false)

query.stop
puts "stopped; active=#{query.active?}"

spark.stop
