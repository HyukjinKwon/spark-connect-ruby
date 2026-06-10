#!/usr/bin/env ruby
# frozen_string_literal: true

# 01_quickstart.rb
# -----------------------------------------------------------------------------
# The "hello world" of spark-connect: connect to a Spark Connect server, create
# a simple DataFrame with range, project a couple of columns, then display and
# materialize the results.
#
# What it shows:
#   - Building a SparkSession from a remote endpoint
#   - spark.range to generate rows
#   - select with a computed column (SparkConnect::F)
#   - show (pretty-print) and collect (pull Rows to the client)
#   - spark.stop to release the session
#
# How to run:
#   export PATH="/opt/homebrew/lib/ruby/gems/3.4.0/bin:$PATH"
#   export SPARK_REMOTE="sc://localhost:15002"   # optional, this is the default
#   ruby examples/01_quickstart.rb
# -----------------------------------------------------------------------------

require "spark-connect"

# Short, idiomatic alias for the functions module (mirrors PySpark's `F`).
F = SparkConnect::F

# Read the endpoint from the environment, falling back to the local server.
remote = ENV.fetch("SPARK_REMOTE", "sc://localhost:15002")

# Build (or reuse) a session pointed at the Spark Connect server.
spark = SparkConnect::SparkSession.builder
                                  .remote(remote)
                                  .app_name("01_quickstart")
                                  .get_or_create

puts "Connected to #{remote} (Spark #{spark.version})"

# Generate the numbers 0..9 as a one-column ("id") DataFrame.
df = spark.range(10)

# Project the original id alongside a derived column. Column arithmetic builds
# an expression tree that is evaluated server-side.
projected = df.select(
  F.col("id"),
  (F.col("id") * 10).alias("times_ten")
)

# Pretty-print the first rows in an ASCII table (like PySpark's DataFrame.show).
puts "\nprojected.show:"
projected.show

# Pull all rows back to the client as an Array of SparkConnect::Row objects.
rows = projected.collect
puts "\nCollected #{rows.size} rows:"
rows.each do |row|
  # Rows behave like ordered records: access fields by name.
  puts "  id=#{row['id']} times_ten=#{row['times_ten']}"
end

# Always release server-side resources when you are done.
spark.stop
puts "\nDone."
