#!/usr/bin/env ruby
# frozen_string_literal: true

# 08_local_data.rb
#
# Shows the several ways to build a DataFrame from local Ruby data with
# SparkSession#create_data_frame (aliased createDataFrame):
#   1. an array of hashes (schema inferred from keys)
#   2. an array of arrays + an explicit list of column names
#   3. an array of arrays + an explicit Types::StructType
#
# It then prints each schema with print_schema (printSchema) and materialises
# one DataFrame to an Apache Arrow table with to_arrow.
#
# Run with:
#   export PATH="/opt/homebrew/lib/ruby/gems/3.4.0/bin:$PATH"
#   ruby examples/08_local_data.rb
#
# The endpoint comes from SPARK_REMOTE (defaults to sc://localhost:15002).

require "spark-connect"

T = SparkConnect::Types

spark = SparkConnect::SparkSession.builder
                                  .remote(ENV.fetch("SPARK_REMOTE", "sc://localhost:15002"))
                                  .app_name("08_local_data")
                                  .get_or_create

begin
  # --- 1. Array of hashes: schema inferred from the hash keys -----------------
  hash_rows = [
    { "id" => 1, "name" => "Alice", "active" => true },
    { "id" => 2, "name" => "Bob",   "active" => false },
  ]
  df_hashes = spark.create_data_frame(hash_rows)
  puts "== From array of hashes =="
  df_hashes.printSchema
  df_hashes.show

  # --- 2. Array of arrays + explicit column names -----------------------------
  array_rows = [
    [1, "widget", 9.99],
    [2, "gadget", 19.5],
    [3, "gizmo",  4.25],
  ]
  df_named = spark.createDataFrame(array_rows, ["id", "product", "price"])
  puts "== From array of arrays + column names =="
  df_named.printSchema
  df_named.show

  # --- 3. Array of arrays + an explicit StructType ----------------------------
  # Build the schema by hand so the types are exactly what we want.
  schema = T::StructType.new([
    T::StructField.new("id", T::LongType.new, nullable: false),
    T::StructField.new("label", T::StringType.new),
    T::StructField.new("tags", T.array(T::StringType.new)),
  ])
  struct_rows = [
    [10, "first",  ["a", "b"]],
    [20, "second", ["c"]],
  ]
  df_typed = spark.create_data_frame(struct_rows, schema)
  puts "== From array of arrays + explicit StructType =="
  df_typed.printSchema
  df_typed.show

  # --- to_arrow: columnar materialisation -------------------------------------
  # to_arrow returns a red-arrow Arrow::Table; print its shape and column names.
  table = df_named.to_arrow
  puts "== to_arrow on the named DataFrame =="
  puts "Arrow table: #{table.n_rows} rows x #{table.n_columns} columns"
  puts "Columns: #{table.schema.fields.map(&:name).join(', ')}"
ensure
  spark.stop
end
