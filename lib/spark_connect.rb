# frozen_string_literal: true

require "securerandom"

require_relative "spark_connect/version"
require_relative "spark_connect/proto"
require_relative "spark_connect/errors"
require_relative "spark_connect/types"
require_relative "spark_connect/row"
require_relative "spark_connect/plan"
require_relative "spark_connect/column"
require_relative "spark_connect/window"
require_relative "spark_connect/functions"
require_relative "spark_connect/arrow"
require_relative "spark_connect/channel_builder"
require_relative "spark_connect/client"
require_relative "spark_connect/conf"
require_relative "spark_connect/observation"
require_relative "spark_connect/grouped_data"
require_relative "spark_connect/na_functions"
require_relative "spark_connect/stat_functions"
require_relative "spark_connect/reader"
require_relative "spark_connect/writer"
require_relative "spark_connect/streaming"
require_relative "spark_connect/catalog"
require_relative "spark_connect/data_frame"
require_relative "spark_connect/session"

# spark-connect is a pure-Ruby client for {https://spark.apache.org/docs/latest/spark-connect-overview.html
# Apache Spark Connect}, the gRPC-based decoupled client-server protocol for
# Apache Spark.
#
# The public surface mirrors PySpark closely: a {SparkConnect::SparkSession}
# is the entry point, {SparkConnect::DataFrame} is the lazy, immutable relation
# builder, {SparkConnect::Column} represents column expressions, and
# {SparkConnect::Functions} (aliased as {SparkConnect::F}) provides the standard
# function library.
#
# @example Connect and run a query
#   require "spark-connect"
#
#   spark = SparkConnect::SparkSession.builder
#                                     .remote("sc://localhost:15002")
#                                     .get_or_create
#   df = spark.range(10).select(SparkConnect::F.col("id") * 2)
#   df.show
#   spark.stop
module SparkConnect
  class << self
    # Convenience shortcut for {SparkConnect::SparkSession.builder}.
    #
    # @return [SparkConnect::SparkSession::Builder]
    def builder
      SparkSession.builder
    end
  end
end
