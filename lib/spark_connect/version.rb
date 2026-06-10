# frozen_string_literal: true

module SparkConnect
  # The released version of the spark-connect gem.
  VERSION = "0.1.0"

  # The Apache Spark version whose Spark Connect protocol definitions this
  # client is generated against. The client aims to be wire-compatible with
  # Spark Connect servers of this major/minor line and newer.
  SPARK_VERSION = "4.0.0"
end
