# frozen_string_literal: true

require_relative "ml/params"
require_relative "ml/linalg"
require_relative "ml/serialize"
require_relative "ml/rpc"
require_relative "ml/base"
require_relative "ml/persistence"
require_relative "ml/pipeline"
require_relative "ml/tuning"
require_relative "ml/stat"
require_relative "ml/functions"
require_relative "ml/classification"
require_relative "ml/regression"
require_relative "ml/clustering"
require_relative "ml/recommendation"
require_relative "ml/fpm"
require_relative "ml/feature"
require_relative "ml/feature_transformers"
require_relative "ml/evaluation"

module SparkConnect
  # Spark ML over Spark Connect, mirroring `pyspark.ml`. The operator classes
  # live in the family modules {SparkConnect::ML::Classification},
  # {SparkConnect::ML::Regression}, {SparkConnect::ML::Clustering},
  # {SparkConnect::ML::Recommendation}, {SparkConnect::ML::FPM},
  # {SparkConnect::ML::Feature} and {SparkConnect::ML::Evaluation}.
  module ML
    Proto = SparkConnect::Proto

    # The session to run ML commands on: the explicit one, else `fallback`
    # (a model's own session), else the active session.
    # @api private
    # @raise [IllegalArgumentError] when no session is available
    def self.session_or_active(session, fallback = nil)
      session || fallback || SparkSession.active ||
        raise(IllegalArgumentError, "no active SparkSession; pass session:")
    end

    # Evict every ML object of the session from the server cache (Spark 4.1+).
    # @param session [SparkSession, nil] defaults to the active session.
    # @return [Integer] the number of objects removed.
    def self.clean_cache(session = nil)
      Rpc.clean_cache(session_or_active(session))
    end

    # The server's ML cache information (Spark 4.1+).
    # @param session [SparkSession, nil] defaults to the active session.
    # @return [Object]
    def self.cache_info(session = nil)
      Rpc.cache_info(session_or_active(session))
    end
  end
end
