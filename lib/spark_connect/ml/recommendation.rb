# frozen_string_literal: true

module SparkConnect
  module ML
    # Recommendation operators (`org.apache.spark.ml.recommendation`).
    module Recommendation
      # Alternating least squares matrix factorization.
      class ALS < Estimator
        operator "org.apache.spark.ml.recommendation.ALS"
        model -> { ALSModel }

        param :rank, :int, default: 10, doc: "rank of the factorization (>= 1)"
        param :max_iter, :int, default: 10, doc: "maximum number of iterations (>= 0)"
        param :reg_param, :double, default: 0.1, doc: "regularization parameter (>= 0)"
        param :num_user_blocks, :int, default: 10, doc: "number of user blocks (>= 1)"
        param :num_item_blocks, :int, default: 10, doc: "number of item blocks (>= 1)"
        param :implicit_prefs, :boolean, default: false, doc: "whether to use implicit preference"
        param :alpha, :double, default: 1.0, doc: "alpha for implicit preference (>= 0)"
        param :user_col, :string, default: "user", doc: "column name for user ids"
        param :item_col, :string, default: "item", doc: "column name for item ids"
        param :seed, :long, default: 1_994_790_107, doc: "random seed"
        param :rating_col, :string, default: "rating", doc: "column name for ratings"
        param :nonnegative, :boolean, default: false, doc: "whether to use nonnegative constraint for least squares"
        param :checkpoint_interval, :int, default: 10,
                                          doc: "checkpoint interval (>= 1), or -1 to disable checkpointing"
        param :intermediate_storage_level, :string, default: "MEMORY_AND_DISK",
                                                    doc: "StorageLevel for intermediate datasets"
        param :final_storage_level, :string, default: "MEMORY_AND_DISK", doc: "StorageLevel for ALS model factors"
        param :cold_start_strategy, :string, default: "nan", doc: "strategy for unknown users or items: nan or drop"
        param :block_size, :int, default: 4096, doc: "block size for stacking input data in matrices (> 0)"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
      end

      # A fitted {ALS} model: the factors and the recommendation methods.
      class ALSModel < Model
        operator "org.apache.spark.ml.recommendation.ALSModel"

        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
        param :block_size, :int, default: 4096, doc: "block size for stacking input data in matrices (> 0)"
        param :user_col, :string, default: "user", doc: "column name for user ids"
        param :item_col, :string, default: "item", doc: "column name for item ids"
        param :cold_start_strategy, :string, default: "nan", doc: "strategy for unknown users or items: nan or drop"

        attribute :rank, :int
        attribute :item_factors, :dataframe, spark_name: "itemFactors"
        attribute :user_factors, :dataframe, spark_name: "userFactors"
        method_call :recommend_for_all_users, arg_count: 1, returns: :dataframe, spark_name: "recommendForAllUsers"
        method_call :recommend_for_all_items, arg_count: 1, returns: :dataframe, spark_name: "recommendForAllItems"
        method_call :recommend_for_user_subset, arg_count: 2, returns: :dataframe,
                                                spark_name: "recommendForUserSubset"
        method_call :recommend_for_item_subset, arg_count: 2, returns: :dataframe,
                                                spark_name: "recommendForItemSubset"
      end
    end
  end
end
