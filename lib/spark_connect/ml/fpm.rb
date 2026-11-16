# frozen_string_literal: true

module SparkConnect
  module ML
    # Frequent pattern mining (`org.apache.spark.ml.fpm`).
    module FPM
      # FP-growth frequent itemset mining and association rules.
      class FPGrowth < Estimator
        operator "org.apache.spark.ml.fpm.FPGrowth"
        model -> { FPGrowthModel }

        param :min_support, :double, default: 0.3, doc: "minimal support level in [0, 1]"
        param :min_confidence, :double, default: 0.8, doc: "minimal confidence for generating association rules in [0, 1]"
        param :items_col, :string, default: "items", doc: "items column name"
        param :num_partitions, :int, doc: "number of partitions used by parallel FP-growth (>= 1)"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
      end

      # A fitted {FPGrowth} model: the frequent itemsets and association rules.
      class FPGrowthModel < Model
        operator "org.apache.spark.ml.fpm.FPGrowthModel"

        param :min_support, :double, default: 0.3, doc: "minimal support level in [0, 1]"
        param :min_confidence, :double, default: 0.8, doc: "minimal confidence for generating association rules in [0, 1]"
        param :items_col, :string, default: "items", doc: "items column name"
        param :num_partitions, :int, doc: "number of partitions used by parallel FP-growth (>= 1)"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"

        attribute :freq_itemsets, :dataframe, spark_name: "freqItemsets"
        attribute :association_rules, :dataframe, spark_name: "associationRules"
      end

      # Sequential pattern mining. Not a Spark ML estimator: PySpark runs it over Connect through the
      # ML connect helper, and so does this class.
      class PrefixSpan < Params
        operator "org.apache.spark.ml.fpm.PrefixSpan"

        param :min_support, :double, default: 0.1, doc: "minimal support level in [0, 1]"
        param :max_pattern_length, :int, default: 10, doc: "maximal length of the sequential patterns (> 0)"
        param :max_local_proj_db_size, :long, default: 32_000_000,
                                              doc: "limit on the size of the local projected database"
        param :sequence_col, :string, default: "sequence", doc: "name of the sequence column"

        # Find the frequent sequential patterns of the sequences in `dataset`.
        #
        # @param dataset [DataFrame] a column of sequences (`array<array<T>>`)
        # @return [DataFrame] the columns `sequence` and `freq`
        def find_frequent_sequential_patterns(dataset)
          session = dataset.session
          args = [dataset, min_support, max_pattern_length, max_local_proj_db_size, sequence_col]
          methods = [AttributeSupport.method_proto("prefixSpanFindFrequentSequentialPatterns", args)]
          fetch = Proto::Fetch.new(obj_ref: Proto::ObjectRef.new(id: Rpc::HELPER_ID), methods: methods)
          DataFrame.new(session, PlanBuilder.relation(session, ml_relation: Proto::MlRelation.new(fetch: fetch)))
        end
      end
    end
  end
end
