# frozen_string_literal: true

module SparkConnect
  module ML
    # The `pyspark.ml.stat` statistics over Connect.
    #
    # Correlation, ChiSquareTest and KolmogorovSmirnovTest run on the server's
    # ML helper (the same calls PySpark's connect client makes), and return
    # lazy DataFrames. Summarizer builds an aggregate `Column` through Spark's
    # internal `aggregate_metrics` function.
    module Stat
      # Helper-object calls return DataFrames: build one from a helper method chain.
      # @api private
      def self.helper_relation(session, method, args)
        methods = [AttributeSupport.method_proto(method, args)]
        ml_rel = Proto::MlRelation.new(fetch: Proto::Fetch.new(
          obj_ref: Proto::ObjectRef.new(id: Rpc::HELPER_ID), methods: methods
        ))
        DataFrame.new(session, PlanBuilder.relation(session, ml_relation: ml_rel))
      end

      # Pearson or Spearman correlation matrix of a vector column.
      class Correlation
        class << self
          # @param dataset [DataFrame]
          # @param column [String] the vector column.
          # @param method [String] "pearson" or "spearman".
          # @return [DataFrame] a single row holding the correlation matrix.
          def corr(dataset, column, method = "pearson")
            Stat.helper_relation(dataset.session, "correlation", [dataset, column.to_s, method.to_s])
          end
        end
      end

      # Pearson's chi-squared independence test between a features column and a label column.
      class ChiSquareTest
        class << self
          # @param dataset [DataFrame]
          # @param features_col [String]
          # @param label_col [String]
          # @param flatten [Boolean] one row per statistic rather than one row in total.
          # @return [DataFrame] columns `pValues`, `degreesOfFreedom`, `statistics`.
          def test(dataset, features_col, label_col, flatten: false)
            Stat.helper_relation(dataset.session, "chiSquareTest", [dataset, features_col.to_s, label_col.to_s, flatten])
          end
        end
      end

      # One-sample two-sided Kolmogorov-Smirnov test.
      class KolmogorovSmirnovTest
        class << self
          # @param dataset [DataFrame]
          # @param sample_col [String]
          # @param dist_name [String] the reference distribution, e.g. "norm".
          # @param params [Array<Numeric>] the distribution parameters.
          # @return [DataFrame]
          def test(dataset, sample_col, dist_name, *params)
            Stat.helper_relation(dataset.session, "kolmogorovSmirnovTest",
                                 [dataset, sample_col.to_s, dist_name.to_s, params.map(&:to_f)])
          end
        end
      end

      # Column-level summary statistics (`Summarizer.mean(col)` and friends).
      class Summarizer
        METRICS = %w[mean sum variance std count numNonZeros max min normL1 normL2].freeze
        # Ruby names for the metrics: `Summarizer.num_non_zeros(col)` and so on.
        RUBY_NAMES = METRICS.to_h do |m|
          [m, m.gsub(/([a-z])([A-Z])/) do
            "#{Regexp.last_match(1)}_#{Regexp.last_match(2).downcase}"
          end.downcase,]
        end

        class << self
          # A builder that computes the named metrics of a vector column.
          # @param names [Array<String>] see {METRICS}.
          # @return [SummaryBuilder]
          def metrics(*names)
            unknown = names.map(&:to_s) - METRICS
            raise IllegalArgumentError, "unknown metrics #{unknown.join(', ')}" unless unknown.empty?

            SummaryBuilder.new(names.map(&:to_s))
          end

          # The value of one metric, as a Column.
          # @param name [String]
          # @param col [Column, String] the vector column.
          # @param weight [Column, nil]
          # @return [Column]
          def metric(name, col, weight = nil)
            metrics(name).summary(col, weight).get_field(name.to_s)
          end

          RUBY_NAMES.each do |metric_name, ruby_name|
            define_method(ruby_name) { |col, weight = nil| metric(metric_name, col, weight) }
          end
        end
      end

      # The metrics requested from a {Summarizer}.
      class SummaryBuilder
        # @return [Array<String>]
        attr_reader :metric_names

        # @param names [Array<String>]
        def initialize(names)
          @metric_names = names
        end

        # The struct column holding every requested metric, keyed by name.
        # @param features_col [Column, String]
        # @param weight_col [Column, nil]
        # @return [Column]
        def summary(features_col, weight_col = nil)
          names = Column.new(Proto::Expression.new(
                               unresolved_function: Proto::Expression::UnresolvedFunction.new(
                                 function_name: "array",
                                 arguments: metric_names.map { |m| Column.to_col(m).to_expr }
                               )
                             ))
          Functions.internal_call("aggregate_metrics", names, Functions.column_of(features_col),
                                  weight_col ? Functions.column_of(weight_col) : Column.to_col(1.0))
        end
      end
    end
  end
end
