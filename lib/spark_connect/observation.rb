# frozen_string_literal: true

module SparkConnect
  # Captures named aggregate metrics computed while a {DataFrame} is being
  # materialised, without an extra pass over the data. Pair with
  # {DataFrame#observe}.
  #
  # @example
  #   obs = SparkConnect::Observation.new("metrics")
  #   df.observe(obs, F.count(F.lit(1)).alias("rows"), F.max("id").alias("max_id")).collect
  #   obs.get  #=> {"rows"=>100, "max_id"=>99}
  class Observation
    # @return [String] the observation name.
    attr_reader :name

    @counter = 0
    class << self
      # @api private
      attr_accessor :counter
    end

    # @param name [String, nil] a unique name (auto-generated when omitted).
    def initialize(name = nil)
      Observation.counter += 1
      @name = name || "observation_#{Observation.counter}"
      @df = nil
    end

    # @api private - bind the observed DataFrame so {#get} can fetch metrics.
    def bind(df)
      @df = df
      self
    end

    # The observed metric values (forces execution if not yet materialised).
    #
    # @return [Hash{String=>Object}]
    def get
      raise IllegalArgumentError, "Observation has not been attached to a DataFrame yet" unless @df

      @metrics ||= fetch_metrics
    end

    private

    def fetch_metrics
      result = @df.session.client.execute_plan(@df.relation)
      observed = result.observed_metrics.find { |m| m.name == @name } || result.observed_metrics.first
      return {} unless observed
      # Spark 4.2+ reports a failure to compute the metrics (e.g. an error in a
      # metric expression) in the metrics message instead of failing the query.
      raise Observation.error_from(observed) if observed.has_root_error_idx?

      keys = observed.keys.to_a
      keys = Array.new(observed.values.size) { |i| "observed_metric_#{i}" } if keys.empty?
      keys.zip(observed.values.map { |lit| Column.literal_value(lit) }).to_h
    end

    class << self
      # Convert the error payload of an observed-metrics message into the
      # matching {SparkConnectError} subclass.
      # @api private
      def error_from(observed)
        errors = observed.errors.to_a
        idx = observed.root_error_idx
        return SparkConnectError.new("Observation error: invalid root_error_idx #{idx}") unless idx.between?(0, errors.size - 1)

        root = errors[idx]
        hierarchy = root.error_type_hierarchy.to_a
        klass =
          if hierarchy.any? { |c| c.end_with?("ParseException") } then ParseError
          elsif hierarchy.any? { |c| c.end_with?("AnalysisException") } then AnalysisError
          else SparkConnectError
          end
        throwable = root.spark_throwable
        klass.new(root.message,
                  error_class: throwable&.has_error_class? ? throwable.error_class : nil,
                  sql_state: throwable&.has_sql_state? ? throwable.sql_state : nil)
      end
    end
  end
end
