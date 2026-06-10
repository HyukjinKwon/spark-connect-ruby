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

      keys = observed.keys.to_a
      decoded = observed.values.map { |lit| decode_literal(lit) }
      keys.zip(decoded).to_h
    end

    def decode_literal(literal)
      kind = literal.literal_type
      kind ? literal.public_send(kind) : nil
    end
  end
end
