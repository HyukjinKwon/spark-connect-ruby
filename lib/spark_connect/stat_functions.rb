# frozen_string_literal: true

module SparkConnect
  # Statistical helpers, returned by {DataFrame#stat}. Mirrors PySpark's
  # `DataFrame.stat` (`DataFrameStatFunctions`).
  #
  # @example
  #   df.stat.corr("x", "y")
  #   df.stat.approx_quantile("x", [0.25, 0.5, 0.75], 0.01)
  #   df.stat.crosstab("a", "b").show
  class DataFrameStatFunctions
    Proto = SparkConnect::Proto

    # @param df [DataFrame]
    def initialize(df)
      @df = df
    end

    # Sample covariance of two columns.
    # @return [Float]
    def cov(col1, col2)
      scalar(@df.build(cov: Proto::StatCov.new(input: @df.relation, col1: col1.to_s, col2: col2.to_s)))
    end

    # Correlation of two columns (`method` is `"pearson"`).
    # @return [Float]
    def corr(col1, col2, method = "pearson")
      rel = Proto::StatCorr.new(input: @df.relation, col1: col1.to_s, col2: col2.to_s, method: method)
      scalar(@df.build(corr: rel))
    end

    # Contingency table (cross-tabulation) of two columns.
    # @return [DataFrame]
    def crosstab(col1, col2)
      @df.build(crosstab: Proto::StatCrosstab.new(input: @df.relation, col1: col1.to_s, col2: col2.to_s))
    end

    # Frequent items in the given columns.
    #
    # @param cols [Array<String>]
    # @param support [Float]
    # @return [DataFrame]
    def freq_items(cols, support = 0.01)
      rel = Proto::StatFreqItems.new(input: @df.relation, cols: Array(cols).map(&:to_s), support: support)
      @df.build(freq_items: rel)
    end

    # Approximate quantiles of numeric columns.
    #
    # @param cols [String, Array<String>]
    # @param probabilities [Array<Float>] values in 0.0..1.0.
    # @param relative_error [Float]
    # @return [Array<Float>, Array<Array<Float>>] one list per column.
    def approx_quantile(cols, probabilities, relative_error)
      single = !cols.is_a?(Array)
      rel = Proto::StatApproxQuantile.new(
        input: @df.relation, cols: Array(cols).map(&:to_s),
        probabilities: probabilities, relative_error: relative_error
      )
      row = @df.build(approx_quantile: rel).collect.first
      result = row.to_a
      single ? result.first : result
    end

    # Stratified sample without replacement, keyed by `col`.
    #
    # @param col [String, Column]
    # @param fractions [Hash{Object=>Float}] per-stratum sampling fraction.
    # @param seed [Integer, nil]
    # @return [DataFrame]
    def sample_by(col, fractions, seed = nil)
      col_expr = (col.is_a?(Column) ? col : Functions.col(col.to_s)).to_expr
      frac = fractions.map do |stratum, fraction|
        Proto::StatSampleBy::Fraction.new(stratum: Column.to_literal(stratum), fraction: fraction)
      end
      rel = Proto::StatSampleBy.new(input: @df.relation, col: col_expr, fractions: frac)
      rel.seed = seed if seed
      @df.build(sample_by: rel)
    end

    private

    def scalar(df)
      row = df.collect.first
      row&.[](0)
    end
  end

  # Reopen {DataFrame} to add the `describe`/`summary` actions, which are
  # naturally statistical and share the Stat* relations.
  class DataFrame
    # Basic descriptive statistics (count, mean, stddev, min, max) per column.
    #
    # @param cols [Array<String>] columns to describe (all when empty).
    # @return [DataFrame]
    def describe(*cols)
      build(describe: Proto::StatDescribe.new(input: @relation, cols: cols.flatten.map(&:to_s)))
    end

    # Configurable summary statistics.
    #
    # @param statistics [Array<String>] e.g. `"count"`, `"mean"`, `"stddev"`,
    #   `"min"`, `"25%"`, `"50%"`, `"75%"`, `"max"`.
    # @return [DataFrame]
    def summary(*statistics)
      build(summary: Proto::StatSummary.new(input: @relation, statistics: statistics.flatten.map(&:to_s)))
    end
  end
end
