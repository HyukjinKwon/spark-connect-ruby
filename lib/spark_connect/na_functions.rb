# frozen_string_literal: true

module SparkConnect
  # Missing-data helpers, returned by {DataFrame#na}. Mirrors PySpark's
  # `DataFrame.na` (`DataFrameNaFunctions`).
  #
  # @example
  #   df.na.drop(how: :any)
  #   df.na.fill(0)
  #   df.na.fill({ "name" => "unknown", "age" => 0 })
  #   df.na.replace("UNKNOWN", nil, subset: ["name"])
  class DataFrameNaFunctions
    Proto = SparkConnect::Proto

    # @param df [DataFrame]
    def initialize(df)
      @df = df
    end

    # Drop rows containing null values.
    #
    # @param how [Symbol] `:any` (drop if any field is null) or `:all`.
    # @param thresh [Integer, nil] keep rows with at least this many non-null
    #   values (overrides `how` when given).
    # @param subset [Array<String>, nil] only consider these columns.
    # @return [DataFrame]
    def drop(how: :any, thresh: nil, subset: nil)
      cols = Array(subset).map(&:to_s)
      min_non_nulls = thresh || (if how.to_sym == :all
                                   1
                                 else
                                   (cols.empty? ? nil : cols.size)
                                 end)
      nd = Proto::NADrop.new(input: @df.relation, cols: cols)
      nd.min_non_nulls = min_non_nulls if min_non_nulls
      @df.build(drop_na: nd)
    end

    # Replace null values.
    #
    # @overload fill(value, subset: nil)
    #   @param value [Object] a scalar used to fill all (or `subset`) columns.
    # @overload fill(value_map)
    #   @param value_map [Hash{String=>Object}] per-column fill values.
    # @return [DataFrame]
    def fill(value, subset: nil)
      cols, values =
        if value.is_a?(Hash)
          [value.keys.map(&:to_s), value.values]
        else
          [Array(subset).map(&:to_s), Array(subset).empty? ? [value] : Array(subset).map { value }]
        end
      nf = Proto::NAFill.new(
        input: @df.relation, cols: cols, values: values.map { |v| na_literal(v) }
      )
      @df.build(fill_na: nf)
    end

    # Replace specific values with others.
    #
    # @param to_replace [Object, Array, Hash] value(s) to replace, or a
    #   `{old => new}` mapping.
    # @param value [Object, Array, nil] replacement value(s) when `to_replace`
    #   is not a Hash.
    # @param subset [Array<String>, nil]
    # @return [DataFrame]
    def replace(to_replace, value = nil, subset: nil)
      mapping =
        if to_replace.is_a?(Hash)
          to_replace
        else
          Array(to_replace).zip(Array(value)).to_h
        end
      replacements = mapping.map do |old, new_value|
        Proto::NAReplace::Replacement.new(
          old_value: na_literal(old), new_value: na_literal(new_value)
        )
      end
      nr = Proto::NAReplace.new(
        input: @df.relation, cols: Array(subset).map(&:to_s), replacements: replacements
      )
      @df.build(replace: nr)
    end

    private

    # Spark's fill/replace handlers only accept Long, Double, String, or Boolean
    # literal values (not 32-bit Int), so widen Ruby Integers to Long and
    # Floats to Double.
    def na_literal(value)
      case value
      when Integer then Proto::Expression::Literal.new(long: value)
      when Float then Proto::Expression::Literal.new(double: value)
      else Column.to_literal(value)
      end
    end
  end
end
