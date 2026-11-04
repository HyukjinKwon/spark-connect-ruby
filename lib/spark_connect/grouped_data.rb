# frozen_string_literal: true

module SparkConnect
  # The result of {DataFrame#group_by} / {DataFrame#rollup} / {DataFrame#cube}.
  # Call an aggregate ({#agg}, {#count}, {#sum}, {#avg}, {#max}, {#min}, ...) to
  # produce a new {DataFrame}, optionally after {#pivot}.
  #
  # @example
  #   df.group_by("dept").agg(F.avg("salary").alias("avg_salary"), F.count("*"))
  #   df.group_by("dept").pivot("year").sum("revenue")
  class GroupedData
    Proto = SparkConnect::Proto

    NUMERIC_TYPES = [
      Types::ByteType, Types::ShortType, Types::IntegerType, Types::LongType,
      Types::FloatType, Types::DoubleType, Types::DecimalType,
    ].freeze

    # @param df [DataFrame]
    # @param grouping [Array<Column>] grouping columns.
    # @param group_type [Symbol] a `GROUP_TYPE_*` enum symbol.
    # @param pivot_col [Column, nil]
    # @param pivot_values [Array, nil]
    # @param grouping_sets [Array<Array<Column>>, nil] for `GROUP_TYPE_GROUPING_SETS`.
    def initialize(df, grouping, group_type, pivot_col: nil, pivot_values: nil, grouping_sets: nil)
      @df = df
      @grouping = grouping
      @group_type = group_type
      @pivot_col = pivot_col
      @pivot_values = pivot_values
      @grouping_sets = grouping_sets
    end

    # Compute aggregate expressions.
    #
    # @overload agg(*columns)
    #   @param columns [Array<Column>] aggregate columns, e.g. `F.sum("x")`.
    # @overload agg(hash)
    #   @param hash [Hash{String=>String}] column-to-function map, e.g.
    #     `{"age" => "max", "salary" => "avg"}`.
    # @return [DataFrame]
    def agg(*exprs)
      raise IllegalArgumentError, "agg requires at least one expression" if exprs.empty?

      agg_exprs =
        if exprs.size == 1 && exprs.first.is_a?(Hash)
          exprs.first.map { |col, fn| Column.invoke(fn.to_s, Functions.col(col.to_s)).to_expr }
        else
          exprs.flatten.map { |c| Column.to_col(c).to_expr }
        end
      build(agg_exprs)
    end

    # Count rows per group.
    # @return [DataFrame]
    def count
      build([Column.invoke("count", Column.lit(1)).alias("count").to_expr])
    end

    # Sum of each numeric column (or all numeric columns when none given).
    # @return [DataFrame]
    def sum(*cols) = numeric_agg("sum", cols)

    # Mean of each numeric column.
    # @return [DataFrame]
    def avg(*cols) = numeric_agg("avg", cols)
    alias mean avg

    # Maximum of each column.
    # @return [DataFrame]
    def max(*cols) = numeric_agg("max", cols)

    # Minimum of each column.
    # @return [DataFrame]
    def min(*cols) = numeric_agg("min", cols)

    # Pivot a column into multiple output columns. Only valid right after
    # {DataFrame#group_by}.
    #
    # @param pivot_col [String, Column]
    # @param values [Array, nil] optional explicit pivot values (faster, deterministic).
    # @return [GroupedData]
    def pivot(pivot_col, values = nil)
      unless @group_type == :GROUP_TYPE_GROUPBY
        what = @group_type == :GROUP_TYPE_PIVOT ? "Repeated PIVOT operation" : "PIVOT after #{group_type_name}"
        raise NotImplementedError, "#{what} is not supported"
      end
      raise IllegalArgumentError, "pivot values must be an Array, got #{values.class}" unless values.nil? || values.is_a?(Array)

      col = pivot_col.is_a?(Column) ? pivot_col : @df.col(pivot_col.to_s)
      GroupedData.new(@df, @grouping, :GROUP_TYPE_PIVOT, pivot_col: col, pivot_values: values)
    end

    private

    def group_type_name
      @group_type.to_s.delete_prefix("GROUP_TYPE_")
    end

    # Like PySpark, aggregate every numeric column when none is named.
    def numeric_agg(fn, cols)
      names = cols.flatten.map(&:to_s)
      names = @df.schema.fields.select { |f| NUMERIC_TYPES.any? { |t| f.data_type.is_a?(t) } }.map(&:name) if names.empty?
      build(names.map { |c| Column.invoke(fn, Functions.col(c)).to_expr })
    end

    def build(agg_exprs)
      agg = Proto::Aggregate.new(
        input: @df.relation,
        group_type: @group_type,
        grouping_expressions: @grouping.map(&:to_expr),
        aggregate_expressions: agg_exprs
      )
      if @group_type == :GROUP_TYPE_PIVOT
        pivot = Proto::Aggregate::Pivot.new(col: @pivot_col.to_expr)
        pivot.values += @pivot_values.map { |v| Column.lit(v).to_expr.literal } if @pivot_values
        agg.pivot = pivot
      end
      if @group_type == :GROUP_TYPE_GROUPING_SETS
        agg.grouping_sets += (@grouping_sets || []).map do |set|
          Proto::Aggregate::GroupingSets.new(grouping_set: set.map(&:to_expr))
        end
      end
      @df.build(aggregate: agg)
    end
  end
end
