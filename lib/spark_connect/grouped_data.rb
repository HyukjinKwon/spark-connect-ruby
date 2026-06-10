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

    # @param df [DataFrame]
    # @param grouping [Array<Column>] grouping columns.
    # @param group_type [Symbol] a `GROUP_TYPE_*` enum symbol.
    # @param pivot_col [Column, nil]
    # @param pivot_values [Array, nil]
    def initialize(df, grouping, group_type, pivot_col: nil, pivot_values: nil)
      @df = df
      @grouping = grouping
      @group_type = group_type
      @pivot_col = pivot_col
      @pivot_values = pivot_values
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

    # Pivot a column into multiple output columns.
    #
    # @param pivot_col [String, Column]
    # @param values [Array, nil] optional explicit pivot values (faster, deterministic).
    # @return [GroupedData]
    def pivot(pivot_col, values = nil)
      GroupedData.new(@df, @grouping, :GROUP_TYPE_PIVOT,
                      pivot_col: Column.to_col(pivot_col.is_a?(String) ? Functions.col(pivot_col) : pivot_col),
                      pivot_values: values)
    end

    private

    def numeric_agg(fn, cols)
      exprs = cols.flatten.map { |c| Column.invoke(fn, Functions.col(c.to_s)).to_expr }
      build(exprs)
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
      @df.build(aggregate: agg)
    end
  end
end
