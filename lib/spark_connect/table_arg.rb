# frozen_string_literal: true

module SparkConnect
  # A {DataFrame} passed as a `TABLE` argument to a table-valued function,
  # created with {DataFrame#as_table}. It is a {Column} (a table-argument
  # subquery expression), so it can be passed wherever a function argument is
  # accepted, e.g. `F.call_function("my_tvf", df.as_table.partition_by("k"))`.
  #
  # Mirrors PySpark's `TableArg`: optionally partition the rows
  # ({#partition_by} or {#with_single_partition}) and then order them within
  # each partition ({#order_by}).
  class TableArg < Column
    Proto = SparkConnect::Proto

    # @param df [DataFrame]
    # @api private
    def initialize(df, partition_spec: [], order_spec: [], with_single_partition: nil)
      @df = df
      @partition_spec = partition_spec
      @order_spec = order_spec
      @with_single_partition = with_single_partition
      options = nil
      unless partition_spec.empty? && order_spec.empty? && with_single_partition.nil?
        options = Proto::SubqueryExpression::TableArgOptions.new(partition_spec: partition_spec, order_spec: order_spec)
        options.with_single_partition = with_single_partition unless with_single_partition.nil?
      end
      super(df.subquery_expression(:SUBQUERY_TYPE_TABLE_ARG, table_arg_options: options).to_expr)
    end

    # Partition the table argument's rows by these columns.
    # @param cols [Array<Column, String>]
    # @return [TableArg]
    def partition_by(*cols)
      raise IllegalArgumentError, "Cannot call partition_by after partition_by or with_single_partition" if partitioned?

      exprs = cols.flatten.map { |c| Functions._col(c).to_expr }
      TableArg.new(@df, partition_spec: @partition_spec + exprs, order_spec: @order_spec,
                        with_single_partition: @with_single_partition)
    end
    alias partitionBy partition_by

    # Order the rows within each partition (after {#partition_by} or
    # {#with_single_partition}).
    # @param cols [Array<Column, String>]
    # @return [TableArg]
    def order_by(*cols)
      raise IllegalArgumentError, "Call partition_by or with_single_partition before order_by" unless partitioned?

      orders = cols.flatten.map do |c|
        expr = Functions._col(c).to_expr
        expr.expr_type == :sort_order ? expr.sort_order : Functions._col(c).asc.to_expr.sort_order
      end
      TableArg.new(@df, partition_spec: @partition_spec, order_spec: @order_spec + orders,
                        with_single_partition: @with_single_partition)
    end
    alias orderBy order_by

    # Pass all rows to a single partition.
    # @return [TableArg]
    def with_single_partition
      raise IllegalArgumentError, "Cannot call with_single_partition after partition_by or with_single_partition" if partitioned?

      TableArg.new(@df, partition_spec: @partition_spec, order_spec: @order_spec, with_single_partition: true)
    end
    alias withSinglePartition with_single_partition

    private

    def partitioned?
      !@partition_spec.empty? || @with_single_partition == true
    end
  end
end
