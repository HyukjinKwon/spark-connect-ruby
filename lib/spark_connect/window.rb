# frozen_string_literal: true

module SparkConnect
  # Defines the partitioning, ordering, and frame for a window aggregation.
  # Build one with the {Window} factory and attach it to an analytic column via
  # {Column#over}.
  #
  # @example
  #   w = SparkConnect::Window.partition_by("dept").order_by(F.col("salary").desc)
  #   df.with_column("rank", F.rank.over(w))
  class WindowSpec
    Proto = SparkConnect::Proto

    # Boundary sentinels (matching Spark's `Window.unboundedPreceding`, etc.).
    UNBOUNDED_PRECEDING = -(2**63)
    UNBOUNDED_FOLLOWING = (2**63) - 1
    CURRENT_ROW = 0

    # @return [Array<Spark::Connect::Expression>]
    attr_reader :partition_spec
    # @return [Array<Spark::Connect::Expression::SortOrder>]
    attr_reader :order_spec
    # @return [Spark::Connect::Expression::Window::WindowFrame, nil]
    attr_reader :frame_spec

    def initialize(partition_spec: [], order_spec: [], frame_spec: nil)
      @partition_spec = partition_spec
      @order_spec = order_spec
      @frame_spec = frame_spec
    end

    # @return [WindowSpec] a copy partitioned by the given columns.
    def partition_by(*cols)
      copy(partition_spec: to_exprs(cols))
    end

    # @return [WindowSpec] a copy ordered by the given columns.
    def order_by(*cols)
      copy(order_spec: to_sort_orders(cols))
    end

    # Row-based frame between `start` and `end` (offsets relative to the current row).
    # @return [WindowSpec]
    def rows_between(start_, end_)
      copy(frame_spec: frame(:FRAME_TYPE_ROW, start_, end_))
    end

    # Range-based frame between `start` and `end` (value offsets over the ordering).
    # @return [WindowSpec]
    def range_between(start_, end_)
      copy(frame_spec: frame(:FRAME_TYPE_RANGE, start_, end_))
    end

    private

    def copy(partition_spec: @partition_spec, order_spec: @order_spec, frame_spec: @frame_spec)
      WindowSpec.new(partition_spec: partition_spec, order_spec: order_spec, frame_spec: frame_spec)
    end

    def to_exprs(cols)
      cols.flatten.map { |c| (c.is_a?(Column) ? c : Functions.col(c.to_s)).to_expr }
    end

    def to_sort_orders(cols)
      cols.flatten.map do |c|
        col = c.is_a?(Column) ? c : Functions.col(c.to_s)
        expr = col.to_expr
        if expr.expr_type == :sort_order
          expr.sort_order
        else
          Proto::Expression::SortOrder.new(child: expr, direction: :SORT_DIRECTION_ASCENDING,
                                           null_ordering: :SORT_NULLS_FIRST)
        end
      end
    end

    def frame(type, start_, end_)
      Proto::Expression::Window::WindowFrame.new(frame_type: type, lower: boundary(start_), upper: boundary(end_))
    end

    def boundary(value)
      fb = Proto::Expression::Window::WindowFrame::FrameBoundary
      case value
      when CURRENT_ROW then fb.new(current_row: true)
      when UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING then fb.new(unbounded: true)
      else fb.new(value: Column.lit(value).to_expr)
      end
    end
  end

  # Factory entry point for building {WindowSpec}s. Mirrors PySpark's `Window`.
  module Window
    UNBOUNDED_PRECEDING = WindowSpec::UNBOUNDED_PRECEDING
    UNBOUNDED_FOLLOWING = WindowSpec::UNBOUNDED_FOLLOWING
    CURRENT_ROW = WindowSpec::CURRENT_ROW

    module_function

    # @return [WindowSpec] partitioned by the given columns.
    def partition_by(*cols)
      WindowSpec.new.partition_by(*cols)
    end

    # @return [WindowSpec] ordered by the given columns.
    def order_by(*cols)
      WindowSpec.new.order_by(*cols)
    end

    # @return [WindowSpec] with a row-based frame.
    def rows_between(start_, end_)
      WindowSpec.new.rows_between(start_, end_)
    end

    # @return [WindowSpec] with a range-based frame.
    def range_between(start_, end_)
      WindowSpec.new.range_between(start_, end_)
    end
  end
end
