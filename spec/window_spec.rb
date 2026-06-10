# frozen_string_literal: true

RSpec.describe SparkConnect::WindowSpec do
  describe "#partition_by" do
    it "populates partition_spec with column expressions" do
      w = described_class.new.partition_by("dept", "team")
      expect(w.partition_spec.size).to eq(2)
      names = w.partition_spec.map { |e| e.unresolved_attribute.unparsed_identifier }
      expect(names).to eq(%w[dept team])
    end

    it "accepts Column arguments" do
      w = described_class.new.partition_by(f.col("dept"))
      expect(w.partition_spec.first.unresolved_attribute.unparsed_identifier).to eq("dept")
    end

    it "returns a copy, leaving the original untouched" do
      base = described_class.new
      w = base.partition_by("dept")
      expect(base.partition_spec).to be_empty
      expect(w.partition_spec.size).to eq(1)
    end
  end

  describe "#order_by" do
    it "populates order_spec as SortOrder messages defaulting to ascending/nulls-first" do
      w = described_class.new.order_by("salary")
      expect(w.order_spec.size).to eq(1)
      so = w.order_spec.first
      expect(so.child.unresolved_attribute.unparsed_identifier).to eq("salary")
      expect(so.direction).to eq(:SORT_DIRECTION_ASCENDING)
      expect(so.null_ordering).to eq(:SORT_NULLS_FIRST)
    end

    it "preserves an explicit desc ordering" do
      w = described_class.new.order_by(f.col("salary").desc)
      expect(w.order_spec.first.direction).to eq(:SORT_DIRECTION_DESCENDING)
    end
  end

  describe "frames" do
    it "rows_between builds a row frame with the right boundaries" do
      w = described_class.new.rows_between(
        SparkConnect::WindowSpec::UNBOUNDED_PRECEDING,
        SparkConnect::WindowSpec::CURRENT_ROW
      )
      frame = w.frame_spec
      expect(frame.frame_type).to eq(:FRAME_TYPE_ROW)
      expect(frame.lower.unbounded).to be(true)
      expect(frame.upper.current_row).to be(true)
    end

    it "range_between builds a range frame with the right boundaries" do
      w = described_class.new.range_between(
        SparkConnect::WindowSpec::CURRENT_ROW,
        SparkConnect::WindowSpec::UNBOUNDED_FOLLOWING
      )
      frame = w.frame_spec
      expect(frame.frame_type).to eq(:FRAME_TYPE_RANGE)
      expect(frame.lower.current_row).to be(true)
      expect(frame.upper.unbounded).to be(true)
    end

    it "encodes numeric offsets as literal value boundaries" do
      w = described_class.new.rows_between(-1, 1)
      frame = w.frame_spec
      lower = frame.lower.value.literal
      upper = frame.upper.value.literal
      expect(lower.integer || lower.long).to eq(-1)
      expect(upper.integer || upper.long).to eq(1)
    end
  end

  describe "chaining" do
    it "combines partition, order and frame on one spec" do
      w = described_class.new
                         .partition_by("dept")
                         .order_by("salary")
                         .rows_between(SparkConnect::WindowSpec::UNBOUNDED_PRECEDING,
                                       SparkConnect::WindowSpec::CURRENT_ROW)
      expect(w.partition_spec.size).to eq(1)
      expect(w.order_spec.size).to eq(1)
      expect(w.frame_spec.frame_type).to eq(:FRAME_TYPE_ROW)
    end
  end
end

RSpec.describe SparkConnect::Window do
  it "exposes boundary constants matching WindowSpec" do
    expect(SparkConnect::Window::UNBOUNDED_PRECEDING).to eq(SparkConnect::WindowSpec::UNBOUNDED_PRECEDING)
    expect(SparkConnect::Window::UNBOUNDED_FOLLOWING).to eq(SparkConnect::WindowSpec::UNBOUNDED_FOLLOWING)
    expect(SparkConnect::Window::CURRENT_ROW).to eq(0)
  end

  it "partition_by factory builds a WindowSpec" do
    w = described_class.partition_by("dept")
    expect(w).to be_a(SparkConnect::WindowSpec)
    expect(w.partition_spec.size).to eq(1)
  end

  it "order_by factory builds a WindowSpec" do
    w = described_class.order_by("salary")
    expect(w.order_spec.size).to eq(1)
  end

  it "rows_between / range_between factories build frames" do
    expect(described_class.rows_between(0, 0).frame_spec.frame_type).to eq(:FRAME_TYPE_ROW)
    expect(described_class.range_between(0, 0).frame_spec.frame_type).to eq(:FRAME_TYPE_RANGE)
  end
end

RSpec.describe "Column#over" do
  it "wraps a window function in an Expression.Window carrying the spec" do
    w = SparkConnect::Window.partition_by("dept").order_by("salary")
    col = f.row_number.over(w)
    expr = col.to_expr

    expect(expr.expr_type).to eq(:window)
    win = expr.window
    expect(win.window_function.unresolved_function.function_name).to eq("row_number")
    expect(win.partition_spec.size).to eq(1)
    expect(win.order_spec.size).to eq(1)
  end

  it "attaches the frame_spec when present" do
    w = SparkConnect::Window.order_by("salary").rows_between(
      SparkConnect::Window::UNBOUNDED_PRECEDING, SparkConnect::Window::CURRENT_ROW
    )
    win = f.rank.over(w).to_expr.window
    expect(win.frame_spec.frame_type).to eq(:FRAME_TYPE_ROW)
  end
end
