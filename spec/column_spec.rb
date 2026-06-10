# frozen_string_literal: true

require "bigdecimal"
require "date"

RSpec.describe SparkConnect::Column do
  def lit_type(value)
    described_class.to_literal(value).literal_type
  end

  describe ".to_literal" do
    it "encodes nil as null" do
      expect(lit_type(nil)).to eq(:null)
    end

    it "encodes booleans" do
      expect(described_class.to_literal(true).literal_type).to eq(:boolean)
      expect(described_class.to_literal(true).boolean).to be(true)
      expect(described_class.to_literal(false).boolean).to be(false)
    end

    it "encodes Integer as int up to the 2_147_483_647 boundary" do
      expect(lit_type(0)).to eq(:integer)
      expect(lit_type(2_147_483_647)).to eq(:integer)
      expect(described_class.to_literal(2_147_483_647).integer).to eq(2_147_483_647)
      expect(lit_type(-2_147_483_648)).to eq(:integer)
    end

    it "encodes Integer above the boundary as long" do
      expect(lit_type(2_147_483_648)).to eq(:long)
      expect(described_class.to_literal(2_147_483_648).long).to eq(2_147_483_648)
      expect(lit_type(-2_147_483_649)).to eq(:long)
    end

    it "encodes Float as double" do
      expect(lit_type(1.5)).to eq(:double)
      expect(described_class.to_literal(1.5).double).to eq(1.5)
    end

    it "encodes a plain String as string" do
      expect(lit_type("hello")).to eq(:string)
      expect(described_class.to_literal("hello").string).to eq("hello")
    end

    it "encodes an ASCII-8BIT String as binary" do
      bin = "abc".dup.force_encoding(Encoding::ASCII_8BIT)
      expect(lit_type(bin)).to eq(:binary)
      expect(described_class.to_literal(bin).binary).to eq(bin)
    end

    it "encodes a Symbol as string" do
      expect(lit_type(:foo)).to eq(:string)
      expect(described_class.to_literal(:foo).string).to eq("foo")
    end

    it "encodes a Time as timestamp (microseconds)" do
      t = Time.at(1)
      expect(lit_type(t)).to eq(:timestamp)
      expect(described_class.to_literal(t).timestamp).to eq(1_000_000)
    end

    it "encodes a Date as days since epoch" do
      expect(lit_type(Date.new(1970, 1, 1))).to eq(:date)
      expect(described_class.to_literal(Date.new(1970, 1, 1)).date).to eq(0)
      expect(described_class.to_literal(Date.new(1970, 1, 2)).date).to eq(1)
    end

    it "encodes a BigDecimal as decimal preserving the value string" do
      expect(lit_type(BigDecimal("1.50"))).to eq(:decimal)
      expect(described_class.to_literal(BigDecimal("1.50")).decimal.value).to eq("1.5")
    end

    it "encodes an Array literal with inferred element type" do
      lit = described_class.to_literal([1, 2, 3])
      expect(lit.literal_type).to eq(:array)
      expect(lit.array.elements.size).to eq(3)
      expect(lit.array.elements.first.integer).to eq(1)
      expect(lit.array.element_type.has_integer?).to be(true)
    end

    it "encodes a Hash literal as a map" do
      lit = described_class.to_literal({ "a" => 1, "b" => 2 })
      expect(lit.literal_type).to eq(:map)
      expect(lit.map.keys.map(&:string)).to eq(%w[a b])
      expect(lit.map.values.map(&:integer)).to eq([1, 2])
      expect(lit.map.key_type.has_string?).to be(true)
      expect(lit.map.value_type.has_integer?).to be(true)
    end

    it "raises on unsupported types" do
      expect { described_class.to_literal(Object.new) }.to raise_error(SparkConnect::IllegalArgumentError)
    end
  end

  describe ".lit" do
    it "wraps a value in a literal expression" do
      col = described_class.lit(7)
      expect(col).to be_a(described_class)
      expect(col.to_expr.expr_type).to eq(:literal)
      expect(col.to_expr.literal.integer).to eq(7)
    end

    it "returns the same Column when given a Column" do
      c = SparkConnect::F.col("x")
      expect(described_class.lit(c)).to equal(c)
    end
  end

  describe ".from_name" do
    it "builds an unresolved attribute" do
      e = described_class.from_name("age").to_expr
      expect(e.expr_type).to eq(:unresolved_attribute)
      expect(e.unresolved_attribute.unparsed_identifier).to eq("age")
    end

    it "builds an unresolved star for *" do
      expect(described_class.from_name("*").to_expr.expr_type).to eq(:unresolved_star)
    end
  end

  describe "arithmetic operators" do
    let(:a) { SparkConnect::F.col("a") }

    def fn(col)
      col.to_expr.unresolved_function.function_name
    end

    it "builds unresolved_function with the right names" do
      expect(fn(a + 1)).to eq("+")
      expect(fn(a - 1)).to eq("-")
      expect(fn(a * 2)).to eq("*")
      expect(fn(a / 2)).to eq("/")
      expect(fn(a % 2)).to eq("%")
      expect(fn(a**2)).to eq("power")
      expect(fn(-a)).to eq("negative")
    end

    it "leaves +@ as identity" do
      expect(+a).to equal(a)
    end

    it "coerces the rhs literal into the function arguments" do
      e = (a + 5).to_expr.unresolved_function
      expect(e.arguments.size).to eq(2)
      expect(e.arguments[0].expr_type).to eq(:unresolved_attribute)
      expect(e.arguments[1].literal.integer).to eq(5)
    end
  end

  describe "comparison operators" do
    let(:a) { SparkConnect::F.col("a") }

    def fn(col)
      col.to_expr.unresolved_function.function_name
    end

    it "builds the right function names" do
      expect(fn(a == 1)).to eq("==")
      expect(fn(a != 1)).to eq("!=")
      expect(fn(a < 1)).to eq("<")
      expect(fn(a <= 1)).to eq("<=")
      expect(fn(a > 1)).to eq(">")
      expect(fn(a >= 1)).to eq(">=")
      expect(fn(a.eq_null_safe(1))).to eq("<=>")
    end
  end

  describe "boolean operators" do
    let(:a) { SparkConnect::F.col("a") }
    let(:b) { SparkConnect::F.col("b") }

    def fn(col)
      col.to_expr.unresolved_function.function_name
    end

    it "builds and/or/not" do
      expect(fn(a & b)).to eq("and")
      expect(fn(a | b)).to eq("or")
      expect(fn(!a)).to eq("not")
      expect(fn(a.not)).to eq("not")
    end
  end

  describe "alias / name / as" do
    let(:a) { SparkConnect::F.col("a") }

    it "builds an Alias expression" do
      e = a.alias("b").to_expr
      expect(e.expr_type).to eq(:alias)
      expect(e.alias.name).to eq(["b"])
    end

    it "name and as are aliases for alias" do
      expect(a.name("b").to_expr.alias.name).to eq(["b"])
      expect(a.as("b").to_expr.alias.name).to eq(["b"])
    end

    it "supports multiple names" do
      expect(a.alias("x", "y").to_expr.alias.name).to eq(%w[x y])
    end

    it "encodes metadata when given" do
      e = a.alias("b", metadata: { "k" => "v" }).to_expr
      expect(e.alias.metadata).to eq(%({"k":"v"}))
    end
  end

  describe "cast" do
    let(:a) { SparkConnect::F.col("a") }

    it "uses type_str for a String type" do
      e = a.cast("int").to_expr
      expect(e.expr_type).to eq(:cast)
      expect(e.cast.type_str).to eq("int")
    end

    it "uses type for a Types::DataType" do
      e = a.cast(SparkConnect::Types.long).to_expr
      expect(e.cast.type.has_long?).to be(true)
    end

    it "astype and as_type are aliases" do
      expect(a.astype("int").to_expr.cast.type_str).to eq("int")
      expect(a.as_type("int").to_expr.cast.type_str).to eq("int")
    end
  end

  describe "sort ordering" do
    let(:a) { SparkConnect::F.col("a") }

    it "asc builds an ascending, nulls-first sort_order" do
      so = a.asc.to_expr
      expect(so.expr_type).to eq(:sort_order)
      expect(so.sort_order.direction).to eq(:SORT_DIRECTION_ASCENDING)
      expect(so.sort_order.null_ordering).to eq(:SORT_NULLS_FIRST)
    end

    it "desc builds a descending, nulls-last sort_order" do
      so = a.desc.to_expr.sort_order
      expect(so.direction).to eq(:SORT_DIRECTION_DESCENDING)
      expect(so.null_ordering).to eq(:SORT_NULLS_LAST)
    end

    it "supports explicit nulls ordering variants" do
      expect(a.asc_nulls_last.to_expr.sort_order.null_ordering).to eq(:SORT_NULLS_LAST)
      expect(a.desc_nulls_first.to_expr.sort_order.null_ordering).to eq(:SORT_NULLS_FIRST)
    end
  end

  describe "predicates" do
    let(:a) { SparkConnect::F.col("a") }

    def uf(col)
      col.to_expr.unresolved_function
    end

    it "isin builds an 'in' function with literal args" do
      e = uf(a.isin(1, 2, 3))
      expect(e.function_name).to eq("in")
      expect(e.arguments.size).to eq(4)
      expect(e.arguments[1].literal.integer).to eq(1)
    end

    it "isin accepts a single Array argument" do
      expect(uf(a.isin([1, 2])).arguments.size).to eq(3)
    end

    it "between expands to (>= lower) and (<= upper)" do
      e = uf(a.between(1, 10))
      expect(e.function_name).to eq("and")
      expect(e.arguments[0].unresolved_function.function_name).to eq(">=")
      expect(e.arguments[1].unresolved_function.function_name).to eq("<=")
    end

    it "string predicates build the matching function names" do
      expect(uf(a.like("a%")).function_name).to eq("like")
      expect(uf(a.rlike("a.*")).function_name).to eq("rlike")
      expect(uf(a.ilike("A%")).function_name).to eq("ilike")
      expect(uf(a.contains("x")).function_name).to eq("contains")
      expect(uf(a.startswith("x")).function_name).to eq("startswith")
      expect(uf(a.endswith("x")).function_name).to eq("endswith")
    end

    it "null predicates build the matching function names" do
      expect(uf(a.is_null).function_name).to eq("isNull")
      expect(uf(a.is_not_null).function_name).to eq("isNotNull")
      expect(uf(a.is_nan).function_name).to eq("isNaN")
      expect(uf(a.isNull).function_name).to eq("isNull")
      expect(uf(a.isNotNull).function_name).to eq("isNotNull")
    end
  end

  describe "complex-type access" do
    let(:a) { SparkConnect::F.col("a") }

    it "get_item builds unresolved_extract_value with a literal extraction" do
      e = a.get_item(0).to_expr
      expect(e.expr_type).to eq(:unresolved_extract_value)
      expect(e.unresolved_extract_value.extraction.literal.integer).to eq(0)
    end

    it "[] delegates to get_item" do
      e = a["k"].to_expr
      expect(e.expr_type).to eq(:unresolved_extract_value)
      expect(e.unresolved_extract_value.extraction.literal.string).to eq("k")
    end

    it "get_field extracts a struct field by name" do
      e = a.get_field("nested").to_expr
      expect(e.expr_type).to eq(:unresolved_extract_value)
      expect(e.unresolved_extract_value.extraction.literal.string).to eq("nested")
    end
  end

  describe "when / otherwise chaining" do
    it "chains when branches and an otherwise default" do
      col = SparkConnect::F.when(SparkConnect::F.col("x") > 0, "pos")
                           .when(SparkConnect::F.col("x") < 0, "neg")
                           .otherwise("zero")
      e = col.to_expr.unresolved_function
      expect(e.function_name).to eq("when")
      # 2 args per when (cond,val) * 2 + 1 otherwise = 5
      expect(e.arguments.size).to eq(5)
    end

    it "raises if when is chained on a non-when column" do
      expect { SparkConnect::F.col("x").when(SparkConnect::F.col("y"), 1) }
        .to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "raises if otherwise is chained on a non-when column" do
      expect { SparkConnect::F.col("x").otherwise(1) }
        .to raise_error(SparkConnect::IllegalArgumentError)
    end
  end

  describe "over" do
    it "wraps the column in a Window referencing the spec" do
      ws = SparkConnect::Window.partition_by("dept").order_by(SparkConnect::F.col("salary").desc)
      col = SparkConnect::F.sum("salary").over(ws)
      e = col.to_expr
      expect(e.expr_type).to eq(:window)
      expect(e.window.partition_spec.size).to eq(1)
      expect(e.window.order_spec.size).to eq(1)
    end

    it "attaches a frame spec when present" do
      ws = SparkConnect::Window.partition_by("dept").rows_between(-1, 1)
      e = SparkConnect::F.sum("x").over(ws).to_expr
      expect(e.window.frame_spec).not_to be_nil
    end
  end
end
