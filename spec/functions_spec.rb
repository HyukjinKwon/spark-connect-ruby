# frozen_string_literal: true

RSpec.describe SparkConnect::Functions do
  def uf(col)
    col.to_expr.unresolved_function
  end

  describe "core constructors" do
    it "col builds an unresolved attribute" do
      e = f.col("a").to_expr
      expect(e.expr_type).to eq(:unresolved_attribute)
      expect(e.unresolved_attribute.unparsed_identifier).to eq("a")
    end

    it "col accepts a Symbol" do
      expect(f.col(:a).to_expr.unresolved_attribute.unparsed_identifier).to eq("a")
    end

    it "column is an alias for col" do
      expect(f.column("a").to_expr.expr_type).to eq(:unresolved_attribute)
    end

    it "lit builds a literal" do
      e = f.lit(5).to_expr
      expect(e.expr_type).to eq(:literal)
      expect(e.literal.integer).to eq(5)
    end

    it "expr builds an expression string" do
      e = f.expr("a + 1").to_expr
      expect(e.expr_type).to eq(:expression_string)
      expect(e.expression_string.expression).to eq("a + 1")
    end
  end

  describe "String args as COLUMN NAMES" do
    it "sum/avg/max/min treat a String as a column reference" do
      %w[sum avg max min].each do |fn|
        e = uf(f.public_send(fn, "salary"))
        expect(e.function_name).to eq(fn)
        expect(e.arguments[0].expr_type).to eq(:unresolved_attribute)
        expect(e.arguments[0].unresolved_attribute.unparsed_identifier).to eq("salary")
      end
    end

    it "count treats a String as a column reference" do
      e = uf(f.count("x"))
      expect(e.function_name).to eq("count")
      expect(e.arguments[0].expr_type).to eq(:unresolved_attribute)
    end
  end

  describe "String args as LITERALS" do
    it "regexp_extract treats the pattern as a literal" do
      e = uf(f.regexp_extract("col", "(\\d+)", 1))
      expect(e.arguments[0].expr_type).to eq(:unresolved_attribute)
      expect(e.arguments[1].literal.string).to eq("(\\d+)")
      expect(e.arguments[2].literal.integer).to eq(1)
    end

    it "date_format treats the format as a literal" do
      e = uf(f.date_format("d", "yyyy-MM-dd"))
      expect(e.arguments[0].expr_type).to eq(:unresolved_attribute)
      expect(e.arguments[1].literal.string).to eq("yyyy-MM-dd")
    end

    it "split treats the pattern as a literal" do
      e = uf(f.split("c", ","))
      expect(e.arguments[1].literal.string).to eq(",")
      expect(e.arguments[2].literal.integer).to eq(-1)
    end

    it "concat_ws treats the separator as a literal but the rest as columns" do
      e = uf(f.concat_ws("-", "a", "b"))
      expect(e.function_name).to eq("concat_ws")
      expect(e.arguments[0].literal.string).to eq("-")
      expect(e.arguments[1].expr_type).to eq(:unresolved_attribute)
      expect(e.arguments[2].expr_type).to eq(:unresolved_attribute)
    end
  end

  describe "when" do
    it "starts a CASE expression" do
      e = uf(f.when(f.col("x") > 0, "pos"))
      expect(e.function_name).to eq("when")
      expect(e.arguments.size).to eq(2)
    end
  end

  describe "count and count_distinct" do
    it "count('*') becomes count(lit(1))" do
      e = uf(f.count("*"))
      expect(e.function_name).to eq("count")
      expect(e.arguments.size).to eq(1)
      expect(e.arguments[0].expr_type).to eq(:literal)
      expect(e.arguments[0].literal.integer).to eq(1)
    end

    it "count_distinct sets is_distinct over all columns" do
      e = uf(f.count_distinct("a", "b"))
      expect(e.function_name).to eq("count")
      expect(e.is_distinct).to be(true)
      expect(e.arguments.size).to eq(2)
      expect(e.arguments[0].expr_type).to eq(:unresolved_attribute)
    end

    it "countDistinct is an alias" do
      expect(uf(f.countDistinct("a")).is_distinct).to be(true)
    end

    it "sum_distinct sets is_distinct on sum" do
      e = uf(f.sum_distinct("a"))
      expect(e.function_name).to eq("sum")
      expect(e.is_distinct).to be(true)
    end
  end

  describe "UNIFORM functions" do
    it "are all defined and build the right function_name" do
      described_class::UNIFORM.each do |fn|
        expect(f).to respond_to(fn)
        expect(uf(f.public_send(fn, "x")).function_name).to eq(fn)
      end
    end

    it "treat String args as column references" do
      e = uf(f.upper("name"))
      expect(e.function_name).to eq("upper")
      expect(e.arguments[0].expr_type).to eq(:unresolved_attribute)
    end

    it "pass through multiple columns" do
      e = uf(f.greatest("a", "b", "c"))
      expect(e.arguments.size).to eq(3)
    end
  end

  describe "NO_ARG functions" do
    it "are all defined and build an argument-free unresolved_function" do
      described_class::NO_ARG.each do |fn|
        expect(f).to respond_to(fn)
        e = uf(f.public_send(fn))
        expect(e.function_name).to eq(fn)
        expect(e.arguments.size).to eq(0)
      end
    end
  end

  describe "higher-order functions" do
    it "transform builds a lambda_function argument" do
      e = uf(f.transform("arr") { |x| x + 1 })
      expect(e.function_name).to eq("transform")
      expect(e.arguments[0].expr_type).to eq(:unresolved_attribute)
      lam = e.arguments[1]
      expect(lam.expr_type).to eq(:lambda_function)
      expect(lam.lambda_function.arguments.size).to eq(1)
    end

    it "filter builds a lambda_function" do
      e = uf(f.filter("arr") { |x| x > 0 })
      expect(e.function_name).to eq("filter")
      expect(e.arguments[1].expr_type).to eq(:lambda_function)
    end

    it "the lambda body references the lambda variable" do
      e = uf(f.transform("arr") { |x| x + 1 })
      body = e.arguments[1].lambda_function.function
      expect(body.expr_type).to eq(:unresolved_function)
      var = body.unresolved_function.arguments[0]
      expect(var.expr_type).to eq(:unresolved_named_lambda_variable)
    end

    it "zip_with builds a 2-argument lambda" do
      e = uf(f.zip_with("l", "r") { |a, b| a + b })
      expect(e.function_name).to eq("zip_with")
      expect(e.arguments[2].lambda_function.arguments.size).to eq(2)
    end

    it "aggregate builds an accumulator and merge lambda" do
      e = uf(f.aggregate("arr", f.lit(0), ->(acc, x) { acc + x }))
      expect(e.function_name).to eq("aggregate")
      expect(e.arguments.size).to eq(3)
      expect(e.arguments[1].expr_type).to eq(:literal)
      expect(e.arguments[2].expr_type).to eq(:lambda_function)
    end

    it "aggregate supports an optional finish lambda" do
      e = uf(f.aggregate("arr", f.lit(0), ->(acc, x) { acc + x }, ->(acc) { acc * 2 }))
      expect(e.arguments.size).to eq(4)
      expect(e.arguments[3].expr_type).to eq(:lambda_function)
    end
  end

  describe "asc / desc helpers" do
    it "build sort orders from a column name" do
      expect(f.asc("a").to_expr.sort_order.direction).to eq(:SORT_DIRECTION_ASCENDING)
      expect(f.desc("a").to_expr.sort_order.direction).to eq(:SORT_DIRECTION_DESCENDING)
    end
  end

  describe "udf" do
    it "is not supported" do
      expect { f.udf }.to raise_error(SparkConnect::NotImplementedError)
    end
  end
end
