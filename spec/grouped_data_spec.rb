# frozen_string_literal: true

RSpec.describe SparkConnect::GroupedData do
  let(:df) { spark.range(10) }

  def agg_rel(grouped_df)
    expect(rel_type(grouped_df)).to eq(:aggregate)
    rel_body(grouped_df)
  end

  describe "#agg" do
    it "builds an aggregate relation with grouping + aggregate expressions" do
      result = df.group_by("id").agg(f.sum("id").alias("total"))
      agg = agg_rel(result)

      expect(agg.group_type).to eq(:GROUP_TYPE_GROUPBY)
      expect(agg.input).to eq(df.relation)
      expect(agg.grouping_expressions.size).to eq(1)
      expect(agg.grouping_expressions.first.unresolved_attribute.unparsed_identifier).to eq("id")
      expect(agg.aggregate_expressions.size).to eq(1)
    end

    it "accepts multiple aggregate Columns" do
      result = df.group_by("id").agg(f.max("id"), f.min("id"))
      agg = agg_rel(result)
      expect(agg.aggregate_expressions.size).to eq(2)
    end

    it "accepts a Hash of {col => fn}" do
      result = df.group_by("id").agg("id" => "max")
      agg = agg_rel(result)

      expect(agg.aggregate_expressions.size).to eq(1)
      fn = agg.aggregate_expressions.first.unresolved_function
      expect(fn.function_name).to eq("max")
      expect(fn.arguments.first.unresolved_attribute.unparsed_identifier).to eq("id")
    end

    it "accepts a multi-entry Hash" do
      result = df.group_by("id").agg("id" => "max", "id2" => "avg")
      agg = agg_rel(result)
      names = agg.aggregate_expressions.map { |e| e.unresolved_function.function_name }
      expect(names).to contain_exactly("max", "avg")
    end
  end

  describe "shorthand aggregates" do
    it "#count builds a count aggregate aliased as 'count'" do
      agg = agg_rel(df.group_by("id").count)
      expect(agg.aggregate_expressions.size).to eq(1)
      a = agg.aggregate_expressions.first.alias
      expect(a.name).to eq(["count"])
      expect(a.expr.unresolved_function.function_name).to eq("count")
    end

    {
      sum: "sum", avg: "avg", mean: "avg", max: "max", min: "min",
    }.each do |method, fn_name|
      it "##{method} builds a #{fn_name} aggregate over the given column" do
        agg = agg_rel(df.group_by("id").public_send(method, "id"))
        fn = agg.aggregate_expressions.first.unresolved_function
        expect(fn.function_name).to eq(fn_name)
        expect(fn.arguments.first.unresolved_attribute.unparsed_identifier).to eq("id")
      end
    end

    it "#sum supports multiple columns" do
      agg = agg_rel(df.group_by("id").sum("a", "b"))
      expect(agg.aggregate_expressions.size).to eq(2)
    end
  end

  describe "group types" do
    it "rollup sets GROUP_TYPE_ROLLUP" do
      expect(agg_rel(df.rollup("id").count).group_type).to eq(:GROUP_TYPE_ROLLUP)
    end

    it "cube sets GROUP_TYPE_CUBE" do
      expect(agg_rel(df.cube("id").count).group_type).to eq(:GROUP_TYPE_CUBE)
    end
  end

  describe "#pivot" do
    it "sets GROUP_TYPE_PIVOT and the pivot column without values" do
      result = df.group_by("id").pivot("year").sum("id")
      agg = agg_rel(result)

      expect(agg.group_type).to eq(:GROUP_TYPE_PIVOT)
      expect(agg.pivot.col.unresolved_attribute.unparsed_identifier).to eq("year")
      expect(agg.pivot.values).to be_empty
    end

    it "encodes explicit pivot values as literals" do
      result = df.group_by("id").pivot("year", [2020, 2021]).sum("id")
      agg = agg_rel(result)

      expect(agg.pivot.values.size).to eq(2)
      expect(agg.pivot.values.map { |v| v.integer || v.long }).to include(2020, 2021)
    end

    it "accepts a Column as the pivot column" do
      result = df.group_by("id").pivot(f.col("year")).sum("id")
      agg = agg_rel(result)
      expect(agg.pivot.col.unresolved_attribute.unparsed_identifier).to eq("year")
    end
  end
end
