# frozen_string_literal: true

RSpec.describe SparkConnect::DataFrameNaFunctions do
  let(:df) { spark.range(10) }

  describe "#drop" do
    it "builds an NADrop relation with min_non_nulls from the subset size for how: :any" do
      result = df.na.drop(subset: %w[a b])
      expect(rel_type(result)).to eq(:drop_na)
      body = rel_body(result)
      expect(body.cols.to_a).to eq(%w[a b])
      expect(body.min_non_nulls).to eq(2)
    end

    it "uses thresh when provided" do
      body = rel_body(df.na.drop(thresh: 1, subset: %w[a b c]))
      expect(body.min_non_nulls).to eq(1)
    end

    it "drops only fully-null rows for how: :all" do
      body = rel_body(df.na.drop(how: :all))
      expect(body.min_non_nulls).to eq(1)
    end
  end

  describe "#fill" do
    it "fills a scalar across a subset, widening Integer to Long" do
      body = rel_body(df.na.fill(0, subset: %w[a b]))
      expect(rel_type(df.na.fill(0, subset: %w[a]))).to eq(:fill_na)
      expect(body.cols.to_a).to eq(%w[a b])
      expect(body.values.map(&:literal_type)).to eq(%i[long long])
      expect(body.values.first.long).to eq(0)
    end

    it "fills per-column from a Hash" do
      body = rel_body(df.na.fill({ "name" => "unknown", "age" => 0 }))
      expect(body.cols.to_a).to eq(%w[name age])
      expect(body.values.map(&:literal_type)).to eq(%i[string long])
    end

    it "encodes Float fill values as Double" do
      body = rel_body(df.na.fill(1.5, subset: %w[x]))
      expect(body.values.first.literal_type).to eq(:double)
    end
  end

  describe "#replace" do
    it "builds NAReplace from a Hash mapping" do
      body = rel_body(df.na.replace({ "old" => "new" }, subset: %w[s]))
      expect(rel_type(df.na.replace({ "a" => "b" }))).to eq(:replace)
      expect(body.cols.to_a).to eq(%w[s])
      repl = body.replacements.first
      expect(repl.old_value.string).to eq("old")
      expect(repl.new_value.string).to eq("new")
    end

    it "builds NAReplace from to_replace/value arrays" do
      body = rel_body(df.na.replace([1, 2], [10, 20], subset: %w[n]))
      expect(body.replacements.map { |r| r.old_value.long }).to eq([1, 2])
      expect(body.replacements.map { |r| r.new_value.long }).to eq([10, 20])
    end
  end
end
