# frozen_string_literal: true

RSpec.describe SparkConnect::DataFrameNaFunctions do
  let(:df) { spark.range(10) }

  describe "#drop" do
    it "builds an NADrop relation without min_non_nulls for how: :any (like PySpark)" do
      result = df.na.drop(subset: %w[a b])
      expect(rel_type(result)).to eq(:drop_na)
      body = rel_body(result)
      expect(body.cols.to_a).to eq(%w[a b])
      expect(body.has_min_non_nulls?).to be(false)
    end

    it "rejects an unknown how" do
      expect { df.na.drop(how: :some) }.to raise_error(SparkConnect::IllegalArgumentError)
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
    it "fills a scalar across a subset with one value (applied to every column), widening Integer to Long" do
      body = rel_body(df.na.fill(0, subset: %w[a b]))
      expect(rel_type(df.na.fill(0, subset: %w[a]))).to eq(:fill_na)
      expect(body.cols.to_a).to eq(%w[a b])
      expect(body.values.map(&:literal_type)).to eq(%i[long])
      expect(body.values.first.long).to eq(0)
    end

    it "rejects unsupported fill values" do
      expect { df.na.fill(Object.new) }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { df.na.fill({}) }.to raise_error(SparkConnect::IllegalArgumentError)
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

    it "builds NAReplace from to_replace/value arrays, sending integers as doubles like PySpark" do
      body = rel_body(df.na.replace([1, 2], [10, 20], subset: %w[n]))
      expect(body.replacements.map { |r| r.old_value.double }).to eq([1.0, 2.0])
      expect(body.replacements.map { |r| r.new_value.double }).to eq([10.0, 20.0])
    end

    it "broadcasts a scalar value and replaces with null when value is nil" do
      body = rel_body(df.na.replace(%w[a b], nil))
      expect(body.replacements.map { |r| r.new_value.literal_type }).to eq(%i[null null])
    end

    it "requires a value unless to_replace is a Hash, and matching lengths" do
      expect { df.na.replace("a") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { df.na.replace([1, 2], [3]) }.to raise_error(SparkConnect::IllegalArgumentError)
    end
  end
end
