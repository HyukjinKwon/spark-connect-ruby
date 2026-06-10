# frozen_string_literal: true

RSpec.describe SparkConnect::DataFrameStatFunctions do
  let(:client) { SpecHelpers::FakeClient.new }
  let(:session) { fake_session(client) }
  let(:df) { session.range(100) }

  def scalar_double(value)
    client.rows = [{ "v" => value }]
    client.schema = SparkConnect::Types.struct(SparkConnect::Types.field("v", SparkConnect::Types.double))
  end

  describe "#cov / #corr" do
    it "builds a StatCov relation and returns the scalar" do
      scalar_double(2.5)
      expect(df.stat.cov("x", "y")).to eq(2.5)
      body = client.last_relation.cov
      expect(body.col1).to eq("x")
      expect(body.col2).to eq("y")
    end

    it "builds a StatCorr relation carrying the method" do
      scalar_double(1.0)
      expect(df.stat.corr("x", "y")).to eq(1.0)
      # `.method` is shadowed by Object#method, so read the proto field via to_h.
      expect(client.last_relation.corr.to_h[:method]).to eq("pearson")
    end
  end

  describe "#crosstab / #freq_items / #sample_by" do
    it "builds a StatCrosstab relation (lazy DataFrame)" do
      result = df.stat.crosstab("a", "b")
      expect(rel_type(result)).to eq(:crosstab)
      expect(rel_body(result).col1).to eq("a")
    end

    it "builds a StatFreqItems relation with support" do
      body = rel_body(df.stat.freq_items(%w[a b], 0.05))
      expect(body.cols.to_a).to eq(%w[a b])
      expect(body.support).to be_within(1e-9).of(0.05)
    end

    it "builds a StatSampleBy relation with fractions" do
      body = rel_body(df.stat.sample_by("k", { "a" => 0.1, "b" => 0.2 }, 7))
      expect(body.fractions.map(&:fraction)).to eq([0.1, 0.2])
      expect(body.seed).to eq(7)
    end
  end

  describe "#approx_quantile" do
    it "returns a flat list for a single column" do
      client.rows = [{ "q" => [25.0, 50.0, 75.0] }]
      client.schema = SparkConnect::Types.struct(
        SparkConnect::Types.field("q", SparkConnect::Types.array(SparkConnect::Types.double))
      )
      result = df.stat.approx_quantile("x", [0.25, 0.5, 0.75], 0.01)
      expect(result).to eq([25.0, 50.0, 75.0])
      body = client.last_relation.approx_quantile
      expect(body.cols.to_a).to eq(%w[x])
      expect(body.probabilities.to_a).to eq([0.25, 0.5, 0.75])
    end
  end

  describe "DataFrame#describe / #summary" do
    it "builds StatDescribe and StatSummary relations" do
      expect(rel_type(df.describe("a", "b"))).to eq(:describe)
      expect(rel_body(df.describe("a")).cols.to_a).to eq(%w[a])
      expect(rel_type(df.summary("count", "mean"))).to eq(:summary)
      expect(rel_body(df.summary("count")).statistics.to_a).to eq(%w[count])
    end
  end
end
