# frozen_string_literal: true

RSpec.describe SparkConnect::Observation do
  describe "naming" do
    it "auto-generates a unique name when none is given" do
      a = described_class.new
      b = described_class.new
      expect(a.name).to match(/\Aobservation_\d+\z/)
      expect(a.name).not_to eq(b.name)
    end

    it "uses the supplied name" do
      expect(described_class.new("metrics").name).to eq("metrics")
    end
  end

  describe "#get" do
    let(:obs) { described_class.new("m") }

    it "raises when not yet attached to a DataFrame" do
      expect { obs.get }.to raise_error(SparkConnect::IllegalArgumentError, /not been attached/)
    end

    it "decodes the observed metric literals into a Hash" do
      spark.range(3).observe(obs, f.count(f.lit(1)).alias("rows"))
      observed = SparkConnect::Proto::ExecutePlanResponse::ObservedMetrics.new(
        name: "m",
        keys: %w[rows max_id],
        values: [
          SparkConnect::Proto::Expression::Literal.new(long: 3),
          SparkConnect::Proto::Expression::Literal.new(long: 2),
        ]
      )
      result = SparkConnect::SparkConnectClient::ExecuteResult.new([], nil, nil, [observed], nil, 0)
      allow(fake_client).to receive(:execute_plan).and_return(result)

      expect(obs.get).to eq({ "rows" => 3, "max_id" => 2 })
    end

    it "memoizes the metrics (executes only once)" do
      spark.range(1).observe(obs, f.count(f.lit(1)).alias("rows"))
      observed = SparkConnect::Proto::ExecutePlanResponse::ObservedMetrics.new(
        name: "m", keys: %w[rows], values: [SparkConnect::Proto::Expression::Literal.new(long: 1)]
      )
      result = SparkConnect::SparkConnectClient::ExecuteResult.new([], nil, nil, [observed], nil, 0)
      expect(fake_client).to receive(:execute_plan).once.and_return(result)

      2.times { obs.get }
    end

    it "returns an empty Hash when the server reports no observed metrics" do
      spark.range(1).observe(obs, f.count(f.lit(1)).alias("rows"))
      result = SparkConnect::SparkConnectClient::ExecuteResult.new([], nil, nil, [], nil, 0)
      allow(fake_client).to receive(:execute_plan).and_return(result)

      expect(obs.get).to eq({})
    end

    it "falls back to the first observed metric when the name does not match" do
      spark.range(1).observe(obs, f.count(f.lit(1)).alias("rows"))
      observed = SparkConnect::Proto::ExecutePlanResponse::ObservedMetrics.new(
        name: "other", keys: %w[rows], values: [SparkConnect::Proto::Expression::Literal.new(long: 7)]
      )
      result = SparkConnect::SparkConnectClient::ExecuteResult.new([], nil, nil, [observed], nil, 0)
      allow(fake_client).to receive(:execute_plan).and_return(result)

      expect(obs.get).to eq({ "rows" => 7 })
    end
  end
end
