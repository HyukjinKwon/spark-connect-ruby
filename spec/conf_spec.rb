# frozen_string_literal: true

RSpec.describe SparkConnect::RuntimeConfig do
  let(:client) { SpecHelpers::FakeClient.new }
  let(:conf) { described_class.new(client) }

  def last_op_type
    client.config_operations.last.op_type
  end

  describe "#set" do
    it "issues a Set operation with the key/value pair" do
      conf.set("spark.sql.shuffle.partitions", 8)
      expect(last_op_type).to eq(:set)
      pair = client.config_operations.last.set.pairs.first
      expect(pair.key).to eq("spark.sql.shuffle.partitions")
      expect(pair.value).to eq("8")
    end
  end

  describe "#get" do
    it "issues a Get operation and returns the value" do
      expect(conf.get("k")).to eq("v") # FakeClient returns key=k,value=v
      expect(last_op_type).to eq(:get)
      expect(client.config_operations.last.get.keys.to_a).to eq(%w[k])
    end

    it "issues a GetWithDefault operation when a default is supplied" do
      conf.get("missing.key", "fallback")
      expect(last_op_type).to eq(:get_with_default)
      pair = client.config_operations.last.get_with_default.pairs.first
      expect(pair.key).to eq("missing.key")
      expect(pair.value).to eq("fallback")
    end
  end

  describe "#unset" do
    it "issues an Unset operation" do
      conf.unset("k")
      expect(last_op_type).to eq(:unset)
      expect(client.config_operations.last.unset.keys.to_a).to eq(%w[k])
    end
  end

  describe "#get_all" do
    it "issues a GetAll operation and returns a hash" do
      expect(conf.get_all).to eq({ "k" => "v" })
      expect(last_op_type).to eq(:get_all)
    end

    it "passes a prefix when given" do
      conf.get_all("spark.sql")
      expect(client.config_operations.last.get_all.prefix).to eq("spark.sql")
    end
  end

  describe "#modifiable?" do
    it "issues an IsModifiable operation" do
      expect(conf.modifiable?("k")).to be(false) # FakeClient value "v" != "true"
      expect(last_op_type).to eq(:is_modifiable)
    end
  end
end
