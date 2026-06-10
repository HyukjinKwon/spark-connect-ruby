# frozen_string_literal: true

RSpec.describe SparkConnect::Row do
  def row
    SparkConnect::Row.new({ "id" => 1, "name" => "alice", "active" => true })
  end

  describe "construction" do
    it "builds from an ordered hash" do
      r = row
      expect(r.fields).to eq(%w[id name active])
      expect(r.values).to eq([1, "alice", true])
    end

    it "builds from positional values with field names" do
      r = described_class.new([1, "alice"], fields: %i[id name])
      expect(r.fields).to eq(%w[id name])
      expect(r.values).to eq([1, "alice"])
    end
  end

  describe "#[]" do
    it "looks up by positional index" do
      expect(row[0]).to eq(1)
      expect(row[1]).to eq("alice")
    end

    it "looks up by string name" do
      expect(row["name"]).to eq("alice")
    end

    it "looks up by symbol name" do
      expect(row[:name]).to eq("alice")
    end

    it "returns nil for an unknown name" do
      expect(row["missing"]).to be_nil
    end
  end

  describe "dynamic method access" do
    it "exposes field values as methods" do
      expect(row.name).to eq("alice")
      expect(row.id).to eq(1)
      expect(row.active).to be(true)
    end

    it "responds_to? known fields" do
      expect(row.respond_to?(:name)).to be(true)
      expect(row.respond_to?(:nope)).to be(false)
    end

    it "raises NoMethodError for unknown fields" do
      expect { row.nope }.to raise_error(NoMethodError)
    end
  end

  describe "conversions" do
    it "#to_h returns an ordered hash" do
      expect(row.to_h).to eq({ "id" => 1, "name" => "alice", "active" => true })
    end

    it "aliases #as_dict to #to_h" do
      expect(row.as_dict).to eq(row.to_h)
    end

    it "#to_a returns a copy of the values" do
      r = row
      arr = r.to_a
      expect(arr).to eq([1, "alice", true])
      arr << "mutated"
      expect(r.values).to eq([1, "alice", true])
    end
  end

  describe "Enumerable" do
    it "iterates over values in order" do
      collected = row.map { |v| v }
      expect(collected).to eq([1, "alice", true])
    end

    it "supports Enumerable methods" do
      expect(row.map(&:to_s)).to eq(%w[1 alice true])
      expect(row.to_a).to eq([1, "alice", true])
    end

    it "reports length and size" do
      expect(row.length).to eq(3)
      expect(row.size).to eq(3)
    end
  end

  describe "#field" do
    it "returns the value for a present field" do
      expect(row.field("name")).to eq("alice")
      expect(row.field(:id)).to eq(1)
    end

    it "raises IllegalArgumentError on a missing field" do
      expect { row.field("missing") }.to raise_error(
        SparkConnect::IllegalArgumentError, /No such field: missing/
      )
    end
  end

  describe "equality and hashing" do
    it "is equal when fields and values match" do
      other = described_class.new({ "id" => 1, "name" => "alice", "active" => true })
      expect(row == other).to be(true)
    end

    it "differs when values differ" do
      other = described_class.new({ "id" => 2, "name" => "alice", "active" => true })
      expect(row).not_to eq(other)
    end

    it "differs when field names differ" do
      other = described_class.new({ "ident" => 1, "name" => "alice", "active" => true })
      expect(row).not_to eq(other)
    end

    it "is not equal to a non-Row" do
      expect(row).not_to eq([1, "alice", true])
    end

    it "hashes equal rows identically" do
      expect(row.hash).to eq(row.hash)
    end

    it "works as a hash key" do
      h = { row => "value" }
      expect(h[row]).to eq("value")
    end
  end

  describe "#to_s / #inspect" do
    it "renders a PySpark-style representation" do
      expect(row.to_s).to eq('Row(id=1, name="alice", active=true)')
      expect(row.inspect).to eq(row.to_s)
    end
  end
end
