# frozen_string_literal: true

RSpec.describe SparkConnect::ArrowConverter do
  T = SparkConnect::Types

  def round_trip(rows, schema)
    bytes = described_class.from_rows(rows, schema)
    described_class.to_rows([bytes])
  end

  it "round-trips primitive columns preserving names and values" do
    schema = T.struct(T.field("id", T.long), T.field("name", T.string), T.field("ok", T.boolean))
    rows = [{ "id" => 1, "name" => "a", "ok" => true }, { "id" => 2, "name" => "b", "ok" => false }]
    result = round_trip(rows, schema)
    expect(result.map(&:to_h)).to eq(rows)
    expect(result.first.fields).to eq(%w[id name ok])
  end

  it "round-trips floating point and integer widths" do
    schema = T.struct(T.field("f", T.double), T.field("i", T.integer))
    rows = [{ "f" => 1.5, "i" => 42 }]
    expect(round_trip(rows, schema).first.to_h).to eq(rows.first)
  end

  it "round-trips array columns" do
    schema = T.struct(T.field("xs", T.array(T.long)))
    rows = [{ "xs" => [1, 2, 3] }, { "xs" => [] }]
    expect(round_trip(rows, schema).map { |r| r["xs"] }).to eq([[1, 2, 3], []])
  end

  it "round-trips struct columns into Ruby Hashes" do
    schema = T.struct(T.field("p", T.struct(T.field("x", T.long), T.field("y", T.long))))
    rows = [{ "p" => { "x" => 1, "y" => 2 } }]
    decoded = round_trip(rows, schema).first["p"]
    expect(decoded["x"]).to eq(1)
    expect(decoded["y"]).to eq(2)
  end

  it "returns an empty array for no batches" do
    expect(described_class.to_rows([])).to eq([])
  end

  it "builds an Arrow table from batches" do
    schema = T.struct(T.field("id", T.long))
    bytes = described_class.from_rows([{ "id" => 1 }, { "id" => 2 }], schema)
    table = described_class.to_table([bytes])
    expect(table.n_rows).to eq(2)
  end

  it "accepts arrays and Row objects as input rows" do
    schema = T.struct(T.field("a", T.long), T.field("b", T.string))
    rows = [[1, "x"], SparkConnect::Row.new({ "a" => 2, "b" => "y" })]
    expect(round_trip(rows, schema).map(&:to_a)).to eq([[1, "x"], [2, "y"]])
  end

  describe "#arrow_field_type" do
    # Regression: TimestampType (an instant) must be tagged UTC so the server
    # reads epoch micros as a point in time rather than session-local wall time.
    # TimestampNTZType must stay zone-less.
    it "tags TimestampType as UTC and leaves TimestampNTZType zone-less" do
      tz = described_class.arrow_field_type(T.timestamp)
      expect(tz).to be_a(Arrow::TimestampDataType)
      expect(tz.time_zone.identifier).to eq("UTC")

      ntz = described_class.arrow_field_type(T.timestamp_ntz)
      expect(ntz).to eq({ type: :timestamp, unit: :micro })
    end

    it "maps the primitive Spark types to Arrow types" do
      expect(described_class.arrow_field_type(T.integer)).to eq(:int32)
      expect(described_class.arrow_field_type(T.long)).to eq(:int64)
      expect(described_class.arrow_field_type(T.double)).to eq(:double)
      expect(described_class.arrow_field_type(T.boolean)).to eq(:boolean)
      expect(described_class.arrow_field_type(T.date)).to eq(:date32)
    end
  end

  describe "#extract_value" do
    it "reads a Hash by string or symbol key and an Array by index" do
      expect(described_class.extract_value({ "a" => 1 }, "a", 0)).to eq(1)
      expect(described_class.extract_value({ a: 2 }, "a", 0)).to eq(2)
      expect(described_class.extract_value([9, 8], "b", 1)).to eq(8)
    end

    it "returns nil for a Hash that has neither the string nor symbol key" do
      expect(described_class.extract_value({ "other" => 1 }, "a", 0)).to be_nil
    end
  end
end
