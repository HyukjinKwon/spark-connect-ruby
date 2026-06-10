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
end
