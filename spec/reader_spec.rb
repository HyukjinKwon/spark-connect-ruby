# frozen_string_literal: true

RSpec.describe SparkConnect::DataFrameReader do
  let(:reader) { spark.read }

  def read_rel(df)
    expect(rel_type(df)).to eq(:read)
    rel_body(df)
  end

  describe "#load" do
    it "builds a read relation with a DataSource carrying format/paths" do
      df = reader.format("csv").load("data.csv")
      ds = read_rel(df).data_source

      expect(ds.format).to eq("csv")
      expect(ds.paths).to eq(["data.csv"])
    end

    it "carries options" do
      df = reader.format("csv").option("header", true).options("sep" => ";").load("a.csv")
      ds = read_rel(df).data_source
      expect(ds.options["header"]).to eq("true")
      expect(ds.options["sep"]).to eq(";")
    end

    it "carries a schema as a DDL/simple string from a StructType" do
      schema = SparkConnect::Types.struct(
        SparkConnect::Types.field("id", SparkConnect::Types.long)
      )
      df = reader.format("csv").schema(schema).load("a.csv")
      ds = read_rel(df).data_source
      expect(ds.schema).to eq(schema.simple_string)
    end

    it "carries a DDL string schema as-is" do
      df = reader.format("json").schema("id BIGINT, name STRING").load("a.json")
      expect(read_rel(df).data_source.schema).to eq("id BIGINT, name STRING")
    end

    it "supports multiple paths" do
      df = reader.format("parquet").load("a", "b")
      expect(read_rel(df).data_source.paths).to eq(%w[a b])
    end

    it "returns a DataFrame" do
      expect(reader.format("csv").load("a")).to be_a(SparkConnect::DataFrame)
    end
  end

  describe "#table" do
    it "builds a read relation with a NamedTable" do
      df = reader.table("db.tbl")
      nt = read_rel(df).named_table
      expect(nt.unparsed_identifier).to eq("db.tbl")
    end
  end

  describe "format helpers" do
    {
      csv: "csv", json: "json", parquet: "parquet", orc: "orc", text: "text",
    }.each do |method, fmt|
      it "##{method} sets format=#{fmt} and the path" do
        df = reader.public_send(method, "f.#{fmt}")
        ds = read_rel(df).data_source
        expect(ds.format).to eq(fmt)
        expect(ds.paths).to eq(["f.#{fmt}"])
      end
    end
  end

  describe "#jdbc" do
    it "uses the jdbc format with url/dbtable options and merged properties" do
      df = reader.jdbc("jdbc:postgresql://h/db", "people", "user" => "u", "password" => "p")
      ds = read_rel(df).data_source

      expect(ds.format).to eq("jdbc")
      expect(ds.options["url"]).to eq("jdbc:postgresql://h/db")
      expect(ds.options["dbtable"]).to eq("people")
      expect(ds.options["user"]).to eq("u")
      expect(ds.options["password"]).to eq("p")
      expect(ds.paths).to be_empty
    end
  end
end
