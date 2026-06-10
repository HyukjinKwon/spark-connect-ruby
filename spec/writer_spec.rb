# frozen_string_literal: true

RSpec.describe SparkConnect::DataFrameWriter do
  let(:df) { spark.range(5) }

  # Run the write and return the WriteOperation recorded by the fake client.
  def write_op(&block)
    block.call(df.write)
    cmd = fake_client.last_command
    expect(cmd.command_type).to eq(:write_operation)
    cmd.write_operation
  end

  describe "#save" do
    it "builds a WriteOperation command with input/source/path" do
      op = write_op { |w| w.format("parquet").save("out.parquet") }
      expect(op.input).to eq(df.relation)
      expect(op.source).to eq("parquet")
      expect(op.path).to eq("out.parquet")
    end

    it "omits the path when none is given" do
      op = write_op { |w| w.format("parquet").save }
      expect(op.path).to eq("")
    end
  end

  describe "#mode" do
    {
      append: :SAVE_MODE_APPEND,
      overwrite: :SAVE_MODE_OVERWRITE,
      error: :SAVE_MODE_ERROR_IF_EXISTS,
      errorifexists: :SAVE_MODE_ERROR_IF_EXISTS,
      error_if_exists: :SAVE_MODE_ERROR_IF_EXISTS,
      ignore: :SAVE_MODE_IGNORE,
    }.each do |sym, proto_mode|
      it "maps #{sym.inspect} to #{proto_mode}" do
        op = write_op { |w| w.format("parquet").mode(sym).save("o") }
        expect(op.mode).to eq(proto_mode)
      end
    end

    it "accepts a string mode" do
      op = write_op { |w| w.format("parquet").mode("overwrite").save("o") }
      expect(op.mode).to eq(:SAVE_MODE_OVERWRITE)
    end

    it "raises on an unknown mode" do
      expect { df.write.mode(:bogus) }.to raise_error(SparkConnect::IllegalArgumentError)
    end
  end

  describe "options and layout" do
    it "carries option(s)" do
      op = write_op { |w| w.format("csv").option("header", true).options("sep" => ";").save("o") }
      expect(op.options["header"]).to eq("true")
      expect(op.options["sep"]).to eq(";")
    end

    it "carries partition_by columns" do
      op = write_op { |w| w.format("parquet").partition_by("a", "b").save("o") }
      expect(op.partitioning_columns).to eq(%w[a b])
    end

    it "carries sort_by columns" do
      op = write_op { |w| w.format("parquet").sort_by("a").bucket_by(4, "b").save_as_table("t") }
      expect(op.sort_column_names).to eq(["a"])
    end

    it "carries bucket_by spec" do
      op = write_op { |w| w.format("parquet").bucket_by(8, "a", "b").save_as_table("t") }
      expect(op.bucket_by.num_buckets).to eq(8)
      expect(op.bucket_by.bucket_column_names).to eq(%w[a b])
    end
  end

  describe "#save_as_table" do
    it "sets a SaveTable with SAVE_AS_TABLE method" do
      op = write_op { |w| w.format("parquet").save_as_table("my_tbl") }
      expect(op.table.table_name).to eq("my_tbl")
      expect(op.table.save_method).to eq(:TABLE_SAVE_METHOD_SAVE_AS_TABLE)
    end
  end

  describe "#insert_into" do
    it "sets a SaveTable with INSERT_INTO method" do
      op = write_op { |w| w.insert_into("my_tbl") }
      expect(op.table.table_name).to eq("my_tbl")
      expect(op.table.save_method).to eq(:TABLE_SAVE_METHOD_INSERT_INTO)
    end
  end

  describe "format helper shortcuts" do
    {
      parquet: "parquet", json: "json", csv: "csv", orc: "orc", text: "text",
    }.each do |method, fmt|
      it "##{method} saves with format=#{fmt}" do
        op = write_op { |w| w.public_send(method, "out") }
        expect(op.source).to eq(fmt)
        expect(op.path).to eq("out")
      end
    end
  end
end

RSpec.describe SparkConnect::DataFrameWriterV2 do
  let(:df) { spark.range(5) }

  def v2_op(&block)
    block.call(df.write_to("cat.db.tbl"))
    cmd = fake_client.last_command
    expect(cmd.command_type).to eq(:write_operation_v2)
    cmd.write_operation_v2
  end

  it "sets table_name, provider and options" do
    op = v2_op { |w| w.using("parquet").option("k", "v").create }
    expect(op.table_name).to eq("cat.db.tbl")
    expect(op.provider).to eq("parquet")
    expect(op.options["k"]).to eq("v")
  end

  {
    create: :MODE_CREATE,
    replace: :MODE_REPLACE,
    create_or_replace: :MODE_CREATE_OR_REPLACE,
    append: :MODE_APPEND,
    overwrite_partitions: :MODE_OVERWRITE_PARTITIONS,
  }.each do |method, mode|
    it "##{method} sets mode #{mode}" do
      op = v2_op { |w| w.public_send(method) }
      expect(op.mode).to eq(mode)
    end
  end

  it "#overwrite sets MODE_OVERWRITE and the condition" do
    op = v2_op { |w| w.overwrite(f.col("id") > 1) }
    expect(op.mode).to eq(:MODE_OVERWRITE)
    expect(op.overwrite_condition.unresolved_function.function_name).to eq(">")
  end

  it "#partition_by sets partitioning_columns expressions" do
    op = v2_op { |w| w.partition_by("a").create }
    expect(op.partitioning_columns.size).to eq(1)
    expect(op.partitioning_columns.first.unresolved_attribute.unparsed_identifier).to eq("a")
  end

  it "#table_property sets table_properties" do
    op = v2_op { |w| w.table_property("owner", "me").create }
    expect(op.table_properties["owner"]).to eq("me")
  end
end
