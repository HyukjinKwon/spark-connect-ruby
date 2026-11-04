# frozen_string_literal: true

RSpec.describe SparkConnect::Types do
  T = described_class

  {
    T.timestamp_ntz_nanos => "timestamp_ntz(9)",
    T.timestamp_ltz_nanos(7) => "timestamp_ltz(7)",
    T.time(3) => "time(3)",
    T.geometry(4326) => "geometry(4326)",
    T.geography("ANY") => "geography(any)",
  }.each do |type, simple|
    it "round-trips #{simple} through the protobuf DataType" do
      expect(type.simple_string).to eq(simple)
      expect(T.from_proto(type.to_proto)).to eq(type)
    end
  end

  it "defaults omitted precisions like Spark (9 for nanos, 6 for TIME)" do
    expect(T.from_proto(SparkConnect::Proto::DataType.new(timestamp_ntz_nanos: SparkConnect::Proto::DataType::TimestampNTZNanos.new)))
      .to eq(T.timestamp_ntz_nanos(9))
    expect(T.from_proto(SparkConnect::Proto::DataType.new(time: SparkConnect::Proto::DataType::Time.new))).to eq(T.time(6))
  end

  it "validates nanosecond precisions" do
    expect { T.timestamp_ntz_nanos(6) }.to raise_error(SparkConnect::IllegalArgumentError)
    expect { T.timestamp_ltz_nanos(10) }.to raise_error(SparkConnect::IllegalArgumentError)
  end

  it "renders Spark's JSON schema form for spatial types" do
    expect(T.geometry(4326).json_value).to eq("geometry(OGC:CRS84)")
    expect(T.geography(4326).json_value).to eq("geography(OGC:CRS84, SPHERICAL)")
    expect(T.geometry(3857).json_value).to eq("geometry(EPSG:3857)")
  end

  it "decodes user-defined types with their SQL type" do
    udt = SparkConnect::Proto::DataType::UDT.new(type: "udt", jvm_class: "a.B", sql_type: T.long.to_proto)
    t = T.from_proto(SparkConnect::Proto::DataType.new(udt: udt))
    expect([t.class, t.sql_type, t.jvm_class, t.simple_string]).to eq([T::UserDefinedType, T.long, "a.B", "bigint"])
    expect(T.from_proto(t.to_proto)).to eq(t)
  end
end

RSpec.describe SparkConnect::Observation do
  it "raises the server error reported in observed metrics (Spark 4.2+)" do
    obs = described_class.new("m")
    spark.range(3).observe(obs, f.count(f.lit(1)).alias("rows"))
    error = SparkConnect::Proto::FetchErrorDetailsResponse::Error.new(
      error_type_hierarchy: ["org.apache.spark.sql.AnalysisException", "java.lang.Exception"],
      message: "[DIVIDE_BY_ZERO] Division by zero.",
      spark_throwable: SparkConnect::Proto::FetchErrorDetailsResponse::SparkThrowable.new(error_class: "DIVIDE_BY_ZERO",
                                                                                          sql_state: "22012")
    )
    observed = SparkConnect::Proto::ExecutePlanResponse::ObservedMetrics.new(name: "m", root_error_idx: 0, errors: [error])
    result = SparkConnect::SparkConnectClient::ExecuteResult.new([], nil, nil, [observed], nil, 0)
    allow(fake_client).to receive(:execute_plan).and_return(result)
    expect { obs.get }.to raise_error(SparkConnect::AnalysisError) { |e|
      expect([e.error_class, e.sql_state, e.message]).to eq(["DIVIDE_BY_ZERO", "22012", "[DIVIDE_BY_ZERO] Division by zero."])
    }
  end

  it "falls back to a generic error for an invalid root index" do
    observed = SparkConnect::Proto::ExecutePlanResponse::ObservedMetrics.new(name: "m", root_error_idx: 3)
    expect(described_class.error_from(observed)).to be_a(SparkConnect::SparkConnectError)
  end

  it "names unnamed metrics like PySpark" do
    obs = described_class.new("m")
    spark.range(3).observe(obs, f.count(f.lit(1)))
    observed = SparkConnect::Proto::ExecutePlanResponse::ObservedMetrics.new(
      name: "m", values: [SparkConnect::Proto::Expression::Literal.new(long: 3)]
    )
    result = SparkConnect::SparkConnectClient::ExecuteResult.new([], nil, nil, [observed], nil, 0)
    allow(fake_client).to receive(:execute_plan).and_return(result)
    expect(obs.get).to eq("observed_metric_0" => 3)
  end
end

RSpec.describe SparkConnect::Pipeline do
  let(:client) { SpecHelpers::FakeClient.new }
  let(:session) { fake_session(client) }
  let(:pipe) { session.pipeline }

  it "create_streaming_table defines a TABLE output without a flow" do
    pipe.create_streaming_table("t", format: "delta", clustering_columns: %w[k])
    commands = client.executed_commands.map(&:pipeline_command)
    output = commands.find { |c| c.command_type == :define_output }.define_output
    expect([output.output_type, output.table_details.format, output.table_details.clustering_columns.to_a])
      .to eq([:TABLE, "delta", %w[k]])
    expect(commands.map(&:command_type)).not_to include(:define_flow)
  end

  it "create_auto_cdc_flow defines an Auto CDC flow" do
    pipe.create_auto_cdc_flow(target: "t", source: "src", keys: ["id", SparkConnect::F.col("k2")], sequence_by: "ts",
                              apply_as_deletes: "op = 'D'", column_list: %w[id v], stored_as_scd_type: 2,
                              track_history_column_list: %w[v], sql_conf: { "a" => 1 })
    flow = client.last_command.pipeline_command.define_flow
    d = flow.auto_cdc_flow_details
    expect([flow.flow_name, flow.target_dataset_name, flow.sql_conf.to_h]).to eq(["t", "t", { "a" => "1" }])
    expect([d.source, d.keys.size, d.stored_as_scd_type.to_s]).to eq(["src", 2, "SCD_TYPE_2"])
    expect(d.sequence_by.expression_string.expression).to eq("ts")
    expect(d.apply_as_deletes.expression_string.expression).to eq("op = 'D'")
    expect(d.column_list.map { |c| c.unresolved_attribute.unparsed_identifier }).to eq(%w[id v])
    expect(d.track_history_column_list.size).to eq(1)
    expect(flow.has_once?).to be(false)
  end

  it "validates Auto CDC arguments like PySpark" do
    base = { target: "t", source: "s", keys: ["id"], sequence_by: "ts" }
    expect { pipe.create_auto_cdc_flow(**base, column_list: %w[a], except_column_list: %w[b]) }
      .to raise_error(SparkConnect::IllegalArgumentError, /cannot be set together/)
    expect { pipe.create_auto_cdc_flow(**base, track_history_column_list: %w[a]) }
      .to raise_error(SparkConnect::IllegalArgumentError, /stored_as_scd_type 2/)
    expect { pipe.create_auto_cdc_flow(**base, stored_as_scd_type: 3) }.to raise_error(SparkConnect::IllegalArgumentError)
    expect { pipe.create_auto_cdc_flow(**base, keys: "id") }.to raise_error(SparkConnect::IllegalArgumentError)
    expect { pipe.create_auto_cdc_flow(**base, sequence_by: 1) }.to raise_error(SparkConnect::IllegalArgumentError)
    expect { pipe.create_auto_cdc_flow(**base, column_list: [], except_column_list: %w[b]) }.not_to raise_error
  end
end
