# frozen_string_literal: true

RSpec.describe SparkConnect::Pipeline do
  let(:client) { SpecHelpers::FakeClient.new }
  let(:session) { fake_session(client) }
  let(:pipe) { session.pipeline(default_database: "db") }

  def last_pc
    client.last_command.pipeline_command
  end

  it "creates a dataflow graph and exposes its id" do
    expect(pipe.graph_id).to eq("graph-1")
    create_cmd = client.executed_commands.first.pipeline_command
    expect(create_cmd.command_type).to eq(:create_dataflow_graph)
    expect(create_cmd.create_dataflow_graph.default_database).to eq("db")
  end

  describe "#create_table" do
    it "defines a TABLE output and a flow with the DataFrame's relation" do
      resolved = pipe.create_table("t", session.range(5), format: "parquet", partition_cols: %w[p])
      expect(resolved).to eq("spark_catalog.default.out")
      commands = client.executed_commands.map(&:pipeline_command)
      output = commands.find { |c| c.command_type == :define_output }
      flow = commands.find { |c| c.command_type == :define_flow }
      expect(output.define_output.output_type).to eq(:TABLE)
      expect(output.define_output.table_details.format).to eq("parquet")
      expect(output.define_output.table_details.partition_cols.to_a).to eq(%w[p])
      expect(flow.define_flow.target_dataset_name).to eq("t")
      expect(flow.define_flow.relation_flow_details.relation).not_to be_nil
    end
  end

  describe "#create_materialized_view" do
    it "defines a MATERIALIZED_VIEW output" do
      pipe.create_materialized_view("mv", session.range(1))
      output = client.executed_commands.map(&:pipeline_command).find { |c| c.command_type == :define_output }
      expect(output.define_output.output_type).to eq(:MATERIALIZED_VIEW)
    end
  end

  describe "#create_temporary_view" do
    it "defines a TEMPORARY_VIEW output" do
      pipe.create_temporary_view("tv", session.range(1))
      output = client.executed_commands.map(&:pipeline_command).find { |c| c.command_type == :define_output }
      expect(output.define_output.output_type).to eq(:TEMPORARY_VIEW)
    end
  end

  describe "#define_flow" do
    it "defines a standalone flow targeting a dataset" do
      pipe.define_flow("f", session.range(3), target: "t", once: true)
      expect(last_pc.command_type).to eq(:define_flow)
      expect(last_pc.define_flow.once).to be(true)
      expect(last_pc.define_flow.target_dataset_name).to eq("t")
    end
  end

  describe "#define_sql" do
    it "registers SQL graph elements" do
      pipe.define_sql("CREATE MATERIALIZED VIEW mv AS SELECT 1")
      expect(last_pc.command_type).to eq(:define_sql_graph_elements)
      expect(last_pc.define_sql_graph_elements.sql_text).to include("MATERIALIZED VIEW")
    end
  end

  describe "#start_run" do
    it "starts the run and returns the streamed events" do
      events = pipe.start_run(full_refresh_all: true, dry: true)
      expect(last_pc.command_type).to eq(:start_run)
      expect(last_pc.start_run.full_refresh_all).to be(true)
      expect(last_pc.start_run.dry).to be(true)
      expect(events.map(&:message)).to eq(["Run started", "Run completed"])
    end

    it "yields each event to a block" do
      seen = []
      pipe.start_run { |e| seen << e.message }
      expect(seen).to eq(["Run started", "Run completed"])
    end
  end

  describe "#drop" do
    it "drops the dataflow graph" do
      pipe.drop
      expect(last_pc.command_type).to eq(:drop_dataflow_graph)
      expect(last_pc.drop_dataflow_graph.dataflow_graph_id).to eq("graph-1")
    end
  end
end
