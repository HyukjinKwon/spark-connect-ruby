# frozen_string_literal: true

RSpec.describe "Structured Streaming" do
  let(:client) { SpecHelpers::FakeClient.new }
  let(:session) { fake_session(client) }

  describe SparkConnect::DataStreamReader do
    it "builds a streaming read relation with format and options" do
      df = session.read_stream.format("rate").option("rowsPerSecond", 5).load
      read = rel_body(df)
      expect(rel_type(df)).to eq(:read)
      expect(read.is_streaming).to be(true)
      expect(read.data_source.format).to eq("rate")
      expect(read.data_source.options["rowsPerSecond"]).to eq("5")
    end

    it "builds a streaming read from a table" do
      read = rel_body(session.read_stream.table("events"))
      expect(read.is_streaming).to be(true)
      expect(read.named_table.unparsed_identifier).to eq("events")
    end

    it "is exposed as readStream too" do
      expect(session.readStream).to be_a(described_class)
    end
  end

  describe SparkConnect::DataStreamWriter do
    let(:sdf) { session.read_stream.format("rate").load }

    it "starts a query and returns a StreamingQuery handle" do
      query = sdf.write_stream.format("console").output_mode("append").start
      expect(query).to be_a(SparkConnect::StreamingQuery)
      op = client.last_command.write_stream_operation_start
      expect(op.format).to eq("console")
      expect(op.output_mode).to eq("append")
    end

    it "encodes the processing-time trigger" do
      sdf.write_stream.format("console").trigger(processing_time: "5 seconds").start
      expect(client.last_command.write_stream_operation_start.processing_time_interval).to eq("5 seconds")
    end

    it "encodes the available-now and once triggers" do
      sdf.write_stream.format("console").trigger(available_now: true).start
      expect(client.last_command.write_stream_operation_start.available_now).to be(true)
      sdf.write_stream.format("console").trigger(once: true).start
      expect(client.last_command.write_stream_operation_start.once).to be(true)
    end

    it "names the query and targets the memory sink" do
      sdf.write_stream.format("memory").query_name("q1").start
      op = client.last_command.write_stream_operation_start
      expect(op.query_name).to eq("q1")
    end

    it "writes to a table via to_table" do
      sdf.write_stream.format("parquet").to_table("db.sink")
      expect(client.last_command.write_stream_operation_start.table_name).to eq("db.sink")
    end

    it "carries the query id and name on the returned handle" do
      query = sdf.write_stream.format("memory").query_name("named").start
      expect(query.id).to eq("test-query-id")
      expect(query.run_id).to eq("test-run-id")
      expect(query.name).to eq("named")
    end
  end

  describe SparkConnect::StreamingQuery do
    let(:instance_id) { SparkConnect::Proto::StreamingQueryInstanceId.new(id: "qid", run_id: "rid") }
    let(:query) { described_class.new(session, instance_id, "myq") }

    def query_result(**kw)
      sqr = SparkConnect::Proto::StreamingQueryCommandResult.new(**kw)
      SparkConnect::SparkConnectClient::ExecuteResult.new.tap { |r| r.streaming_query_result = sqr }
    end

    it "captures the id, run_id and name (blank name becomes nil)" do
      expect(query.id).to eq("qid")
      expect(query.run_id).to eq("rid")
      expect(query.name).to eq("myq")
      expect(described_class.new(session, instance_id, "").name).to be_nil
    end

    it "reports status and active?" do
      status = SparkConnect::Proto::StreamingQueryCommandResult::StatusResult.new(
        status_message: "Waiting", is_data_available: true, is_trigger_active: false, is_active: true
      )
      allow(client).to receive(:execute_command).and_return(query_result(status: status))
      expect(query.status).to eq({
                                   "message" => "Waiting", "isDataAvailable" => true,
                                   "isTriggerActive" => false, "isActive" => true,
                                 })
      expect(query.active?).to be(true)
    end

    it "parses recent_progress and last_progress JSON" do
      rp = SparkConnect::Proto::StreamingQueryCommandResult::RecentProgressResult.new(
        recent_progress_json: ['{"batchId":0}', '{"batchId":1}']
      )
      allow(client).to receive(:execute_command).and_return(query_result(recent_progress: rp))
      expect(query.recent_progress).to eq([{ "batchId" => 0 }, { "batchId" => 1 }])
      expect(query.last_progress).to eq({ "batchId" => 1 })
    end

    it "returns await_termination's terminated flag" do
      at = SparkConnect::Proto::StreamingQueryCommandResult::AwaitTerminationResult.new(terminated: true)
      allow(client).to receive(:execute_command).and_return(query_result(await_termination: at))
      expect(query.await_termination(1000)).to be(true)
    end

    it "process_all_available and stop send commands and return nil" do
      allow(client).to receive(:execute_command).and_return(query_result)
      expect(query.process_all_available).to be_nil
      expect(query.stop).to be_nil
    end

    it "returns nil exception message when empty, the message otherwise" do
      empty = SparkConnect::Proto::StreamingQueryCommandResult::ExceptionResult.new(exception_message: "")
      allow(client).to receive(:execute_command).and_return(query_result(exception: empty))
      expect(query.exception).to be_nil

      boom = SparkConnect::Proto::StreamingQueryCommandResult::ExceptionResult.new(exception_message: "boom")
      allow(client).to receive(:execute_command).and_return(query_result(exception: boom))
      expect(query.exception).to eq("boom")
    end

    it "explains the query plan" do
      ex = SparkConnect::Proto::StreamingQueryCommandResult::ExplainResult.new(result: "== Plan ==")
      allow(client).to receive(:execute_command).and_return(query_result(explain: ex))
      expect(query.explain(extended: true)).to eq("== Plan ==")
    end

    it "has a readable to_s / inspect" do
      expect(query.to_s).to include("id=qid")
      expect(query.inspect).to eq(query.to_s)
    end
  end

  describe SparkConnect::StreamingQueryManager do
    let(:manager) { described_class.new(session) }

    def manager_result(**kw)
      mr = SparkConnect::Proto::StreamingQueryManagerCommandResult.new(**kw)
      SparkConnect::SparkConnectClient::ExecuteResult.new.tap { |r| r.streaming_manager_result = mr }
    end

    def instance(id, name)
      SparkConnect::Proto::StreamingQueryManagerCommandResult::StreamingQueryInstance.new(
        id: SparkConnect::Proto::StreamingQueryInstanceId.new(id: id, run_id: "r-#{id}"), name: name
      )
    end

    it "is reachable from the session" do
      expect(session.streams).to be_a(described_class)
    end

    it "lists active queries" do
      active = SparkConnect::Proto::StreamingQueryManagerCommandResult::ActiveResult.new(
        active_queries: [instance("a", "qa"), instance("b", "qb")]
      )
      allow(client).to receive(:execute_command).and_return(manager_result(active: active))
      queries = manager.active
      expect(queries.map(&:id)).to eq(%w[a b])
      expect(queries.first).to be_a(SparkConnect::StreamingQuery)
    end

    it "gets a query by id when present" do
      result = manager_result(query: instance("a", "qa"))
      allow(client).to receive(:execute_command).and_return(result)
      expect(manager.get("a").id).to eq("a")
    end

    it "returns nil from get when the result is not a query" do
      allow(client).to receive(:execute_command).and_return(manager_result(reset_terminated: true))
      expect(manager.get("missing")).to be_nil
    end

    it "awaits any termination and resets terminated state" do
      at = SparkConnect::Proto::StreamingQueryManagerCommandResult::AwaitAnyTerminationResult.new(terminated: true)
      allow(client).to receive(:execute_command).and_return(manager_result(await_any_termination: at))
      expect(manager.await_any_termination(500)).to be(true)

      allow(client).to receive(:execute_command).and_return(manager_result(reset_terminated: true))
      expect(manager.reset_terminated).to be_nil
    end
  end

  describe "DataFrame streaming helpers" do
    let(:sdf) { session.read_stream.format("rate").load }

    it "applies a watermark" do
      wm = sdf.with_watermark("timestamp", "10 minutes")
      expect(rel_type(wm)).to eq(:with_watermark)
      expect(rel_body(wm).event_time).to eq("timestamp")
      expect(rel_body(wm).delay_threshold).to eq("10 minutes")
    end

    it "range-repartitions with sort-order partition expressions" do
      df = session.range(10).repartition_by_range(4, "id")
      body = rel_body(df)
      expect(rel_type(df)).to eq(:repartition_by_expression)
      expect(body.num_partitions).to eq(4)
      expect(body.partition_exprs.first.expr_type).to eq(:sort_order)
    end

    it "checkpoints into a cached remote relation" do
      df = session.range(5).checkpoint
      expect(rel_type(df)).to eq(:cached_remote_relation)
      expect(rel_body(df).relation_id).to eq("test-relation-id")
      expect(client.last_command.checkpoint_command.local).to be(false)
    end

    it "local-checkpoints with local = true" do
      session.range(5).local_checkpoint
      expect(client.last_command.checkpoint_command.local).to be(true)
    end

    it "transform yields self for fluent chaining" do
      df = session.range(5)
      expect(df.transform { |d| d.limit(2) }).to be_a(SparkConnect::DataFrame)
    end
  end
end
