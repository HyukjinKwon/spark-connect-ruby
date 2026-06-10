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

  describe SparkConnect::StreamingQueryManager do
    it "is reachable from the session" do
      expect(session.streams).to be_a(described_class)
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
