# frozen_string_literal: true

# Integration specs for Structured Streaming. End-to-end against a live Spark
# Connect server; only run when SPARK_REMOTE is set.
RSpec.describe "Structured Streaming (integration)", :integration, if: ENV.fetch("SPARK_REMOTE", nil) do
  let(:session) { live_session }

  it "reads a streaming source and reports it as streaming" do
    sdf = session.read_stream.format("rate").option("rowsPerSecond", 10).load
    expect(sdf.streaming?).to be(true)
    expect(sdf.schema.names).to include("timestamp", "value")
  end

  it "runs a rate source into a memory sink and observes progress" do
    name = "it_rate_#{Process.pid}"
    query = session.read_stream
                   .format("rate").option("rowsPerSecond", 10).load
                   .write_stream
                   .format("memory").query_name(name).output_mode("append")
                   .trigger(processing_time: "500 milliseconds")
                   .start

    begin
      expect(query.id).to be_a(String)
      expect(query.active?).to be(true)

      # Give the query a couple of triggers to produce data.
      sleep 3

      expect(query.status["isActive"]).to be(true)
      expect(query.recent_progress).to be_an(Array)
      expect(session.streams.active.map(&:id)).to include(query.id)
      expect(session.streams.get(query.id)&.id).to eq(query.id)

      count = session.sql("SELECT count(*) AS c FROM #{name}").collect.first["c"]
      expect(count).to be > 0
    ensure
      query.stop
    end

    expect(query.active?).to be(false)
  end

  it "supports the available-now trigger for a bounded run" do
    name = "it_once_#{Process.pid}"
    query = session.read_stream
                   .format("rate").option("rowsPerSecond", 5).option("numPartitions", 1).load
                   .write_stream
                   .format("memory").query_name(name).trigger(available_now: true)
                   .start
    query.await_termination(15_000)
    query.stop
    expect(query.active?).to be(false)
  end
end
