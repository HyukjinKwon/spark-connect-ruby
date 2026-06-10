# frozen_string_literal: true

require "tmpdir"

# Integration specs for Spark Declarative Pipelines. End-to-end against a live
# Spark Connect server (Spark 4.1+); only run when SPARK_REMOTE is set.
RSpec.describe "Declarative Pipelines (integration)", :integration, if: ENV.fetch("SPARK_REMOTE", nil) do
  let(:session) { live_session }
  let(:f) { SparkConnect::F }

  it "creates a graph, defines chained materialized views, and runs them" do
    pipe = session.pipeline
    expect(pipe.graph_id).to be_a(String)

    src = session.range(10).select(f.col("id"), (f.col("id") % 2).alias("p"))
    pipe.create_materialized_view("scr_it_even", src.filter(f.col("p") == 0))
    pipe.create_materialized_view(
      "scr_it_doubled", pipe.read("scr_it_even").select((f.col("id") * 2).alias("d"))
    )

    Dir.mktmpdir("scr_pipe_it") do |dir|
      events = pipe.start_run(storage: "file://#{dir}", full_refresh_all: true)
      expect(events).not_to be_empty
      expect(events.map(&:message).join("\n")).to include("COMPLETED")
    end

    expect(session.read.table("scr_it_even").count).to eq(5)
    expect(session.read.table("scr_it_doubled").order_by("d").collect.map { |r| r["d"] }).to eq([0, 4, 8, 12, 16])
  ensure
    session.sql("DROP TABLE IF EXISTS scr_it_even").collect
    session.sql("DROP TABLE IF EXISTS scr_it_doubled").collect
  end

  it "validates a graph with a dry run" do
    pipe = session.pipeline
    pipe.create_materialized_view("scr_it_dry", session.range(3))
    Dir.mktmpdir("scr_pipe_dry") do |dir|
      events = pipe.start_run(storage: "file://#{dir}", dry: true)
      expect(events).to be_an(Array)
    end
  end
end
