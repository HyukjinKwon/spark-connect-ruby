# frozen_string_literal: true

require "time"
require "bigdecimal"

# End-to-end regression coverage for bugs fixed in the client. These run against
# a live Spark Connect server and only execute when SPARK_REMOTE is set.
RSpec.describe "client regressions (integration)", :integration, if: ENV.fetch("SPARK_REMOTE", nil) do
  let(:session) { live_session }
  let(:t) { SparkConnect::Types }

  describe "TimestampType is an instant (UTC-normalised) on create_data_frame" do
    # Regression: TimestampType used to be shipped as a zone-less Arrow timestamp,
    # so the server read epoch micros as session-local wall-clock instead of an
    # instant, shifting the value by the session time zone.
    around do |example|
      original = session.conf.get("spark.sql.session.timeZone", "UTC")
      session.conf.set("spark.sql.session.timeZone", "America/New_York")
      example.run
    ensure
      session.conf.set("spark.sql.session.timeZone", original)
    end

    it "preserves the instant regardless of session time zone" do
      schema = t.struct(t.field("ts", t.timestamp))
      df = session.create_data_frame([[Time.utc(2021, 1, 2, 3, 4, 5)]], schema)
      rendered = df.select(f.col("ts").cast("string").alias("s")).collect.first["s"]
      # 2021-01-02T03:04:05Z is 22:04:05 the previous day in America/New_York.
      expect(rendered).to eq("2021-01-01 22:04:05")
    end

    it "keeps TimestampNTZ as wall-clock (no zone shift)" do
      schema = t.struct(t.field("ts", t.timestamp_ntz))
      df = session.create_data_frame([[Time.utc(2021, 1, 2, 3, 4, 5)]], schema)
      rendered = df.select(f.col("ts").cast("string").alias("s")).collect.first["s"]
      expect(rendered).to eq("2021-01-02 03:04:05")
    end
  end

  describe "RuntimeConfig#get with a non-String default" do
    # Regression: a non-String default raised Google::Protobuf::TypeError.
    it "coerces an Integer default and returns a String" do
      value = session.conf.get("spark.sql.shuffle.partitions.absent.xyz", 8)
      expect(value).to eq("8")
    end
  end

  describe "Builder#app_name" do
    # Regression: app_name was stored but never forwarded, so it was a no-op.
    it "forwards spark.app.name to the new session" do
      s = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE")).app_name("ruby-regression").create
      expect(s.conf.get("spark.app.name")).to eq("ruby-regression")
    ensure
      s&.stop
    end
  end

  describe "DataFrame#drop_duplicates_within_watermark" do
    # Regression: this was a plain alias of #drop_duplicates and never set the
    # within_watermark flag. The server must accept the watermark-aware plan.
    it "builds a plan the server accepts on a streaming DataFrame" do
      sdf = session.read_stream.format("rate").option("rowsPerSecond", 1).load
      deduped = sdf.with_watermark("timestamp", "10 seconds").drop_duplicates_within_watermark(%w[value])
      expect(deduped.streaming?).to be(true)
      expect(deduped.columns).to include("value", "timestamp")
    end
  end
end

# rubocop:enable RSpec/SpecFilePathFormat
