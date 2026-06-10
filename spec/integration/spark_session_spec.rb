# frozen_string_literal: true

# Integration specs for {SparkConnect::SparkSession}. These run end-to-end
# against a live Spark Connect server and only execute when SPARK_REMOTE is set.
RSpec.describe SparkConnect::SparkSession, :integration, if: ENV.fetch("SPARK_REMOTE", nil) do
  let(:session) { live_session }

  it "reports the server Spark version" do
    expect(session.version).to be_a(String)
    expect(session.version).to match(/\A\d+\.\d+/)
  end

  it "exposes a session id" do
    expect(session.session_id).to be_a(String)
    expect(session.session_id).not_to be_empty
  end

  describe "#range" do
    it "produces the expected ids" do
      ids = session.range(5).collect.map { |r| r[0] }
      expect(ids).to eq([0, 1, 2, 3, 4])
    end

    it "honours start, end and step" do
      ids = session.range(2, 10, 2).collect.map { |r| r["id"] }
      expect(ids).to eq([2, 4, 6, 8])
    end

    it "counts rows" do
      expect(session.range(10).count).to eq(10)
    end
  end

  describe "#create_data_frame" do
    it "builds a DataFrame from Ruby hashes and collects it" do
      data = [
        { "name" => "alice", "age" => 30 },
        { "name" => "bob", "age" => 25 },
      ]
      df = session.create_data_frame(data)
      rows = df.order_by("name").collect
      expect(rows.map { |r| r["name"] }).to eq(%w[alice bob])
      expect(rows.map { |r| r["age"] }).to eq([30, 25])
    end

    it "supports filtering created data" do
      data = [{ "x" => 1 }, { "x" => 2 }, { "x" => 3 }]
      df = session.create_data_frame(data)
      kept = df.filter(f.col("x") > 1).collect.map { |r| r["x"] }
      expect(kept.sort).to eq([2, 3])
    end
  end

  describe "#sql" do
    it "executes a SQL query and collects the result" do
      rows = session.sql("SELECT 1 AS a, 'hi' AS b").collect
      expect(rows.length).to eq(1)
      expect(rows.first["a"]).to eq(1)
      expect(rows.first["b"]).to eq("hi")
    end

    it "runs a multi-row SQL query deterministically" do
      rows = session.sql("SELECT id FROM range(3) ORDER BY id").collect
      expect(rows.map { |r| r["id"] }).to eq([0, 1, 2])
    end
  end
end

# rubocop:enable RSpec/SpecFilePathFormat
