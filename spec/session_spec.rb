# frozen_string_literal: true

RSpec.describe SparkConnect::SparkSession do
  let(:client) { SpecHelpers::FakeClient.new }
  let(:session) { fake_session(client) }

  describe "#next_plan_id" do
    it "starts at 0 and increments monotonically" do
      expect(session.next_plan_id).to eq(0)
      expect(session.next_plan_id).to eq(1)
      expect(session.next_plan_id).to eq(2)
    end
  end

  describe "#session_id" do
    it "delegates to the client" do
      expect(session.session_id).to eq("test-session-id")
    end
  end

  describe "#range" do
    it "builds a range relation from a single end argument" do
      df = session.range(10)
      expect(rel_type(df)).to eq(:range)
      r = rel_body(df)
      expect(r.start).to eq(0)
      expect(r.end).to eq(10)
      expect(r.step).to eq(1)
      expect(r.has_num_partitions?).to be(false)
    end

    it "builds a range relation from start, end, step, num_partitions" do
      df = session.range(5, 20, 3, 4)
      r = rel_body(df)
      expect(r.start).to eq(5)
      expect(r.end).to eq(20)
      expect(r.step).to eq(3)
      expect(r.has_num_partitions?).to be(true)
      expect(r.num_partitions).to eq(4)
    end

    it "assigns a fresh plan id via the common message" do
      df = session.range(3)
      expect(df.relation.common.plan_id).to eq(0)
      df2 = session.range(3)
      expect(df2.relation.common.plan_id).to eq(1)
    end
  end

  describe "#sql" do
    it "builds an sql relation with no args" do
      df = session.sql("SELECT 1")
      expect(rel_type(df)).to eq(:sql)
      expect(rel_body(df).query).to eq("SELECT 1")
    end

    it "binds a Hash of args as named_arguments" do
      df = session.sql("SELECT :x", { x: 7 })
      sql = rel_body(df)
      expect(sql.named_arguments.keys).to eq(["x"])
      expect(sql.named_arguments["x"]).to be_a(SparkConnect::Proto::Expression)
      expect(sql.named_arguments["x"].literal.integer).to eq(7)
    end

    it "binds an Array of args as pos_arguments" do
      df = session.sql("SELECT ?, ?", [1, 2])
      sql = rel_body(df)
      expect(sql.pos_arguments.size).to eq(2)
      expect(sql.pos_arguments.map { |e| e.literal.integer }).to eq([1, 2])
    end
  end

  describe "#read" do
    it "returns a DataFrameReader" do
      expect(session.read).to be_a(SparkConnect::DataFrameReader)
    end
  end

  describe "#create_data_frame" do
    it "infers a schema from an array of hashes and builds a local_relation" do
      df = session.create_data_frame([{ "a" => 1, "b" => "x" }, { "a" => 2, "b" => "y" }])
      expect(rel_type(df)).to eq(:local_relation)
      local = rel_body(df)
      expect(local.schema).to include("a", "b")
      expect(local.data).not_to be_empty
    end

    it "accepts an explicit StructType" do
      st = SparkConnect::Types.struct(
        SparkConnect::Types.field("n", SparkConnect::Types.long)
      )
      df = session.create_data_frame([{ "n" => 1 }], st)
      expect(rel_type(df)).to eq(:local_relation)
      expect(rel_body(df).schema).to include("n")
    end

    it "accepts an Array of column names" do
      df = session.create_data_frame([[1, 2]], %w[x y])
      expect(rel_type(df)).to eq(:local_relation)
      expect(rel_body(df).schema).to include("x", "y")
    end

    it "raises on empty data without a schema" do
      expect { session.create_data_frame([]) }
        .to raise_error(SparkConnect::IllegalArgumentError, /empty data/)
    end

    it "is aliased as createDataFrame" do
      expect(session.method(:createDataFrame)).to eq(session.method(:create_data_frame))
    end
  end

  describe "#conf" do
    it "returns a RuntimeConfig" do
      expect(session.conf).to be_a(SparkConnect::RuntimeConfig)
    end
  end

  describe "#catalog" do
    it "returns a memoized Catalog" do
      expect(session.catalog).to be_a(SparkConnect::Catalog)
      expect(session.catalog).to equal(session.catalog)
    end
  end

  describe "#version" do
    it "issues a spark_version analyze and returns the version" do
      client.spark_version = "4.1.0"
      expect(session.version).to eq("4.1.0")
      expect(client.analyze_requests.last.keys).to eq([:spark_version])
    end
  end

  describe SparkConnect::SparkSession::Builder do
    after { SparkConnect::SparkSession.active = nil }

    it "is returned by .builder" do
      expect(SparkConnect::SparkSession.builder).to be_a(described_class)
    end

    it "supports fluent remote/app_name/config" do
      b = described_class.new
      expect(b.remote("sc://h:1/")).to equal(b)
      expect(b.app_name("app")).to equal(b)
      expect(b.config("k", "v")).to equal(b)
    end

    it "create builds a fresh session each time" do
      s1 = described_class.new.remote("sc://localhost:15002").create
      s2 = described_class.new.remote("sc://localhost:15002").create
      expect(s1).to be_a(SparkConnect::SparkSession)
      expect(s2).to be_a(SparkConnect::SparkSession)
      expect(s1).not_to equal(s2)
    end

    it "get_or_create returns the active session if present" do
      active = fake_session(client)
      SparkConnect::SparkSession.active = active
      built = described_class.new.remote("sc://localhost:15002").get_or_create
      expect(built).to equal(active)
    end

    it "get_or_create sets and returns a new active session when none exists" do
      SparkConnect::SparkSession.active = nil
      built = described_class.new.remote("sc://localhost:15002").get_or_create
      expect(built).to be_a(SparkConnect::SparkSession)
      expect(SparkConnect::SparkSession.active).to equal(built)
    end

    it "aliases getOrCreate and build" do
      b = described_class.new
      expect(b.method(:getOrCreate)).to eq(b.method(:get_or_create))
      expect(b.method(:build)).to eq(b.method(:create))
    end
  end
end
