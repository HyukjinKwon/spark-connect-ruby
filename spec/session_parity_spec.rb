# frozen_string_literal: true

RSpec.describe SparkConnect::SparkSession do
  describe "#empty_data_frame" do
    it "builds an empty local relation with the schema" do
      schema = SparkConnect::Types.struct(SparkConnect::Types.field("a", SparkConnect::Types.integer))
      df = spark.empty_data_frame(schema)
      expect(rel_type(df)).to eq(:local_relation)
      expect(SparkConnect::ArrowConverter.to_rows([rel_body(df).data])).to be_empty
      expect(spark.method(:emptyDataFrame)).to eq(spark.method(:empty_data_frame))
    end
  end

  describe "#new_session / #clone_session" do
    it "derive sessions from the client" do
      fresh = spark.new_session
      expect(fresh.session_id).not_to eq(spark.session_id)
      clone = spark.clone_session("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
      expect(clone.session_id).to eq("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    end
  end

  describe "#stop" do
    it "closes the client, is idempotent, and forgets the active session" do
      described_class.active = spark
      spark.stop
      spark.stop
      expect(spark.stopped?).to be(true)
      expect(spark.is_stopped).to be(true)
      expect(described_class.active).to be_nil
    end
  end

  describe ".active / .get_active_session / .default_session" do
    it "prefers the thread's active session, else the default" do
      a = fake_session(SpecHelpers::FakeClient.new)
      described_class.active = a
      expect(described_class.get_active_session).to equal(a)
      expect(described_class.active).to equal(a)
      Thread.new { expect(described_class.get_active_session).to be_nil }.join
      expect(described_class.getActiveSession).to equal(a)
    end
  end

  describe "#tvf" do
    it "builds UnresolvedTableValuedFunction relations" do
      out = spark.tvf.explode(f.array(f.lit(1)))
      expect(rel_type(out)).to eq(:unresolved_table_valued_function)
      expect(rel_body(out).function_name).to eq("explode")
      expect(rel_body(spark.tvf.sql_keywords).arguments).to be_empty
      expect(rel_body(spark.tvf.stack(f.lit(2), f.lit(1), f.lit(2))).arguments.size).to eq(3)
      expect { spark.tvf.json_tuple(f.lit("{}")) }.to raise_error(SparkConnect::IllegalArgumentError)
      expect(rel_type(spark.tvf.range(3))).to eq(:range)
    end
  end

  describe "progress handlers" do
    it "registers blocks and callables on the client" do
      spark.register_progress_handler { |**| nil }
      h = ->(**_kw) {}
      spark.registerProgressHandler(h)
      expect(fake_client.progress_handlers.size).to eq(2)
      spark.remove_progress_handler(h)
      spark.clear_progress_handlers
      expect(fake_client.progress_handlers).to be_empty
    end
  end

  describe SparkConnect::SparkSession::Builder do
    it "accepts master as a no-op, a config Hash, and appName" do
      b = described_class.new.master("local[*]").config("a" => 1, "b" => 2).appName("x")
      expect(b.instance_variable_get(:@options)).to eq("a" => 1, "b" => 2, "spark.app.name" => "x")
    end

    it "rejects enable_hive_support and remote together with channel_builder" do
      expect { described_class.new.enable_hive_support }.to raise_error(SparkConnect::NotImplementedError)
      b = described_class.new.remote("sc://h").channel_builder(SparkConnect::ChannelBuilder.new("sc://h"))
      expect { b.create }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "uses a custom channel builder" do
      cb = SparkConnect::ChannelBuilder.new("sc://custom-host:1")
      fake = SpecHelpers::FakeClient.new
      expect(SparkConnect::SparkConnectClient).to receive(:new).with(cb).and_return(fake)
      expect(described_class.new.channel_builder(cb).create).to be_a(SparkConnect::SparkSession)
    end

    it "yields the session to a block and stops it afterwards" do
      fake = SpecHelpers::FakeClient.new
      allow(SparkConnect::SparkConnectClient).to receive(:new).and_return(fake)
      value = described_class.new.remote("sc://h").create(&:session_id)
      expect(value).to eq("test-session-id")
      expect(fake.closed?).to be(true)
    end

    it "get_or_create reapplies builder options to an existing session" do
      existing = fake_session(SpecHelpers::FakeClient.new)
      SparkConnect::SparkSession.active = existing
      described_class.new.config("spark.sql.y", "2").get_or_create
      expect(existing.client.config_operations.last.set.pairs.map(&:key)).to eq(["spark.sql.y"])
    end
  end
end
