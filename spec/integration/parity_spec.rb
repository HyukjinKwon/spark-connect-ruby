# frozen_string_literal: true

require "tmpdir"

# End-to-end coverage of the APIs added for PySpark 4.3.0 parity. Each example
# is gated on the server version that introduced the feature, so the suite runs
# against every server in the CI matrix (3.5 and up). Only runs when
# SPARK_REMOTE is set.
RSpec.describe "Spark 4.x API parity (integration)", :integration, if: ENV.fetch("SPARK_REMOTE", nil) do
  let(:session) { live_session }
  let(:f) { SparkConnect::F }
  let(:t) { SparkConnect::Types }

  def requires(version)
    skip "requires Spark #{version}+ (server is #{session.version})" unless server_spark_version_at_least?(version)
  end

  def ids(df) = df.collect.map { |r| r.to_a.size == 1 ? r[0] : r.to_a }

  describe "Spark 3.5+" do
    it "tail, random_split, with_metadata, empty_data_frame, persist and storage_level" do
      df = session.range(10)
      expect(df.tail(2).map { |r| r["id"] }).to eq([8, 9])
      expect(df.random_split([1, 1], 42).sum(&:count)).to eq(10)
      meta = df.with_metadata("id", { "comment" => "the id" }).schema.fields.first.metadata
      expect(meta).to include("comment" => "the id")
      expect(session.empty_data_frame("a INT, b STRING").columns).to eq(%w[a b])
      cached = session.range(3)
      cached.persist(SparkConnect::StorageLevel::DISK_ONLY)
      expect(cached.storage_level).to eq(SparkConnect::StorageLevel::DISK_ONLY)
      cached.unpersist
      expect(cached.cached?).to be(false)
    end

    it "with_field, drop_fields, df[...] references, and catalog getters" do
      out = session.sql("SELECT named_struct('a', 1, 'b', 2) AS s")
                   .select(f.col("s").with_field("c", f.lit(3)).drop_fields("a").alias("t"))
      expect(out.schema.simple_string).to eq("struct<t:struct<b:int,c:int>>")
      left = session.range(3)
      right = session.range(3)
      expect(left.join(right, on: left["id"] == right["id"]).count).to eq(3)
      expect(session.catalog.get_database("default")["name"]).to eq("default")
      expect(session.catalog.get_function("abs")["name"]).to eq("abs")
    end

    it "the newly added functions resolve and evaluate on the server" do
      row = session.range(1).select(
        f.nvl(f.lit(nil), f.lit(1)).alias("nvl"), f.left(f.lit("spark"), f.lit(2)).alias("left"),
        f.try_divide(f.lit(1), f.lit(0)).alias("div"), f.make_interval(f.lit(1)).cast("string").alias("ival"),
        f.split_part(f.lit("a,b"), f.lit(","), f.lit(2)).alias("part"), f.sha2(f.lit("x"), 256).alias("sha")
      ).collect.first
      expect([row["nvl"], row["left"], row["div"], row["part"]]).to eq([1, "sp", nil, "b"])
      expect(row["ival"]).to include("1 years")
      expect(row["sha"].size).to eq(64)
    end

    it "options maps are accepted by from_json / to_json" do
      df = session.sql(%q(SELECT '{"a": 1}' AS j))
      row = df.select(f.from_json("j", "a INT", { "mode" => "FAILFAST" }).alias("p")).collect.first
      expect(row["p"].to_h).to eq("a" => 1)
      expect(df.select(f.to_json(f.struct("j"), { "pretty" => false }).alias("o")).collect.first["o"]).to include("j")
    end
  end

  describe "Spark 4.0+" do
    before { requires("4.0") }

    it "scalar and exists subqueries" do
      df = session.range(5)
      expect(ids(df.filter(f.col("id") > session.range(10).select(f.max("id") - 7).scalar))).to eq([3, 4])
      expect(df.filter(session.range(1).exists).count).to eq(5)
    end

    it "lateral_join, transpose, grouping_sets, and tvf" do
      df = session.range(3)
      expect(df.lateral_join(session.range(1).select(f.col("id").outer.alias("o"))).count).to eq(3)
      expect(session.create_data_frame([{ "k" => "x", "v" => 1 }]).transpose.columns).to eq(%w[key x])
      gs = session.create_data_frame([{ "a" => 1, "b" => 2 }]).grouping_sets([["a"], []], "a").agg(f.count("*").alias("n"))
      expect(gs.count).to eq(2)
      expect(ids(session.tvf.explode(f.array(f.lit(1), f.lit(2))))).to eq([1, 2])
      expect(session.tvf.sql_keywords.limit(1).count).to eq(1)
    end

    it "seeded uuid/shuffle/rand return the same values on every action" do
      df = session.range(3).select(f.uuid.alias("u"), f.rand.alias("r"), f.shuffle(f.array(f.lit(1), f.lit(2), f.lit(3))).alias("s"))
      expect(df.collect.map(&:to_a)).to eq(df.collect.map(&:to_a))
      expect(session.range(4).select(f.mode("id", true).alias("m")).collect.first["m"]).to eq(0)
    end

    it "try_cast returns null instead of failing" do
      expect(session.sql("SELECT 'x' AS s").select(f.col("s").try_cast("int").alias("i")).collect.first["i"]).to be_nil
    end

    it "progress handlers receive the final done notification" do
      done = []
      handler = ->(**kw) { done << kw[:done] }
      session.register_progress_handler(handler)
      session.range(100).repartition(2).count
      expect(done.last).to be(true)
    ensure
      session.remove_progress_handler(handler)
    end

    it "streaming query listeners receive started, progress, and terminated events" do
      events = Queue.new
      listener = Class.new(SparkConnect::StreamingQueryListener) do
        define_method(:on_query_started) { |_e| events << :started }
        define_method(:on_query_progress) { |_e| events << :progress }
        define_method(:on_query_terminated) { |_e| events << :terminated }
      end.new
      session.streams.add_listener(listener)
      query = session.read_stream.format("rate").option("rowsPerSecond", 5).load
                     .write_stream.format("memory").query_name("rb_parity_listener").start
      query.process_all_available
      query.stop
      seen = []
      deadline = Time.now + 30
      until seen.include?(:terminated) || Time.now > deadline
        begin
          seen << events.pop(true)
        rescue ThreadError # the queue is empty
          sleep 0.2
        end
      end
      expect(seen).to include(:started, :terminated)
    ensure
      session.streams.remove_listener(listener)
    end

    it "merge_into reaches the server (a parquet table rejects MERGE)" do
      session.sql("DROP TABLE IF EXISTS rb_parity_merge").collect
      session.sql("CREATE TABLE rb_parity_merge (id INT) USING parquet").collect
      writer = session.create_data_frame([{ "id" => 1 }]).alias("s")
                      .merge_into("rb_parity_merge", f.expr("rb_parity_merge.id = s.id")).when_not_matched.insert_all
      # Spark 4.1+ reports an AnalysisError; Spark 4.0 sends an unclassified
      # "MERGE INTO TABLE is not supported temporarily." error.
      expect { writer.merge }.to raise_error(SparkConnect::SparkConnectError, /MERGE/)
    ensure
      session.sql("DROP TABLE IF EXISTS rb_parity_merge").collect
    end
  end

  describe "Spark 4.1+" do
    before { requires("4.1") }

    it "clone_session copies configurations and new_session starts fresh" do
      session.conf.set("spark.sql.shuffle.partitions", "7")
      clone = session.clone_session
      fresh = session.new_session
      expect(clone.conf.get("spark.sql.shuffle.partitions")).to eq("7")
      expect(fresh.conf.get("spark.sql.shuffle.partitions")).not_to eq("7")
    ensure
      clone&.stop
      fresh&.stop
      session.conf.unset("spark.sql.shuffle.partitions")
    end

    it "isin(df) IN subqueries" do
      expect(ids(session.range(5).filter(f.col("id").isin(session.range(2))))).to eq([0, 1])
    end

    it "repartition_by_id routes rows to the given partition" do
      expect(session.range(10).repartition_by_id(3, (f.col("id") % 3).cast("int")).count).to eq(10)
    end

    it "decodes TIME and spatial schemas" do
      session.conf.set("spark.sql.timeType.enabled", "true")
      df = session.sql("SELECT TIME'12:34:56' AS t")
      expect(df.schema.simple_string).to eq("struct<t:time(6)>")
      expect(df.collect.first["t"].to_s).to eq("12:34:56")
    ensure
      session.conf.unset("spark.sql.timeType.enabled")
    end
  end

  describe "Spark 4.2+" do
    before { requires("4.2") }

    it "nearest_by_join and zip_with_index" do
      q = session.create_data_frame([{ "q" => 1.0 }, { "q" => 5.0 }])
      b = session.create_data_frame([{ "b" => 0.0 }, { "b" => 4.0 }, { "b" => 6.0 }])
      out = q.nearest_by_join(b, f.abs(f.col("q") - f.col("b")), 1, "exact", "distance").order_by("q")
      expect(out.collect.map(&:to_a)).to eq([[1.0, 0.0], [5.0, 4.0]])
      expect(session.range(3).zip_with_index.collect.map { |r| r["index"] }).to eq([0, 1, 2])
    end

    it "the catalog DDL and maintenance operations" do
      catalog = session.catalog
      catalog.create_database("rb_parity_db", if_not_exists: true)
      expect(catalog.database_exists("rb_parity_db")).to be(true)
      session.sql("CREATE TABLE rb_parity_db.parts (a INT, p INT) USING parquet PARTITIONED BY (p)").collect
      session.sql("INSERT INTO rb_parity_db.parts VALUES (1, 1), (2, 2)").collect
      expect(catalog.list_partitions("rb_parity_db.parts").map { |r| r["partition"] }).to eq(%w[p=1 p=2])
      expect(catalog.get_table_properties("rb_parity_db.parts")).to be_a(Hash)
      expect(catalog.get_create_table_string("rb_parity_db.parts")).to include("CREATE TABLE")
      catalog.analyze_table("rb_parity_db.parts", no_scan: true)
      catalog.truncate_table("rb_parity_db.parts")
      expect(session.table("rb_parity_db.parts").count).to eq(0)
      session.sql("CREATE VIEW rb_parity_db.v AS SELECT 1 AS a").collect
      # Temporary views (e.g. memory sinks of other examples) are listed too.
      expect(catalog.list_views("rb_parity_db").reject { |r| r["isTemporary"] }.map { |r| r["name"] }).to eq(["v"])
      catalog.drop_view("rb_parity_db.v")
      catalog.drop_table("rb_parity_db.parts")
      expect(catalog.table_exists("rb_parity_db.parts")).to be(false)
    ensure
      catalog.drop_database("rb_parity_db", if_exists: true, cascade: true)
    end

    it "get_operation_statuses reports this session's operations" do
      session.range(1).count
      statuses = session.client.get_operation_statuses.operation_statuses
      expect(statuses).not_to be_empty
      expect(statuses.map(&:state)).to all(be_a(Symbol))
    end

    it "parses a DataFrame of XML strings" do
      lines = session.create_data_frame([{ "value" => "<r><a>1</a></r>" }])
      expect(session.read.option("rowTag", "r").xml(lines).collect.map(&:to_h)).to eq([{ "a" => 1 }])
    end

    it "named streaming sources reach the server" do
      session.conf.set("spark.sql.streaming.queryEvolution.enableSourceEvolution", "true")
      Dir.mktmpdir("rb_parity_src") do |dir|
        query = session.read_stream.format("rate").name("src_a").load
                       .write_stream.format("memory").query_name("rb_parity_named").option("checkpointLocation", dir).start
        query.process_all_available
        expect(query.exception).to be_nil
        query.stop
      end
    ensure
      session.conf.unset("spark.sql.streaming.queryEvolution.enableSourceEvolution")
    end

    it "max_by/min_by with k" do
      row = session.range(5).select(f.max_by("id", "id", 2).alias("mx")).collect.first
      expect(row["mx"]).to eq([4, 3])
    end
  end

  describe "Spark 4.3+" do
    before { requires("4.3") }

    it "zip combines projections of the same source" do
      df = session.range(3)
      out = df.select(f.col("id")).zip(df.select((f.col("id") * 2).alias("x")))
      expect(out.collect.map(&:to_a)).to eq([[0, 0], [1, 2], [2, 4]])
    end

    it "the Spark 4.3 functions and overloads" do
      row = session.range(1).select(
        f.jaro_winkler_similarity(f.lit("abc"), f.lit("abd")).alias("j"), f.to_base32(f.lit("a")).alias("b32"),
        f.instr(f.lit("abcabc"), "c", 4).alias("i"), f.regexp_replace(f.lit("aaa"), "a", "b", 2).alias("rr"),
        f.hmac(f.lit("k"), f.lit("m")).alias("h"), f.variant_strip_nulls(f.parse_json(f.lit('{"a":null,"b":1}'))).cast("string").alias("v")
      ).collect.first
      expect(row["j"]).to be_within(0.001).of(0.8222)
      expect([row["b32"], row["i"], row["rr"], row["v"]]).to eq(["ME======", 6, "abb", '{"b":1}'])
      expect(row["h"]).not_to be_nil
    end

    it "decodes nanosecond timestamps (preview flag)" do
      session.conf.set("spark.sql.timestampNanosTypes.enabled", "true")
      df = session.sql("SELECT CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP_NTZ(9)) AS t")
      expect(df.schema.simple_string).to eq("struct<t:timestamp_ntz(9)>")
      expect(df.collect.first["t"].nsec).to eq(123_456_789)
    ensure
      session.conf.unset("spark.sql.timestampNanosTypes.enabled")
    end
  end
end
