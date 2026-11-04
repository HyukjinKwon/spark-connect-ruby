# frozen_string_literal: true

# Catalog, reader/writer, and streaming APIs added for PySpark 4.3.0 parity.
RSpec.describe "Spark 4.2/4.3 catalog and I/O APIs" do
  let(:client) { SpecHelpers::FakeClient.new }
  let(:session) { fake_session(client) }
  let(:t) { SparkConnect::Types }

  def cat(&) = yield(session.catalog).then { client.last_relation.catalog }

  describe SparkConnect::Catalog do
    it "builds the Spark 4.2 DDL / maintenance commands" do
      expect(cat { |c| c.drop_table("t", if_exists: true, purge: true) }.drop_table.to_h)
        .to eq(table_name: "t", if_exists: true, purge: true)
      expect(cat { |c| c.drop_view("v") }.drop_view.view_name).to eq("v")
      cd = cat { |c| c.create_database("db", if_not_exists: true, properties: { "comment" => "x" }) }.create_database
      expect([cd.db_name, cd.if_not_exists, cd.properties.to_h]).to eq(["db", true, { "comment" => "x" }])
      expect(cat { |c| c.drop_database("db", cascade: true) }.drop_database.cascade).to be(true)
      expect(cat { |c| c.truncate_table("t") }.truncate_table.table_name).to eq("t")
      expect(cat { |c| c.analyze_table("t", no_scan: true) }.analyze_table.no_scan).to be(true)
      expect(cat { |c| c.refresh_by_path("/p") }.refresh_by_path.path).to eq("/p")
      cache = cat { |c| c.cache_table("t", SparkConnect::StorageLevel::DISK_ONLY) }.cache_table
      expect(cache.storage_level.use_disk).to be(true)
    end

    it "decodes list_partitions, get_table_properties, and get_create_table_string" do
      client.schema = t.struct(t.field("partition", t.string))
      client.rows = [{ "partition" => "p=1" }]
      expect(session.catalog.list_partitions("t").map { |r| r["partition"] }).to eq(["p=1"])
      client.schema = t.struct(t.field("key", t.string), t.field("value", t.string))
      client.rows = [{ "key" => "k", "value" => "v" }]
      expect(session.catalog.get_table_properties("t")).to eq("k" => "v")
      client.schema = t.struct(t.field("ddl", t.string))
      client.rows = [{ "ddl" => "CREATE TABLE t" }]
      expect(session.catalog.get_create_table_string("t", as_serde: true)).to eq("CREATE TABLE t")
      expect(client.last_relation.catalog.get_create_table_string.as_serde).to be(true)
    end

    it "lists views (defaulting the database when a pattern is given) and takes patterns elsewhere" do
      client.schema = t.struct(t.field("name", t.string))
      client.rows = [{ "name" => "default" }]
      lv = cat { |c| c.list_views("db", "v*") }.list_views
      expect([lv.db_name, lv.pattern]).to eq(["db", "v*"])
      expect(cat { |c| c.list_views(nil, "v*") }.list_views.db_name).to eq("default")
      expect(cat { |c| c.list_tables(nil, "t*") }.list_tables.pattern).to eq("t*")
      expect(cat { |c| c.list_databases("d*") }.list_databases.pattern).to eq("d*")
      expect(cat { |c| c.list_catalogs("c*") }.list_catalogs.pattern).to eq("c*")
      expect(cat { |c| c.list_functions("db", "f*") }.list_functions.pattern).to eq("f*")
    end

    it "gets a database, table, or function as a Row" do
      client.schema = t.struct(t.field("name", t.string))
      client.rows = [{ "name" => "x" }]
      expect(session.catalog.get_database("db")["name"]).to eq("x")
      expect(client.last_relation.catalog.get_database.db_name).to eq("db")
      session.catalog.get_table("t")
      expect(client.last_relation.catalog.get_table.table_name).to eq("t")
      session.catalog.get_function("f")
      expect(client.last_relation.catalog.get_function.function_name).to eq("f")
    end

    it "has the PySpark camelCase aliases" do
      %i[dropTable dropView createDatabase dropDatabase listPartitions listViews getTableProperties
         getCreateTableString truncateTable analyzeTable getTable getDatabase getFunction isCached
         refreshByPath listTables].each { |m| expect(session.catalog).to respond_to(m) }
    end
  end

  describe SparkConnect::DataFrameReader do
    it "changes builds a RelationChanges with the reader options" do
      df = session.read.option("startingVersion", 1).changes("t")
      rc = rel_body(df)
      expect(rel_type(df)).to eq(:relation_changes)
      expect([rc.unparsed_identifier, rc.options.to_h, rc.is_streaming]).to eq(["t", { "startingVersion" => "1" }, false])
      expect { session.read.schema("a INT").changes("t") }.to raise_error(SparkConnect::AnalysisError)
    end

    it "parses a DataFrame of JSON/CSV/XML strings with a Parse relation" do
      lines = session.range(1)
      schema = t.struct(t.field("a", t.integer))
      { json: :PARSE_FORMAT_JSON, csv: :PARSE_FORMAT_CSV, xml: :PARSE_FORMAT_XML }.each do |fmt, pf|
        out = session.read.schema(schema).option("k", "v").public_send(fmt, lines)
        body = rel_body(out)
        expect(rel_type(out)).to eq(:parse)
        expect([body.format, body.input, body.options.to_h]).to eq([pf, lines.relation, { "k" => "v" }])
        expect(SparkConnect::Types.from_proto(body.schema)).to eq(schema)
      end
      ddl = rel_body(session.read.schema("a INT").json(lines))
      expect(ddl.schema).not_to be_nil # resolved through the server's DDL parser
    end

    it "reads XML files and partitioned/predicated JDBC" do
      expect(rel_body(session.read.option("rowTag", "r").xml("a.xml")).data_source.format).to eq("xml")
      part = rel_body(session.read.jdbc("jdbc:x", "t", { "user" => "u" }, column: "id", lower_bound: 0,
                                                                          upper_bound: 9, num_partitions: 3))
      expect(part.data_source.options.to_h).to include("partitionColumn" => "id", "numPartitions" => "3", "user" => "u")
      pred = rel_body(session.read.jdbc("jdbc:x", "t", predicates: ["a < 1", "a >= 1"]))
      expect(pred.data_source.predicates.to_a).to eq(["a < 1", "a >= 1"])
      expect { session.read.jdbc("jdbc:x", "t", column: "id") }.to raise_error(SparkConnect::IllegalArgumentError)
    end
  end

  describe SparkConnect::DataFrameWriter do
    let(:df) { session.range(1) }

    def op = client.last_command.write_operation

    it "sets clustering columns and schema evolution, and writes XML / JDBC" do
      df.write.cluster_by("a", "b").with_schema_evolution.save("p")
      expect([op.clustering_columns.to_a, op.with_schema_evolution]).to eq([%w[a b], true])
      df.write.option("rowTag", "r").xml("out")
      expect([op.source, op.path]).to eq(%w[xml out])
      df.write.jdbc("jdbc:x", "t", :append, "user" => "u")
      expect(op.options.to_h).to include("url" => "jdbc:x", "dbtable" => "t", "user" => "u")
      expect(op.mode).to eq(:SAVE_MODE_APPEND)
      expect { df.write.cluster_by }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "ignores nil option values like PySpark" do
      df.write.option("a", nil).options("b" => 1).save
      expect(op.options.to_h).to eq("b" => "1")
    end
  end

  describe SparkConnect::DataFrameWriterV2 do
    it "sets clustering, options, partition transforms, and schema evolution" do
      session.range(1).write_to("t").partitioned_by(SparkConnect::F.years("ts"), "p").cluster_by("c")
             .options("k" => "v").with_schema_evolution.create_or_replace
      op = client.last_command.write_operation_v2
      expect(op.partitioning_columns.first.unresolved_function.function_name).to eq("years")
      expect([op.clustering_columns.to_a, op.options.to_h, op.with_schema_evolution]).to eq([%w[c], { "k" => "v" }, true])
      expect(session.range(1).write_to("t")).to respond_to(:partitionedBy, :clusterBy, :withSchemaEvolution)
    end
  end

  describe SparkConnect::DataStreamReader do
    it "names the source, validating the name" do
      ds = rel_body(session.read_stream.format("rate").name("src_1").load).data_source
      expect(ds.source_name).to eq("src_1")
      expect { session.read_stream.name("bad-name") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { session.read_stream.name(:sym) }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "changes builds a streaming RelationChanges" do
      rc = rel_body(session.read_stream.option("startingVersion", 2).changes("t"))
      expect([rc.unparsed_identifier, rc.is_streaming]).to eq(["t", true])
    end

    it "sends a StructType schema as JSON and supports XML" do
      schema = t.struct(t.field("a", t.integer))
      ds = rel_body(session.read_stream.schema(schema).xml("dir")).data_source
      expect([ds.format, JSON.parse(ds.schema)]).to eq(["xml", JSON.parse(schema.json)])
    end
  end

  describe SparkConnect::DataStreamWriter do
    let(:sdf) { session.read_stream.format("rate").load }

    def start_op = client.last_command.write_stream_operation_start

    it "supports the real-time trigger and clustering" do
      sdf.write_stream.format("memory").trigger(real_time: " 5 minutes ").cluster_by("a").start
      expect(start_op.real_time_batch_duration).to eq("5 minutes")
      expect(start_op.clustering_column_names.to_a).to eq(%w[a])
    end

    it "validates the trigger like PySpark" do
      w = sdf.write_stream
      expect { w.trigger }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { w.trigger(once: true, real_time: "1 second") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { w.trigger(real_time: " ") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { w.trigger(once: false) }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "start/to_table accept the PySpark keyword shortcuts" do
      sdf.write_stream.start("out", format: "parquet", output_mode: "append", query_name: "q", checkpointLocation: "/c")
      expect([start_op.format, start_op.output_mode, start_op.query_name, start_op.options["checkpointLocation"]])
        .to eq(%w[parquet append q /c])
    end

    it "rejects foreach / foreach_batch" do
      expect { sdf.write_stream.foreach(->(r) { r }) }.to raise_error(SparkConnect::NotImplementedError)
      expect { sdf.write_stream.foreachBatch(->(b, i) { [b, i] }) }.to raise_error(SparkConnect::NotImplementedError)
    end
  end

  describe SparkConnect::StreamingQueryListener do
    let(:events) { [] }
    let(:listener) do
      log = events
      Class.new(described_class) do
        define_method(:on_query_started) { |e| log << [:started, e.name, e.job_tags] }
        define_method(:onQueryTerminated) { |e| log << [:terminated, e.error_class_on_exception] }
      end.new
    end

    it "builds events from the server JSON and dispatches snake_case or camelCase callbacks" do
      started = described_class.event_from_json(:started, { id: "1", runId: "2", name: "q", timestamp: "t", jobTags: ["x"] }.to_json)
      described_class.dispatch(listener, started)
      term = described_class.event_from_json(:terminated, { id: "1", runId: "2", exception: "e", errorClassOnException: "E" }.to_json)
      described_class.dispatch(listener, term)
      described_class.dispatch(listener, described_class.event_from_json(:idle, { id: "1", runId: "2", timestamp: "t" }.to_json))
      expect(events).to eq([[:started, "q", ["x"]], [:terminated, "E"]])
    end

    it "registers a server-side listener and streams events through the bus" do
      ev = SparkConnect::Proto::StreamingQueryListenerEvent.new(
        event_type: :QUERY_TERMINATED_EVENT, event_json: { id: "1", runId: "2", exception: nil, errorClassOnException: nil }.to_json
      )
      responses = [
        SparkConnect::Proto::ExecutePlanResponse.new(streaming_query_listener_events_result:
          SparkConnect::Proto::StreamingQueryListenerEventsResult.new(listener_bus_listener_added: true)),
        SparkConnect::Proto::ExecutePlanResponse.new(streaming_query_listener_events_result:
          SparkConnect::Proto::StreamingQueryListenerEventsResult.new(events: [ev])),
      ]
      client.define_singleton_method(:stream_command) do |cmd, &blk|
        @streamed = cmd
        responses.each(&blk)
      end
      session.streams.add_listener(listener)
      deadline = Time.now + 10
      sleep 0.01 until events.any? || Time.now > deadline
      expect(events).to eq([[:terminated, nil]])
      session.streams.remove_listener(listener)
      expect(client.last_command.streaming_query_listener_bus_command.remove_listener_bus_listener).to be(true)
      expect(session.streams.listeners).to be_empty
    end

    it "posts the QueryStartedEvent returned by start" do
      session.streams.instance_variable_get(:@listener_bus).instance_variable_get(:@listeners) << listener
      json = { id: "1", runId: "2", name: "q", timestamp: "t" }.to_json
      allow(client).to receive(:execute_command).and_wrap_original do |m, cmd|
        m.call(cmd).tap { |r| r.write_stream_result&.query_started_event_json = json }
      end
      session.read_stream.format("rate").load.write_stream.format("memory").start
      expect(events).to eq([[:started, "q", []]])
    end
  end
end
