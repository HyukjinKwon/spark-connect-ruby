# frozen_string_literal: true

require "securerandom"

module SparkConnect
  # The entry point to programming Spark with the DataFrame API over Spark
  # Connect. Create one with the {Builder}:
  #
  # @example
  #   spark = SparkConnect::SparkSession.builder
  #                                     .remote("sc://localhost:15002")
  #                                     .app_name("my-app")
  #                                     .get_or_create
  #
  # A session owns the underlying {SparkConnectClient}, a monotonic plan-id
  # allocator (so each relation is uniquely identifiable to the server), and the
  # {RuntimeConfig} and {Catalog} facades.
  class SparkSession
    Proto = SparkConnect::Proto

    # @return [SparkConnectClient]
    attr_reader :client

    # @param client [SparkConnectClient]
    def initialize(client)
      @client = client
      @plan_id = -1
      @plan_id_lock = Mutex.new
      @conf = RuntimeConfig.new(client)
      @subquery_relations = {}
      @release_session_on_close = true
    end

    # @api private - whether {#stop} releases the server-side session.
    attr_accessor :release_session_on_close

    @default_session = nil
    @session_lock = Mutex.new
    ACTIVE_SESSION_KEY = :spark_connect_active_session

    class << self
      # @return [Builder] a new session builder.
      def builder
        Builder.new
      end

      # The active session of the current thread, else the process-wide
      # default session (the first session built by {Builder#get_or_create} /
      # {Builder#create}); stopped sessions are skipped.
      #
      # Unlike PySpark's `SparkSession.active()`, this returns nil (instead of
      # raising) when there is no session, which is the idiomatic Ruby shape for
      # an optional lookup.
      #
      # @return [SparkSession, nil]
      def active
        get_active_session || default_session
      end

      # Make `session` the active session of the current thread (and the
      # process-wide default when none is set). `nil` clears the active session.
      # @param session [SparkSession, nil]
      def active=(session)
        Thread.current[ACTIVE_SESSION_KEY] = session
        @session_lock.synchronize { @default_session ||= session } if session
      end

      # @return [SparkSession, nil] the active session of the current thread, if not stopped.
      def get_active_session
        s = Thread.current[ACTIVE_SESSION_KEY]
        s && !s.stopped? ? s : nil
      end
      alias getActiveSession get_active_session

      # @return [SparkSession, nil] the process-wide default session, if not stopped.
      def default_session
        s = @session_lock.synchronize { @default_session }
        s && !s.stopped? ? s : nil
      end

      # Clear the process-wide default session (it is not stopped). @return [void]
      def clear_default_session
        @session_lock.synchronize { @default_session = nil }
        nil
      end

      # Clear the current thread's active session (it is not stopped). @return [void]
      def clear_active_session
        Thread.current[ACTIVE_SESSION_KEY] = nil
      end

      # @api private - forget `session` wherever it is the default/active one.
      def forget(session)
        @session_lock.synchronize { @default_session = nil if @default_session.equal?(session) }
        Thread.current[ACTIVE_SESSION_KEY] = nil if Thread.current[ACTIVE_SESSION_KEY].equal?(session)
      end
    end

    # Allocate the next unique plan id. Used by {PlanBuilder.relation}.
    # @return [Integer]
    def next_plan_id
      @plan_id_lock.synchronize { @plan_id += 1 }
    end

    # @api private - remember a DataFrame's relation so a subquery expression
    #   that refers to it by plan id can be expanded into a `WithRelations`.
    # @param relation [Spark::Connect::Relation]
    # @return [Integer] the relation's plan id.
    def register_subquery_relation(relation)
      plan_id = relation.common.plan_id
      @plan_id_lock.synchronize { @subquery_relations[plan_id] = relation }
      plan_id
    end

    # @api private
    # @return [Spark::Connect::Relation, nil]
    def subquery_relation(plan_id)
      @plan_id_lock.synchronize { @subquery_relations[plan_id] }
    end

    # @return [String] the client session id (UUID).
    def session_id
      @client.session_id
    end

    # Create a {DataFrame} with a single `id` column over the given integer range.
    #
    # @overload range(end_)
    # @overload range(start, end_, step = 1, num_partitions = nil)
    # @return [DataFrame]
    def range(start, end_ = nil, step = 1, num_partitions = nil)
      if end_.nil?
        end_ = start
        start = 0
      end
      r = Proto::Range.new(start: start, end: end_, step: step)
      r.num_partitions = num_partitions if num_partitions
      DataFrame.new(self, PlanBuilder.relation(self, range: r))
    end

    # Execute a SQL query and return a lazy {DataFrame} over its result.
    #
    # @param query [String]
    # @param args [Hash{String=>Object}, Array<Object>, nil] named or positional
    #   parameters bound into the query.
    # @return [DataFrame]
    def sql(query, args = nil)
      sql = Proto::SQL.new(query: query)
      case args
      when Hash
        args.each { |k, v| sql.named_arguments[k.to_s] = Column.to_col(v).to_expr }
      when Array
        sql.pos_arguments += args.map { |v| Column.to_col(v).to_expr }
      end
      DataFrame.new(self, PlanBuilder.relation(self, sql: sql))
    end

    # Return a {DataFrame} reading the named table or view.
    #
    # @param name [String]
    # @return [DataFrame]
    def table(name)
      read.table(name)
    end

    # @return [DataFrameReader] interface for loading external data.
    def read
      DataFrameReader.new(self)
    end

    # @return [DataStreamReader] interface for loading a streaming DataFrame.
    def read_stream
      DataStreamReader.new(self)
    end
    alias readStream read_stream

    # @return [StreamingQueryManager] the manager for this session's streaming queries.
    def streams
      @streams ||= StreamingQueryManager.new(self)
    end

    # @return [TableValuedFunction] built-in table-valued functions
    #   (`spark.tvf.explode(...)`, `spark.tvf.range(...)`, ...).
    def tvf
      TableValuedFunction.new(self)
    end

    # Create a new Spark Declarative Pipeline (a dataflow graph) in this session.
    #
    # @param default_catalog [String, nil]
    # @param default_database [String, nil]
    # @param sql_conf [Hash{String=>String}] SQL configs applied to all flows.
    # @return [Pipeline]
    def pipeline(default_catalog: nil, default_database: nil, sql_conf: {})
      Pipeline.new(self, default_catalog: default_catalog, default_database: default_database, sql_conf: sql_conf)
    end

    # Build a {DataFrame} from local Ruby data.
    #
    # @param data [Array<Hash>, Array<Array>, Array<Row>]
    # @param schema [Types::StructType, Array<String>, String, nil] an explicit
    #   schema, a list of column names, a DDL string, or `nil` to infer.
    # @return [DataFrame]
    def create_data_frame(data, schema = nil)
      data = data.to_a
      struct = resolve_schema(data, schema)
      arrow_bytes = ArrowConverter.from_rows(data, struct)
      local = Proto::LocalRelation.new(data: arrow_bytes, schema: struct.simple_string.sub(/\Astruct</, "").sub(/>\z/, ""))
      DataFrame.new(self, PlanBuilder.relation(self, local_relation: local))
    end
    alias create_dataframe create_data_frame
    alias createDataFrame create_data_frame

    # An empty {DataFrame} with the given schema.
    #
    # @param schema [Types::StructType, String] a schema or DDL string.
    # @return [DataFrame]
    def empty_data_frame(schema)
      create_data_frame([], schema)
    end
    alias emptyDataFrame empty_data_frame

    # @return [RuntimeConfig] runtime configuration facade.
    attr_reader :conf

    # @return [Catalog] the catalog facade (databases, tables, functions, cache).
    def catalog
      @catalog ||= Catalog.new(self)
    end

    # @return [String] the Spark version reported by the server.
    def version
      @client.analyze(spark_version: Proto::AnalyzePlanRequest::SparkVersion.new).spark_version.version
    end

    # Make this the active (and, if unset, the default) session.
    # @return [self]
    def set_active
      SparkSession.active = self
      self
    end

    # Start a brand-new session against the same endpoint: a fresh server-side
    # session with no configuration, temporary views, or functions carried over
    # (Spark Connect `newSession` semantics, like PySpark 4.3). Configurations
    # set through the builder are not reapplied.
    # @return [SparkSession]
    def new_session
      session = SparkSession.new(@client.new_session_client)
      session.release_session_on_close = true
      session
    end
    alias newSession new_session

    # Clone this session on the server: the new session starts with a copy of
    # this session's SQL configurations, temporary views, registered functions,
    # and catalog state, and is isolated from it afterwards (Spark 4.1+).
    #
    # @param new_session_id [String, nil] a UUID for the clone (generated when omitted).
    # @return [SparkSession]
    def clone_session(new_session_id = nil)
      session = SparkSession.new(@client.clone_session(new_session_id))
      session.release_session_on_close = true
      session
    end
    alias cloneSession clone_session

    # Register a callable invoked with execution progress of every query run by
    # this session. It is called with the keywords `stages:` (an Array of
    # {StageInfo}), `inflight_tasks:`, `operation_id:`, and `done:`.
    #
    # @param handler [#call] a Proc/lambda or any object responding to `call`.
    # @return [void]
    def register_progress_handler(handler = nil, &block)
      @client.register_progress_handler(handler || block)
    end
    alias registerProgressHandler register_progress_handler

    # Remove a handler added with {#register_progress_handler}. @return [void]
    def remove_progress_handler(handler)
      @client.remove_progress_handler(handler)
    end
    alias removeProgressHandler remove_progress_handler

    # Remove all progress handlers. @return [void]
    def clear_progress_handlers
      @client.clear_progress_handlers
    end
    alias clearProgressHandlers clear_progress_handlers

    # Interrupt all operations running in this session.
    # @return [Array<String>] the ids of the interrupted operations.
    def interrupt_all
      @client.interrupt(type: :all).interrupted_ids.to_a
    end

    # Interrupt all operations tagged with `tag` (see {#add_tag}).
    # @return [Array<String>]
    def interrupt_tag(tag)
      @client.interrupt(type: :tag, value: tag.to_s).interrupted_ids.to_a
    end

    # Interrupt a single operation by id.
    # @return [Array<String>]
    def interrupt_operation(operation_id)
      @client.interrupt(type: :operation_id, value: operation_id.to_s).interrupted_ids.to_a
    end

    # Add an operation tag applied to all subsequent executions in this session.
    # @return [void]
    def add_tag(tag)
      @client.add_tag(tag)
    end

    # Remove a previously added operation tag. @return [void]
    def remove_tag(tag)
      @client.remove_tag(tag)
    end

    # @return [Array<String>] the currently active operation tags.
    def get_tags
      @client.tags.dup
    end

    # Remove all operation tags. @return [void]
    def clear_tags
      @client.clear_tags
    end

    # Release the server-side session and stop the client. Idempotent.
    # @return [void]
    def stop
      return nil if stopped?

      @streams&.close
      @client.release_session if @release_session_on_close
      @client.close
      SparkSession.forget(self)
      nil
    end

    # @return [Boolean] whether {#stop} has been called.
    def stopped?
      @client.respond_to?(:closed?) && @client.closed?
    end
    alias is_stopped stopped?

    # @api private
    def create_data_frame_from_relation(relation)
      DataFrame.new(self, relation)
    end

    private

    def resolve_schema(data, schema)
      case schema
      when Types::StructType then schema
      when String then parse_ddl_schema(schema)
      when Array then infer_schema(data, names: schema.map(&:to_s))
      when nil then infer_schema(data)
      else
        raise IllegalArgumentError, "Unsupported schema: #{schema.inspect}"
      end
    end

    def parse_ddl_schema(ddl)
      # Ask the server to parse the DDL into a concrete schema.
      proto = @client.analyze(ddl_parse: Proto::AnalyzePlanRequest::DDLParse.new(ddl_string: ddl)).ddl_parse.parsed
      Types.from_proto(proto)
    end

    def infer_schema(data, names: nil)
      raise IllegalArgumentError, "Cannot infer schema from empty data; pass a schema" if data.empty?

      first = data.first
      case first
      when Hash
        keys = first.keys.map(&:to_s)
        Types::StructType.new(keys.map.with_index do |k, i|
          Types::StructField.new(names ? names[i] : k, column_type(data, k, i), nullable: true)
        end)
      when Row
        Types::StructType.new(first.fields.map.with_index do |k, i|
          Types::StructField.new(names ? names[i] : k, column_type(data, k, i), nullable: true)
        end)
      when Array
        Types::StructType.new(first.each_index.map do |i|
          Types::StructField.new(names ? names[i] : "_#{i + 1}", column_type(data, nil, i), nullable: true)
        end)
      else
        raise IllegalArgumentError, "Cannot infer schema from rows of type #{first.class}"
      end
    end

    def column_type(data, key, index)
      sample = data.map { |row| ArrowConverter.extract_value(row, key, index) }.find { |v| !v.nil? }
      Column.infer_type(sample)
    end
  end

  # Fluent builder for {SparkSession}. Returned by {SparkSession.builder}.
  class SparkSession
    class Builder
      # Options that only make sense when connecting and are never applied as
      # session configurations.
      CONNECT_ONLY_OPTIONS = %w[spark.remote spark.master].freeze

      def initialize
        @options = {}
        @remote = nil
        @channel_builder = nil
      end

      # Set the connection string (`sc://...`).
      # @return [self]
      def remote(url)
        @remote = url
        self
      end

      # Use a custom {ChannelBuilder} (e.g. a subclass that customizes the gRPC
      # channel) instead of a connection string.
      # @param channel_builder [ChannelBuilder]
      # @return [self]
      def channel_builder(channel_builder)
        @channel_builder = channel_builder
        self
      end
      alias channelBuilder channel_builder

      # Accepted for PySpark compatibility; the master is chosen by the Spark
      # Connect server, so this is a no-op.
      # @return [self]
      def master(_master)
        self
      end

      # Hive support is a server-side setting and cannot be enabled from a
      # Spark Connect client (PySpark raises too).
      # @raise [NotImplementedError]
      def enable_hive_support
        raise NotImplementedError, "enableHiveSupport is not supported over Spark Connect; configure the server instead"
      end
      alias enableHiveSupport enable_hive_support

      # Set the application name.
      # @return [self]
      def app_name(name)
        @options["spark.app.name"] = name
        self
      end
      alias appName app_name

      # Set an arbitrary configuration option to apply after connecting.
      # @overload config(key, value)
      # @overload config(map)
      #   @param map [Hash] several options at once.
      # @return [self]
      def config(key, value = nil)
        if key.is_a?(Hash)
          key.each { |k, v| @options[k.to_s] = v }
        else
          @options[key.to_s] = value
        end
        self
      end

      # Reuse the active (or default) session, else build a new one. Builder
      # options are applied to the returned session either way, like PySpark.
      # With a block, yields the session and stops it afterwards (the Ruby
      # counterpart of PySpark's `with ... as spark:`), returning the block's value.
      #
      # @yieldparam session [SparkSession]
      # @return [SparkSession, Object]
      def get_or_create(&block)
        session = SparkSession.active
        if session
          apply_options(session)
        else
          session = create
        end
        block ? use(session, &block) : session
      end
      alias getOrCreate get_or_create

      # Always build a new {SparkSession} (which becomes the active session,
      # and the default one when none is set). With a block, behaves like
      # {#get_or_create} with a block.
      #
      # @yieldparam session [SparkSession]
      # @return [SparkSession, Object]
      def create(&block)
        raise IllegalArgumentError, "Set either remote(...) or channel_builder(...), not both" if @remote && @channel_builder

        channel = @channel_builder || ChannelBuilder.new(@remote || ENV["SPARK_REMOTE"] || "sc://localhost:15002")
        session = SparkSession.new(SparkConnectClient.new(channel))
        SparkSession.active = session
        apply_options(session)
        block ? use(session, &block) : session
      end
      alias build create

      private

      def apply_options(session)
        opts = @options.except(*CONNECT_ONLY_OPTIONS)
        session.conf.set_all(opts, silent: true) unless opts.empty?
      end

      def use(session)
        yield session
      ensure
        session.stop
      end
    end
  end
end
