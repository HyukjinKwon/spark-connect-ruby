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
      @conf = RuntimeConfig.new(client)
    end

    class << self
      # @return [Builder] a new session builder.
      def builder
        Builder.new
      end

      # The currently active session set by {#set_active} / {Builder#get_or_create}.
      # @return [SparkSession, nil]
      attr_accessor :active

      # @api private
    end

    # Allocate the next unique plan id. Used by {PlanBuilder.relation}.
    # @return [Integer]
    def next_plan_id
      @plan_id += 1
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
      StreamingQueryManager.new(self)
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

    # Make this the active/default session.
    # @return [self]
    def set_active
      SparkSession.active = self
      self
    end

    # Release the server-side session and stop the client.
    # @return [void]
    def stop
      @client.release_session
      SparkSession.active = nil if SparkSession.active.equal?(self)
      nil
    end

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
      def initialize
        @options = {}
        @remote = nil
      end

      # Set the connection string (`sc://...`).
      # @return [self]
      def remote(url)
        @remote = url
        self
      end

      # Set the application name.
      # @return [self]
      def app_name(name)
        @options["spark.app.name"] = name
        self
      end

      # Set an arbitrary configuration option to apply after connecting.
      # @return [self]
      def config(key, value)
        @options[key.to_s] = value
        self
      end

      # Build (or reuse the active) {SparkSession}.
      # @return [SparkSession]
      def get_or_create
        existing = SparkSession.active
        return existing if existing

        session = create
        SparkSession.active = session
        session
      end
      alias getOrCreate get_or_create

      # Always build a new {SparkSession}.
      # @return [SparkSession]
      def create
        url = @remote || ENV["SPARK_REMOTE"] || "sc://localhost:15002"
        client = SparkConnectClient.new(ChannelBuilder.new(url))
        session = SparkSession.new(client)
        @options.each { |k, v| session.conf.set(k, v) unless k == "spark.app.name" }
        session
      end
      alias build create
    end
  end
end
