# frozen_string_literal: true

module SparkConnect
  # Loads data from external sources into a {DataFrame}. Returned by
  # {SparkSession#read}. Mirrors PySpark's `DataFrameReader`.
  #
  # @example
  #   spark.read.format("csv").option("header", true).load("data.csv")
  #   spark.read.json("events.json")
  #   spark.read.table("my_table")
  #   spark.read.json(lines_df)  # parse a DataFrame of JSON strings
  class DataFrameReader
    Proto = SparkConnect::Proto

    # @param session [SparkSession]
    def initialize(session)
      @session = session
      @format = nil
      @schema = nil
      @options = {}
    end

    # Set the input format (`"csv"`, `"json"`, `"parquet"`, `"orc"`, ...).
    # @return [self]
    def format(source)
      @format = source.to_s
      self
    end

    # Set the input schema (a {Types::StructType} or DDL string).
    # @return [self]
    def schema(schema)
      unless schema.is_a?(Types::StructType) || schema.is_a?(String)
        raise IllegalArgumentError, "schema must be a StructType or DDL String, got #{schema.class}"
      end

      @schema = schema
      self
    end

    # Set a single read option. A `nil` value is ignored.
    # @return [self]
    def option(key, value)
      @options[key.to_s] = value.to_s unless value.nil?
      self
    end

    # Set multiple read options.
    # @return [self]
    def options(opts)
      opts.each { |k, v| option(k, v) }
      self
    end

    # Load data from the given path(s) using the configured format.
    #
    # @param paths [Array<String>]
    # @return [DataFrame]
    def load(*paths)
      read_relation(data_source: data_source(paths.flatten.map(&:to_s)))
    end

    # Read a registered table or view.
    #
    # @param name [String]
    # @return [DataFrame]
    def table(name)
      nt = Proto::Read::NamedTable.new(unparsed_identifier: name.to_s, options: @options)
      read_relation(named_table: nt)
    end

    # The row-level changes (Change Data Capture) of a table as a batch
    # DataFrame (Spark 4.2+; the table's catalog must support change logs).
    # Configure the range with options such as `startingVersion`,
    # `endingVersion`, `startingTimestamp`, `endingTimestamp`,
    # `deduplicationMode`, and `computeUpdates`.
    #
    # @param table_name [String]
    # @return [DataFrame]
    def changes(table_name)
      raise AnalysisError.new("User specified schema not supported with `changes`.", error_class: "_LEGACY_ERROR_TEMP_1189") if @schema

      rc = Proto::RelationChanges.new(unparsed_identifier: table_name.to_s, options: @options)
      DataFrame.new(@session, PlanBuilder.relation(@session, relation_changes: rc))
    end

    # CSV at `paths`, or parse a single-string-column {DataFrame} of CSV lines.
    # @return [DataFrame]
    def csv(*paths) = text_source("csv", :PARSE_FORMAT_CSV, paths)
    # JSON at `paths`, or parse a single-string-column {DataFrame} of JSON documents.
    # @return [DataFrame]
    def json(*paths) = text_source("json", :PARSE_FORMAT_JSON, paths)
    # XML at `paths` (set `option("rowTag", ...)`), or parse a single-string-column
    # {DataFrame} of XML records (Spark 4.2+ for the DataFrame form).
    # @return [DataFrame]
    def xml(*paths) = text_source("xml", :PARSE_FORMAT_XML, paths)
    # @return [DataFrame] Parquet at `paths`.
    def parquet(*paths) = format("parquet").load(*paths)
    # @return [DataFrame] ORC at `paths`.
    def orc(*paths) = format("orc").load(*paths)
    # @return [DataFrame] text at `paths` (one `value` column per line).
    def text(*paths) = format("text").load(*paths)

    JDBC_PARTITION_KEYS = %i[column lower_bound upper_bound num_partitions predicates].freeze

    # Read from a JDBC source, optionally partitioned on a numeric column
    # (`column:` with `lower_bound:`, `upper_bound:`, `num_partitions:`) or by a
    # list of WHERE-clause `predicates:` (one partition each).
    #
    # @example
    #   spark.read.jdbc(url, "people", "user" => "u", "password" => "p")
    #   spark.read.jdbc(url, "people", { "user" => "u" }, column: "id", lower_bound: 0, upper_bound: 100, num_partitions: 4)
    # @param url [String] the JDBC URL.
    # @param table [String] the table name (or subquery).
    # @param properties [Hash] connection properties (`user`, `password`, ...);
    #   String-keyed keyword arguments are merged into it.
    # @return [DataFrame]
    def jdbc(url, table, properties = {}, **opts)
      # Symbol keys are partitioning options; anything else is a connection property.
      partitioning, extra = opts.partition { |k, _| JDBC_PARTITION_KEYS.include?(k) }.map(&:to_h)
      properties = properties.merge(extra)
      column, lower_bound, upper_bound, num_partitions, predicates = partitioning.values_at(*JDBC_PARTITION_KEYS)
      format("jdbc")
      unless column.nil?
        if lower_bound.nil? || upper_bound.nil? || num_partitions.nil?
          raise IllegalArgumentError, "lower_bound, upper_bound, and num_partitions are required with column"
        end

        options("partitionColumn" => column, "lowerBound" => lower_bound, "upperBound" => upper_bound,
                "numPartitions" => num_partitions)
      end
      options(properties.transform_keys(&:to_s))
      options("url" => url, "dbtable" => table)
      return load if predicates.nil? || !column.nil?

      read_relation(data_source: data_source([], predicates: predicates.map(&:to_s)))
    end

    private

    def data_source(paths, predicates: [])
      ds = Proto::Read::DataSource.new(options: @options, paths: paths, predicates: predicates)
      ds.format = @format if @format
      ds.schema = schema_string if @schema
      ds
    end

    # Like PySpark, a StructType is sent as its JSON form (which the server
    # parses losslessly); a String is sent as given (DDL or JSON).
    def schema_string
      @schema.is_a?(Types::StructType) ? @schema.json : @schema
    end

    def text_source(fmt, parse_format, paths)
      return parse(paths.first, parse_format) if paths.size == 1 && paths.first.is_a?(DataFrame)

      format(fmt).load(*paths)
    end

    def parse(df, parse_format)
      parse = Proto::Parse.new(input: df.relation, format: parse_format, options: @options)
      parse.schema = parse_schema.to_proto if @schema
      DataFrame.new(@session, PlanBuilder.relation(@session, parse: parse))
    end

    # The Parse relation carries a typed schema; resolve a DDL string through
    # the server's parser.
    def parse_schema
      return @schema if @schema.is_a?(Types::StructType)

      parsed = @session.client.analyze(ddl_parse: Proto::AnalyzePlanRequest::DDLParse.new(ddl_string: @schema)).ddl_parse.parsed
      Types.from_proto(parsed)
    end

    def read_relation(**read_kw)
      DataFrame.new(@session, PlanBuilder.relation(@session, read: Proto::Read.new(**read_kw)))
    end
  end
end
