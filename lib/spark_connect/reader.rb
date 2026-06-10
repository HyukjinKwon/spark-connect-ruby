# frozen_string_literal: true

module SparkConnect
  # Loads data from external sources into a {DataFrame}. Returned by
  # {SparkSession#read}. Mirrors PySpark's `DataFrameReader`.
  #
  # @example
  #   spark.read.format("csv").option("header", true).load("data.csv")
  #   spark.read.json("events.json")
  #   spark.read.table("my_table")
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
      @schema = schema.is_a?(Types::StructType) ? schema.simple_string : schema.to_s
      self
    end

    # Set a single read option.
    # @return [self]
    def option(key, value)
      @options[key.to_s] = value.to_s
      self
    end

    # Set multiple read options.
    # @return [self]
    def options(opts)
      opts.each { |k, v| @options[k.to_s] = v.to_s }
      self
    end

    # Load data from the given path(s) using the configured format.
    #
    # @param paths [Array<String>]
    # @return [DataFrame]
    def load(*paths)
      ds = Proto::Read::DataSource.new(options: @options, paths: paths.flatten.map(&:to_s))
      ds.format = @format if @format
      ds.schema = @schema if @schema
      read_relation(data_source: ds)
    end

    # Read a registered table or view.
    #
    # @param name [String]
    # @return [DataFrame]
    def table(name)
      nt = Proto::Read::NamedTable.new(unparsed_identifier: name.to_s, options: @options)
      read_relation(named_table: nt)
    end

    # @return [DataFrame] CSV at `paths`.
    def csv(*paths) = format("csv").load(*paths)
    # @return [DataFrame] JSON at `paths`.
    def json(*paths) = format("json").load(*paths)
    # @return [DataFrame] Parquet at `paths`.
    def parquet(*paths) = format("parquet").load(*paths)
    # @return [DataFrame] ORC at `paths`.
    def orc(*paths) = format("orc").load(*paths)
    # @return [DataFrame] text at `paths` (one `value` column per line).
    def text(*paths) = format("text").load(*paths)

    # Read from a JDBC source.
    #
    # @param url [String] the JDBC URL.
    # @param table [String] the table name (or subquery).
    # @param properties [Hash] connection properties (`user`, `password`, ...).
    # @return [DataFrame]
    def jdbc(url, table, properties = {})
      opts = { "url" => url, "dbtable" => table }.merge(properties.transform_keys(&:to_s))
      format("jdbc").options(opts).load
    end

    private

    def read_relation(**read_kw)
      DataFrame.new(@session, PlanBuilder.relation(@session, read: Proto::Read.new(**read_kw)))
    end
  end
end
