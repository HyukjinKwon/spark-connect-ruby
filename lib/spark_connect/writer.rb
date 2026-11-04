# frozen_string_literal: true

module SparkConnect
  # Saves the contents of a {DataFrame} to external storage. Returned by
  # {DataFrame#write}. Mirrors PySpark's `DataFrameWriter`.
  #
  # @example
  #   df.write.format("parquet").mode(:overwrite).save("out.parquet")
  #   df.write.mode(:append).save_as_table("my_table")
  class DataFrameWriter
    Proto = SparkConnect::Proto
    WO = Proto::WriteOperation

    SAVE_MODES = {
      append: :SAVE_MODE_APPEND,
      overwrite: :SAVE_MODE_OVERWRITE,
      error: :SAVE_MODE_ERROR_IF_EXISTS,
      errorifexists: :SAVE_MODE_ERROR_IF_EXISTS,
      error_if_exists: :SAVE_MODE_ERROR_IF_EXISTS,
      ignore: :SAVE_MODE_IGNORE,
      default: :SAVE_MODE_UNSPECIFIED,
    }.freeze

    # @param df [DataFrame]
    def initialize(df)
      @df = df
      @source = nil
      @mode = :SAVE_MODE_UNSPECIFIED
      @options = {}
      @partitioning_columns = []
      @sort_columns = []
      @bucket_cols = nil
      @num_buckets = nil
      @clustering_columns = []
      @schema_evolution = false
    end

    # @return [self] set the output format.
    def format(source)
      @source = source.to_s
      self
    end

    # @return [self] set the save mode (`:append`, `:overwrite`, `:ignore`,
    #   `:error`). `nil` keeps the current mode.
    def mode(save_mode)
      return self if save_mode.nil?

      @mode = SAVE_MODES.fetch(save_mode.to_s.downcase.to_sym) do
        raise IllegalArgumentError, "Unknown save mode: #{save_mode}"
      end
      self
    end

    # @return [self] set a write option (a `nil` value is ignored).
    def option(key, value)
      @options[key.to_s] = value.to_s unless value.nil?
      self
    end

    # @return [self] set multiple write options.
    def options(opts)
      opts.each { |k, v| option(k, v) }
      self
    end

    # @return [self] cluster the output (e.g. liquid clustering) by these columns.
    def cluster_by(*cols)
      cols = cols.flatten.map(&:to_s)
      raise IllegalArgumentError, "cluster_by needs one or more clustering columns" if cols.empty?

      @clustering_columns = cols
      self
    end
    alias clusterBy cluster_by

    # Enable automatic schema evolution for this write (Spark 4.2+); the target
    # table must declare the `AUTOMATIC_SCHEMA_EVOLUTION` capability. This is
    # the Scala API's `withSchemaEvolution` (PySpark exposes it only on
    # {MergeIntoWriter}).
    # @return [self]
    def with_schema_evolution
      @schema_evolution = true
      self
    end
    alias withSchemaEvolution with_schema_evolution

    # @return [self] partition the output by these columns.
    def partition_by(*cols)
      @partitioning_columns = cols.flatten.map(&:to_s)
      self
    end
    alias partitionBy partition_by

    # @return [self] sort within partitions/buckets by these columns.
    def sort_by(*cols)
      @sort_columns = cols.flatten.map(&:to_s)
      self
    end
    alias sortBy sort_by

    # @return [self] bucket the output into `num_buckets` by these columns.
    def bucket_by(num_buckets, *cols)
      @num_buckets = num_buckets
      @bucket_cols = cols.flatten.map(&:to_s)
      self
    end
    alias bucketBy bucket_by

    # Save to a path.
    # @param path [String, nil]
    # @return [void]
    def save(path = nil)
      op = base_operation
      op.path = path if path
      execute(op)
    end

    # Save as a managed/registered table.
    # @return [void]
    def save_as_table(name)
      op = base_operation
      op.table = WO::SaveTable.new(table_name: name.to_s, save_method: :TABLE_SAVE_METHOD_SAVE_AS_TABLE)
      execute(op)
    end
    alias saveAsTable save_as_table

    # Insert into an existing table (by position).
    # @return [void]
    def insert_into(name)
      op = base_operation
      op.table = WO::SaveTable.new(table_name: name.to_s, save_method: :TABLE_SAVE_METHOD_INSERT_INTO)
      execute(op)
    end
    alias insertInto insert_into

    # @return [void] convenience for `format("parquet").save(path)`.
    def parquet(path) = format("parquet").save(path)
    def json(path) = format("json").save(path)
    def csv(path) = format("csv").save(path)
    def orc(path) = format("orc").save(path)
    def text(path) = format("text").save(path)
    # @return [void] convenience for `format("xml").save(path)` (set `option("rowTag", ...)`).
    def xml(path) = format("xml").save(path)

    # Write to a JDBC table.
    #
    # @param url [String]
    # @param table [String]
    # @param mode [Symbol, String, nil] the save mode.
    # @param properties [Hash] connection properties (`user`, `password`, ...).
    # @return [void]
    def jdbc(url, table, mode = nil, properties = {})
      format("jdbc").mode(mode).options(properties.transform_keys(&:to_s)).options("url" => url, "dbtable" => table).save
    end

    private

    def base_operation
      op = WO.new(
        input: @df.relation, mode: @mode, options: @options,
        partitioning_columns: @partitioning_columns, sort_column_names: @sort_columns,
        clustering_columns: @clustering_columns, with_schema_evolution: @schema_evolution
      )
      op.source = @source if @source
      op.bucket_by = WO::BucketBy.new(bucket_column_names: @bucket_cols, num_buckets: @num_buckets) if @num_buckets
      op
    end

    def execute(op)
      @df.session.client.execute_command(Proto::Command.new(write_operation: op))
      nil
    end
  end

  # The DataSourceV2 write interface, returned by {DataFrame#write_to}. Mirrors
  # PySpark's `DataFrameWriterV2`.
  #
  # @example
  #   df.write_to("catalog.db.table").using("parquet").create
  #   df.write_to("catalog.db.table").append
  class DataFrameWriterV2
    Proto = SparkConnect::Proto
    WO2 = Proto::WriteOperationV2

    # @param df [DataFrame]
    # @param table [String]
    def initialize(df, table)
      @df = df
      @table = table.to_s
      @provider = nil
      @options = {}
      @table_properties = {}
      @partitioning = []
      @clustering_columns = []
      @schema_evolution = false
    end

    # @return [self] set the table provider/format.
    def using(provider)
      @provider = provider.to_s
      self
    end

    # @return [self] set a write option (a `nil` value is ignored).
    def option(key, value)
      @options[key.to_s] = value.to_s unless value.nil?
      self
    end

    # @return [self] set multiple write options.
    def options(opts)
      opts.each { |k, v| option(k, v) }
      self
    end

    # @return [self] set a table property.
    def table_property(key, value)
      @table_properties[key.to_s] = value.to_s
      self
    end
    alias tableProperty table_property

    # Partition the table by columns or partition transforms (`F.years("ts")`,
    # `F.bucket(4, "id")`, ...).
    # @return [self]
    def partitioned_by(*cols)
      @partitioning = cols.flatten.map { |c| (c.is_a?(Column) ? c : Functions.col(c.to_s)).to_expr }
      self
    end
    alias partition_by partitioned_by
    alias partitionedBy partitioned_by

    # @return [self] cluster the table by these columns.
    def cluster_by(*cols)
      @clustering_columns = cols.flatten.map(&:to_s)
      self
    end
    alias clusterBy cluster_by

    # Enable automatic schema evolution for this write (Spark 4.2+).
    # @return [self]
    def with_schema_evolution
      @schema_evolution = true
      self
    end
    alias withSchemaEvolution with_schema_evolution

    # Create the table. @return [void]
    def create = run(:MODE_CREATE)
    # Replace the table. @return [void]
    def replace = run(:MODE_REPLACE)
    # Create or replace the table. @return [void]
    def create_or_replace = run(:MODE_CREATE_OR_REPLACE)
    alias createOrReplace create_or_replace
    # Append rows. @return [void]
    def append = run(:MODE_APPEND)
    # Overwrite rows matching `condition`. @return [void]
    def overwrite(condition) = run(:MODE_OVERWRITE, overwrite_condition: Column.to_col(condition).to_expr)
    # Dynamically overwrite partitions. @return [void]
    def overwrite_partitions = run(:MODE_OVERWRITE_PARTITIONS)
    alias overwritePartitions overwrite_partitions

    private

    def run(mode, overwrite_condition: nil)
      op = WO2.new(
        input: @df.relation, table_name: @table, mode: mode,
        options: @options, table_properties: @table_properties, partitioning_columns: @partitioning,
        clustering_columns: @clustering_columns, with_schema_evolution: @schema_evolution
      )
      op.provider = @provider if @provider
      op.overwrite_condition = overwrite_condition if overwrite_condition
      @df.session.client.execute_command(Proto::Command.new(write_operation_v2: op))
      nil
    end
  end
end
