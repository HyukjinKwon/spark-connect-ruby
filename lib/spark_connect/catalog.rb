# frozen_string_literal: true

module SparkConnect
  # The catalog interface for inspecting and managing databases, tables,
  # functions, and the query cache. Returned by {SparkSession#catalog}. Mirrors
  # PySpark's `Catalog`.
  #
  # Methods that return rows ({#list_databases}, {#list_tables}, ...) return
  # arrays of {Row}; predicate methods return booleans.
  #
  # @example
  #   spark.catalog.list_tables.each { |t| puts t["name"] }
  #   spark.catalog.table_exists("my_table")  #=> true
  class Catalog
    Proto = SparkConnect::Proto
    C = Proto::Catalog

    # @param session [SparkSession]
    def initialize(session)
      @session = session
    end

    # @return [String] the current default catalog.
    def current_catalog
      scalar(C.new(current_catalog: Proto::CurrentCatalog.new))
    end

    # Set the current catalog. @return [void]
    def set_current_catalog(name)
      run(C.new(set_current_catalog: Proto::SetCurrentCatalog.new(catalog_name: name.to_s)))
    end

    # @return [Array<Row>] all catalogs.
    def list_catalogs
      rows(C.new(list_catalogs: Proto::ListCatalogs.new))
    end

    # @return [String] the current default database.
    def current_database
      scalar(C.new(current_database: Proto::CurrentDatabase.new))
    end

    # Set the current database. @return [void]
    def set_current_database(name)
      run(C.new(set_current_database: Proto::SetCurrentDatabase.new(db_name: name.to_s)))
    end

    # @return [Array<Row>] all databases.
    def list_databases
      rows(C.new(list_databases: Proto::ListDatabases.new))
    end

    # @param db_name [String, nil] restrict to a database.
    # @return [Array<Row>] tables (and views).
    def list_tables(db_name = nil)
      lt = Proto::ListTables.new
      lt.db_name = db_name if db_name
      rows(C.new(list_tables: lt))
    end

    # @return [Array<Row>] functions registered in the catalog.
    def list_functions(db_name = nil)
      lf = Proto::ListFunctions.new
      lf.db_name = db_name if db_name
      rows(C.new(list_functions: lf))
    end

    # @param table_name [String]
    # @return [Array<Row>] the columns of a table.
    def list_columns(table_name, db_name = nil)
      lc = Proto::ListColumns.new(table_name: table_name.to_s)
      lc.db_name = db_name if db_name
      rows(C.new(list_columns: lc))
    end

    # @return [Boolean] whether a table or view exists.
    def table_exists(table_name, db_name = nil)
      te = Proto::TableExists.new(table_name: table_name.to_s)
      te.db_name = db_name if db_name
      scalar(C.new(table_exists: te)) == true
    end

    # @return [Boolean] whether a database exists.
    def database_exists(db_name)
      scalar(C.new(database_exists: Proto::DatabaseExists.new(db_name: db_name.to_s))) == true
    end

    # @return [Boolean] whether a function exists.
    def function_exists(function_name, db_name = nil)
      fe = Proto::FunctionExists.new(function_name: function_name.to_s)
      fe.db_name = db_name if db_name
      scalar(C.new(function_exists: fe)) == true
    end

    # Drop a session-local temporary view. @return [Boolean]
    def drop_temp_view(view_name)
      scalar(C.new(drop_temp_view: Proto::DropTempView.new(view_name: view_name.to_s))) == true
    end

    # Drop a global temporary view. @return [Boolean]
    def drop_global_temp_view(view_name)
      scalar(C.new(drop_global_temp_view: Proto::DropGlobalTempView.new(view_name: view_name.to_s))) == true
    end

    # @return [Boolean] whether the table is cached.
    def cached?(table_name)
      scalar(C.new(is_cached: Proto::IsCached.new(table_name: table_name.to_s))) == true
    end

    # Cache a table in memory. @return [void]
    def cache_table(table_name)
      run(C.new(cache_table: Proto::CacheTable.new(table_name: table_name.to_s)))
    end

    # Remove a table from the cache. @return [void]
    def uncache_table(table_name)
      run(C.new(uncache_table: Proto::UncacheTable.new(table_name: table_name.to_s)))
    end

    # Clear all cached tables. @return [void]
    def clear_cache
      run(C.new(clear_cache: Proto::ClearCache.new))
    end

    # Invalidate and refresh cached metadata for a table. @return [void]
    def refresh_table(table_name)
      run(C.new(refresh_table: Proto::RefreshTable.new(table_name: table_name.to_s)))
    end

    # Recover all partitions of a table. @return [void]
    def recover_partitions(table_name)
      run(C.new(recover_partitions: Proto::RecoverPartitions.new(table_name: table_name.to_s)))
    end

    # Create a managed table and return a {DataFrame} over it.
    #
    # @param table_name [String]
    # @param path [String, nil]
    # @param source [String, nil] the data source/format.
    # @param schema [Types::StructType, nil]
    # @param description [String, nil]
    # @param options [Hash{String=>String}]
    # @return [DataFrame]
    def create_table(table_name, path: nil, source: nil, schema: nil, description: nil, options: {})
      ct = Proto::CreateTable.new(table_name: table_name.to_s, options: stringify(options))
      ct.path = path if path
      ct.source = source if source
      ct.description = description if description
      ct.schema = schema.to_proto if schema
      catalog_df(C.new(create_table: ct)).collect # eagerly create the table
      @session.table(table_name.to_s)
    end

    # Create a table backed by data at `path` (an external/unmanaged table).
    #
    # @return [DataFrame]
    def create_external_table(table_name, path: nil, source: nil, schema: nil, options: {})
      ct = Proto::CreateExternalTable.new(table_name: table_name.to_s, options: stringify(options))
      ct.path = path if path
      ct.source = source if source
      ct.schema = schema.to_proto if schema
      catalog_df(C.new(create_external_table: ct)).collect # eagerly create the table
      @session.table(table_name.to_s)
    end

    private

    def stringify(options)
      options.to_h { |k, v| [k.to_s, v.to_s] }
    end

    def catalog_df(catalog)
      DataFrame.new(@session, PlanBuilder.relation(@session, catalog: catalog))
    end

    def rows(catalog)
      catalog_df(catalog).collect
    end

    def scalar(catalog)
      row = catalog_df(catalog).collect.first
      row&.[](0)
    end

    def run(catalog)
      catalog_df(catalog).collect
      nil
    end
  end
end
