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

    # PySpark-style camelCase aliases, defined at the end of the class.
    CAMEL_ALIASES = {
      currentCatalog: :current_catalog, setCurrentCatalog: :set_current_catalog, listCatalogs: :list_catalogs,
      currentDatabase: :current_database, setCurrentDatabase: :set_current_database, listDatabases: :list_databases,
      getDatabase: :get_database, listTables: :list_tables, getTable: :get_table, listViews: :list_views,
      listFunctions: :list_functions, getFunction: :get_function, listColumns: :list_columns,
      tableExists: :table_exists, databaseExists: :database_exists, functionExists: :function_exists,
      dropTempView: :drop_temp_view, dropGlobalTempView: :drop_global_temp_view, isCached: :cached?,
      cacheTable: :cache_table, uncacheTable: :uncache_table, clearCache: :clear_cache, refreshTable: :refresh_table,
      refreshByPath: :refresh_by_path, recoverPartitions: :recover_partitions, createTable: :create_table,
      createExternalTable: :create_external_table, dropTable: :drop_table, dropView: :drop_view,
      createDatabase: :create_database, dropDatabase: :drop_database, listPartitions: :list_partitions,
      getTableProperties: :get_table_properties, getCreateTableString: :get_create_table_string,
      truncateTable: :truncate_table, analyzeTable: :analyze_table,
    }.freeze

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

    # @param pattern [String, nil] a SQL LIKE pattern the names must match.
    # @return [Array<Row>] the catalogs (`name`, `description`).
    def list_catalogs(pattern = nil)
      lc = Proto::ListCatalogs.new
      lc.pattern = pattern if pattern
      rows(C.new(list_catalogs: lc))
    end

    # @return [String] the current default database.
    def current_database
      scalar(C.new(current_database: Proto::CurrentDatabase.new))
    end

    # Set the current database. @return [void]
    def set_current_database(name)
      run(C.new(set_current_database: Proto::SetCurrentDatabase.new(db_name: name.to_s)))
    end

    # @param pattern [String, nil] a SQL LIKE pattern the names must match.
    # @return [Array<Row>] the databases (`name`, `catalog`, `description`, `locationUri`).
    def list_databases(pattern = nil)
      ld = Proto::ListDatabases.new
      ld.pattern = pattern if pattern
      rows(C.new(list_databases: ld))
    end

    # @param db_name [String] the database (may be catalog-qualified).
    # @return [Row] the database (`name`, `catalog`, `description`, `locationUri`).
    def get_database(db_name)
      rows(C.new(get_database: Proto::GetDatabase.new(db_name: db_name.to_s))).first
    end

    # @param db_name [String, nil] restrict to a database.
    # @param pattern [String, nil] a SQL LIKE pattern the names must match.
    # @return [Array<Row>] tables and views (`name`, `catalog`, `namespace`,
    #   `description`, `tableType`, `isTemporary`).
    def list_tables(db_name = nil, pattern = nil)
      lt = Proto::ListTables.new
      lt.db_name = db_name if db_name
      lt.pattern = pattern if pattern
      rows(C.new(list_tables: lt))
    end

    # @param table_name [String] a (possibly qualified) table or view name.
    # @return [Row] the table (`name`, `catalog`, `namespace`, `description`,
    #   `tableType`, `isTemporary`).
    def get_table(table_name, db_name = nil)
      gt = Proto::GetTable.new(table_name: table_name.to_s)
      gt.db_name = db_name if db_name
      rows(C.new(get_table: gt)).first
    end

    # Views (not tables) in a database (Spark 4.2+).
    #
    # @param db_name [String, nil] the database (current database when nil).
    # @param pattern [String, nil] a SQL LIKE pattern the names must match.
    # @return [Array<Row>] like {#list_tables}.
    def list_views(db_name = nil, pattern = nil)
      db_name = current_database if pattern && db_name.nil?
      lv = Proto::ListViews.new
      lv.db_name = db_name if db_name
      lv.pattern = pattern if pattern
      rows(C.new(list_views: lv))
    end

    # @return [Array<Row>] functions registered in the catalog.
    def list_functions(db_name = nil, pattern = nil)
      lf = Proto::ListFunctions.new
      lf.db_name = db_name if db_name
      lf.pattern = pattern if pattern
      rows(C.new(list_functions: lf))
    end

    # @param function_name [String] a (possibly qualified) function name.
    # @return [Row] the function (`name`, `catalog`, `namespace`, `description`,
    #   `className`, `isTemporary`).
    def get_function(function_name, db_name = nil)
      gf = Proto::GetFunction.new(function_name: function_name.to_s)
      gf.db_name = db_name if db_name
      rows(C.new(get_function: gf)).first
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
    alias is_cached cached?

    # Cache a table (in memory and on disk by default).
    # @param storage_level [StorageLevel, nil]
    # @return [void]
    def cache_table(table_name, storage_level = nil)
      ct = Proto::CacheTable.new(table_name: table_name.to_s)
      ct.storage_level = storage_level.to_proto if storage_level
      run(C.new(cache_table: ct))
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

    # Invalidate and refresh cached data/metadata of everything under `path`.
    # @return [void]
    def refresh_by_path(path)
      run(C.new(refresh_by_path: Proto::RefreshByPath.new(path: path.to_s)))
    end

    # Recover all partitions of a table. @return [void]
    def recover_partitions(table_name)
      run(C.new(recover_partitions: Proto::RecoverPartitions.new(table_name: table_name.to_s)))
    end

    # ---- DDL and table maintenance (Spark 4.2+) -------------------------------

    # Drop a persistent table (not a temporary view; see {#drop_temp_view}).
    #
    # @param if_exists [Boolean] do not fail when the table does not exist.
    # @param purge [Boolean] skip the trash (where supported).
    # @return [void]
    def drop_table(table_name, if_exists: false, purge: false)
      run(C.new(drop_table: Proto::DropTable.new(table_name: table_name.to_s, if_exists: if_exists, purge: purge)))
    end

    # Drop a persistent view.
    # @return [void]
    def drop_view(view_name, if_exists: false)
      run(C.new(drop_view: Proto::DropView.new(view_name: view_name.to_s, if_exists: if_exists)))
    end

    # Create a database (namespace).
    #
    # @param db_name [String] may be catalog-qualified / multi-part.
    # @param if_not_exists [Boolean]
    # @param properties [Hash{String=>String}] e.g. `comment`, `location`.
    # @return [void]
    def create_database(db_name, if_not_exists: false, properties: {})
      cd = Proto::CreateDatabase.new(db_name: db_name.to_s, if_not_exists: if_not_exists, properties: stringify(properties))
      run(C.new(create_database: cd))
    end

    # Drop a database (namespace).
    #
    # @param cascade [Boolean] also drop its tables, views, and functions.
    # @return [void]
    def drop_database(db_name, if_exists: false, cascade: false)
      run(C.new(drop_database: Proto::DropDatabase.new(db_name: db_name.to_s, if_exists: if_exists, cascade: cascade)))
    end

    # The partitions of a table, like `SHOW PARTITIONS`.
    # @return [Array<Row>] one Row with a `partition` String (e.g. `"p=1"`) per partition.
    def list_partitions(table_name)
      rows(C.new(list_partitions: Proto::ListPartitions.new(table_name: table_name.to_s)))
    end

    # The properties of a table or view, like `SHOW TBLPROPERTIES`.
    # @return [Hash{String=>String}]
    def get_table_properties(table_name)
      rows(C.new(get_table_properties: Proto::GetTableProperties.new(table_name: table_name.to_s))).to_h { |r| [r[0], r[1]] }
    end

    # The `SHOW CREATE TABLE` DDL of a table or view.
    # @param as_serde [Boolean] request Hive SerDe DDL where applicable.
    # @return [String]
    def get_create_table_string(table_name, as_serde: false)
      gcts = Proto::GetCreateTableString.new(table_name: table_name.to_s, as_serde: as_serde)
      scalar(C.new(get_create_table_string: gcts)).to_s
    end

    # Remove all rows from a table (not supported for views).
    # @return [void]
    def truncate_table(table_name)
      run(C.new(truncate_table: Proto::TruncateTable.new(table_name: table_name.to_s)))
    end

    # Compute table statistics, like `ANALYZE TABLE ... COMPUTE STATISTICS`.
    # @param no_scan [Boolean] only compute size statistics (`NOSCAN`).
    # @return [void]
    def analyze_table(table_name, no_scan: false)
      run(C.new(analyze_table: Proto::AnalyzeTable.new(table_name: table_name.to_s, no_scan: no_scan)))
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

    CAMEL_ALIASES.each { |camel, snake| alias_method camel, snake }

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
