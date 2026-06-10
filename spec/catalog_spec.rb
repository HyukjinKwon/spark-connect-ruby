# frozen_string_literal: true

RSpec.describe SparkConnect::Catalog do
  let(:client) { SpecHelpers::FakeClient.new }
  let(:session) { fake_session(client) }
  let(:catalog) { session.catalog }

  def last_cat_type
    client.last_relation.catalog.cat_type
  end

  describe "metadata listing" do
    before do
      client.rows = [{ "name" => "default" }]
      client.schema = SparkConnect::Types.struct(SparkConnect::Types.field("name", SparkConnect::Types.string))
    end

    it "lists databases via a catalog relation" do
      rows = catalog.list_databases
      expect(last_cat_type).to eq(:list_databases)
      expect(rows.first["name"]).to eq("default")
    end

    it "lists tables, optionally scoped to a database" do
      catalog.list_tables("db1")
      rel = client.last_relation.catalog
      expect(rel.cat_type).to eq(:list_tables)
      expect(rel.list_tables.db_name).to eq("db1")
    end

    it "lists columns of a table" do
      catalog.list_columns("t1")
      expect(client.last_relation.catalog.list_columns.table_name).to eq("t1")
    end

    it "lists functions and catalogs" do
      catalog.list_functions
      expect(last_cat_type).to eq(:list_functions)
      catalog.list_catalogs
      expect(last_cat_type).to eq(:list_catalogs)
    end
  end

  describe "current catalog / database" do
    before do
      client.rows = [{ "v" => "default" }]
      client.schema = SparkConnect::Types.struct(SparkConnect::Types.field("v", SparkConnect::Types.string))
    end

    it "reads the current database and catalog" do
      expect(catalog.current_database).to eq("default")
      expect(last_cat_type).to eq(:current_database)
      catalog.current_catalog
      expect(last_cat_type).to eq(:current_catalog)
    end

    it "sets the current database and catalog" do
      catalog.set_current_database("db2")
      expect(client.last_relation.catalog.set_current_database.db_name).to eq("db2")
      catalog.set_current_catalog("cat2")
      expect(client.last_relation.catalog.set_current_catalog.catalog_name).to eq("cat2")
    end
  end

  describe "existence predicates" do
    before do
      client.rows = [{ "v" => true }]
      client.schema = SparkConnect::Types.struct(SparkConnect::Types.field("v", SparkConnect::Types.boolean))
    end

    it "checks table existence" do
      expect(catalog.table_exists("t1")).to be(true)
      expect(client.last_relation.catalog.table_exists.table_name).to eq("t1")
    end

    it "checks database and function existence" do
      expect(catalog.database_exists("db1")).to be(true)
      expect(last_cat_type).to eq(:database_exists)
      expect(catalog.function_exists("f1")).to be(true)
      expect(last_cat_type).to eq(:function_exists)
    end

    it "reports whether a table is cached" do
      expect(catalog.cached?("t1")).to be(true)
      expect(last_cat_type).to eq(:is_cached)
    end
  end

  describe "cache and maintenance commands" do
    it "issues cache/uncache/clear/refresh/recover relations" do
      catalog.cache_table("t1")
      expect(last_cat_type).to eq(:cache_table)
      catalog.uncache_table("t1")
      expect(last_cat_type).to eq(:uncache_table)
      catalog.clear_cache
      expect(last_cat_type).to eq(:clear_cache)
      catalog.refresh_table("t1")
      expect(last_cat_type).to eq(:refresh_table)
      catalog.recover_partitions("t1")
      expect(last_cat_type).to eq(:recover_partitions)
    end
  end
end
