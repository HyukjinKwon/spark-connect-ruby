# frozen_string_literal: true

RSpec.describe "Views, JSON, and session management" do
  let(:client) { SpecHelpers::FakeClient.new }
  let(:session) { fake_session(client) }
  let(:df) { session.range(5) }

  describe "temporary views" do
    it "creates a session-local temp view" do
      df.create_temp_view("v")
      cmd = client.last_command.create_dataframe_view
      expect(cmd.name).to eq("v")
      expect(cmd.is_global).to be(false)
      expect(cmd.replace).to be(false)
    end

    it "creates or replaces a temp view" do
      df.create_or_replace_temp_view("v")
      expect(client.last_command.create_dataframe_view.replace).to be(true)
    end

    it "creates global temp views" do
      df.create_global_temp_view("g")
      expect(client.last_command.create_dataframe_view.is_global).to be(true)
      df.create_or_replace_global_temp_view("g")
      expect(client.last_command.create_dataframe_view.is_global).to be(true)
      expect(client.last_command.create_dataframe_view.replace).to be(true)
    end
  end

  describe "#col_regex" do
    it "builds an unresolved-regex column" do
      expect(df.col_regex("`a.*`").to_expr.expr_type).to eq(:unresolved_regex)
    end
  end

  describe "#to_json" do
    it "projects each row to a JSON value column" do
      expect(rel_type(df.to_json)).to eq(:project)
    end
  end

  describe "session tags" do
    it "adds, lists, and clears operation tags" do
      session.add_tag("etl")
      session.add_tag("nightly")
      expect(session.get_tags).to eq(%w[etl nightly])
      session.remove_tag("etl")
      expect(session.get_tags).to eq(%w[nightly])
      session.clear_tags
      expect(session.get_tags).to eq([])
    end
  end

  describe "#interrupt_*" do
    it "interrupts all, by tag, and by operation id" do
      expect(session.interrupt_all).to eq(["op-1"])
      expect(client.interrupts.last[:type]).to eq(:all)
      session.interrupt_tag("etl")
      expect(client.interrupts.last).to eq({ type: :tag, value: "etl" })
      session.interrupt_operation("op-9")
      expect(client.interrupts.last).to eq({ type: :operation_id, value: "op-9" })
    end
  end

  describe "#new_session" do
    it "creates a session with a distinct id" do
      other = session.new_session
      expect(other).to be_a(SparkConnect::SparkSession)
      expect(other.session_id).not_to eq(session.session_id)
    end
  end

  describe "Catalog#create_table" do
    it "builds a CreateTable catalog relation" do
      session.catalog.create_table("t", source: "parquet", path: "/data/t")
      ct = client.last_relation.catalog.create_table
      expect(ct.table_name).to eq("t")
      expect(ct.source).to eq("parquet")
      expect(ct.path).to eq("/data/t")
    end

    it "builds a CreateExternalTable catalog relation" do
      session.catalog.create_external_table("e", path: "/data/e")
      expect(client.last_relation.catalog.create_external_table.table_name).to eq("e")
    end
  end
end
