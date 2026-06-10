# frozen_string_literal: true

# Integration specs for {SparkConnect::Functions}. End-to-end against a live
# Spark Connect server; only run when SPARK_REMOTE is set.
RSpec.describe SparkConnect::Functions, if: ENV.fetch("SPARK_REMOTE", nil) do
  F = SparkConnect::F

  before(:all) do
    skip "set SPARK_REMOTE to run integration specs" unless ENV["SPARK_REMOTE"]
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE", nil)).create
  end

  after(:all) do
    @session&.stop
  end

  let(:session) { @session }

  def names_df
    session.create_data_frame([{ "name" => "alice" }, { "name" => "bob" }])
  end

  describe "string functions" do
    it "uppercases with F.upper" do
      rows = names_df.select(F.upper(F.col("name")).alias("u")).order_by("u").collect
      expect(rows.map { |r| r["u"] }).to eq(%w[ALICE BOB])
    end

    it "computes string length with F.length" do
      rows = names_df.select(F.length(F.col("name")).alias("len")).order_by(F.col("len").asc).collect
      expect(rows.map { |r| r["len"] }).to eq([3, 5])
    end
  end

  describe "literals and arithmetic" do
    it "evaluates F.lit and a binary op" do
      row = session.range(1).select((F.col("id") + F.lit(41)).alias("answer")).collect.first
      expect(row["answer"]).to eq(41)
    end
  end

  describe "aggregate functions" do
    it "sums a column with F.sum" do
      total = session.range(5).select(F.sum("id").alias("s")).collect.first["s"]
      expect(total).to eq(10)
    end

    it "counts rows with F.count" do
      n = session.range(7).select(F.count(F.lit(1)).alias("n")).collect.first["n"]
      expect(n).to eq(7)
    end
  end
end
