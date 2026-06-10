# frozen_string_literal: true

# Integration specs for {SparkConnect::DataFrame}. End-to-end against a live
# Spark Connect server; only run when SPARK_REMOTE is set.
RSpec.describe SparkConnect::DataFrame, if: ENV.fetch("SPARK_REMOTE", nil) do
  F = SparkConnect::F

  before(:all) do
    skip "set SPARK_REMOTE to run integration specs" unless ENV["SPARK_REMOTE"]
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE", nil)).create
  end

  after(:all) do
    @session&.stop
  end

  let(:session) { @session }

  describe "#select" do
    it "evaluates arithmetic with an alias" do
      rows = session.range(3).select((F.col("id") * 10).alias("ten_x")).collect
      expect(rows.map { |r| r["ten_x"] }).to eq([0, 10, 20])
    end

    it "selects several expressions" do
      rows = session.range(2).select(
        F.col("id").alias("i"),
        (F.col("id") + 1).alias("next")
      ).collect
      expect(rows.map(&:to_h)).to eq([{ "i" => 0, "next" => 1 }, { "i" => 1, "next" => 2 }])
    end
  end

  describe "#filter" do
    it "keeps matching rows" do
      ids = session.range(10).filter((F.col("id") % 2) == 0).order_by("id").collect.map { |r| r["id"] }
      expect(ids).to eq([0, 2, 4, 6, 8])
    end
  end

  describe "#group_by / #agg" do
    it "aggregates count and sum grouped and ordered" do
      df = session.create_data_frame([
                                       { "dept" => "a", "salary" => 10 },
                                       { "dept" => "a", "salary" => 20 },
                                       { "dept" => "b", "salary" => 30 },
                                     ])
      rows = df.group_by("dept")
               .agg(F.count(F.lit(1)).alias("n"), F.sum("salary").alias("total"))
               .order_by("dept")
               .collect
      expect(rows.map { |r| r["dept"] }).to eq(%w[a b])
      expect(rows.map { |r| r["n"] }).to eq([2, 1])
      expect(rows.map { |r| r["total"] }).to eq([30, 30])
    end
  end

  describe "#join" do
    it "joins two DataFrames on a key" do
      left = session.create_data_frame([{ "k" => 1, "l" => "x" }, { "k" => 2, "l" => "y" }])
      right = session.create_data_frame([{ "k" => 1, "r" => "p" }, { "k" => 2, "r" => "q" }])
      rows = left.join(right, on: "k").order_by("k").collect
      expect(rows.map { |r| [r["k"], r["l"], r["r"]] }).to eq([[1, "x", "p"], [2, "y", "q"]])
    end
  end

  describe "#order_by" do
    it "sorts descending" do
      ids = session.range(4).order_by(F.col("id").desc).collect.map { |r| r["id"] }
      expect(ids).to eq([3, 2, 1, 0])
    end
  end

  describe "window functions" do
    it "computes row_number over partitionBy/orderBy" do
      df = session.create_data_frame([
                                       { "g" => "a", "v" => 5 },
                                       { "g" => "a", "v" => 3 },
                                       { "g" => "b", "v" => 9 },
                                     ])
      w = SparkConnect::Window.partition_by("g").order_by(F.col("v").asc)
      rows = df.with_column("rn", F.row_number.over(w)).order_by("g", "v").collect
      expect(rows.map { |r| [r["g"], r["v"], r["rn"]] }).to eq([
                                                                 ["a", 3, 1], ["a", 5, 2], ["b", 9, 1],
                                                               ])
    end
  end

  describe "#schema" do
    it "matches an explicitly constructed StructType" do
      df = session.range(3).select(F.col("id"))
      expected = SparkConnect::Types.struct(
        SparkConnect::Types.field("id", SparkConnect::Types.long, nullable: false)
      )
      expect(df.schema).to eq(expected)
    end
  end

  describe "#show_string" do
    it "returns a non-empty formatted table" do
      s = session.range(3).show_string(3)
      expect(s).to be_a(String)
      expect(s).not_to be_empty
      expect(s).to include("id")
    end
  end
end
