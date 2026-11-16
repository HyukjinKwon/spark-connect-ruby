# frozen_string_literal: true

# Integration specs for the ALS, FPGrowth and PrefixSpan operators, against a live Spark Connect server.
RSpec.describe "ML recommendation and FPM operators", if: ENV.fetch("SPARK_REMOTE", nil) do
  before(:all) do
    skip "set SPARK_REMOTE to run integration specs" unless ENV["SPARK_REMOTE"]
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE", nil)).create
  end

  after(:all) do
    @session&.stop
  end

  let(:session) { @session }
  let(:recommendation) { SparkConnect::ML::Recommendation }
  let(:fpm) { SparkConnect::ML::FPM }

  describe "ALS" do
    # Three users, four items, explicit ratings.
    let(:ratings) do
      session.sql("SELECT * FROM VALUES (0, 0, 4.0F), (0, 1, 2.0F), (1, 1, 3.0F), (1, 2, 5.0F), " \
                  "(2, 0, 1.0F), (2, 3, 4.0F) AS t(user, item, rating)")
    end
    let(:model) { recommendation::ALS.new(rank: 2, max_iter: 5, seed: 1, nonnegative: true).fit(ratings) }

    it "fits a model of the requested rank with one factor row per user and item" do
      expect(model).to be_a(recommendation::ALSModel)
      expect(model.rank).to eq(2)
      expect(model.user_factors.collect.size).to eq(3)
      expect(model.item_factors.collect.size).to eq(4)
    end

    it "recommends numItems items for every user" do
      rows = model.recommend_for_all_users(2).collect

      expect(rows.map { |r| r["user"] }).to contain_exactly(0, 1, 2)
      expect(rows.map { |r| r["recommendations"].size }).to all(eq(2))
    end

    it "recommends numUsers users for every item" do
      rows = model.recommend_for_all_items(2).collect

      expect(rows.map { |r| r["item"] }).to contain_exactly(0, 1, 2, 3)
      expect(rows.map { |r| r["recommendations"].size }).to all(eq(2))
    end

    it "restricts the recommendations to a user subset" do
      rows = model.recommend_for_user_subset(session.sql("SELECT 1 AS user"), 2).collect

      expect(rows.map { |r| r["user"] }).to eq([1])
      expect(rows.first["recommendations"].size).to eq(2)
    end

    it "restricts the recommendations to an item subset" do
      rows = model.recommend_for_item_subset(session.sql("SELECT 1 AS item"), 2).collect

      expect(rows.map { |r| r["item"] }).to eq([1])
      expect(rows.first["recommendations"].size).to eq(2)
    end

    it "predicts a finite rating for every rating row" do
      predictions = model.transform(ratings).collect.map { |r| r["prediction"] }

      expect(predictions.size).to eq(6)
      expect(predictions).to all(be_a(Float))
    end
  end

  describe "FPGrowth" do
    # Four transactions: a and b co-occur in three of them, c and d in two.
    let(:transactions) do
      session.sql("SELECT * FROM VALUES (array('a', 'b', 'c')), (array('a', 'b')), (array('a', 'b', 'd')), " \
                  "(array('c', 'd'))  AS t(items)")
    end
    let(:model) { fpm::FPGrowth.new(min_support: 0.5, min_confidence: 0.6).fit(transactions) }

    it "finds the frequent itemset {a, b} with its count" do
      expect(model).to be_a(fpm::FPGrowthModel)
      itemsets = model.freq_itemsets.collect.to_h { |r| [r["items"].sort, r["freq"]] }

      expect(itemsets[%w[a b]]).to eq(3)
      expect(itemsets[%w[a]]).to eq(3)
      expect(itemsets[%w[c]]).to eq(2)
      expect(itemsets.key?(%w[c d])).to be(false)
    end

    it "derives the association rules a to b and b to a with confidence 1.0" do
      rules = model.association_rules.collect.to_h { |r| [[r["antecedent"], r["consequent"]], r["confidence"]] }

      expect(rules[[["a"], ["b"]]]).to eq(1.0)
      expect(rules[[["b"], ["a"]]]).to eq(1.0)
    end

    it "predicts the consequent of a rule for a new transaction" do
      predicted = model.transform(session.sql("SELECT array('a') AS items")).collect.map { |r| r["prediction"] }

      expect(predicted).to eq([["b"]])
    end
  end

  describe "PrefixSpan" do
    # Three sequences; <a> is in all three, <a, b> in two, <c> in one.
    let(:sequences) do
      session.sql("SELECT * FROM VALUES (array(array('a'), array('b'), array('c'))), " \
                  "(array(array('a'), array('b'))), (array(array('a'))) AS t(sequence)")
    end

    it "finds the sequential patterns at the minimum support" do
      patterns = fpm::PrefixSpan.new(min_support: 0.5, max_pattern_length: 3)
                                .find_frequent_sequential_patterns(sequences)
                                .collect.to_h { |r| [r["sequence"], r["freq"]] }

      expect(patterns[[["a"]]]).to eq(3)
      expect(patterns[[["a"], ["b"]]]).to eq(2)
      expect(patterns.key?([["c"]])).to be(false)
    end
  end
end
