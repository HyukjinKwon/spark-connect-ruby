# frozen_string_literal: true

require_relative "support"

RSpec.describe SparkConnect::ML::FPM do
  include MlSpecSupport

  let(:client) { MlSpecSupport::FakeMlClient.new }
  let(:session) { ml_session(client) }
  let(:frame) { ml_frame(session) }
  let(:fpm) { described_class }

  describe "FPGrowth" do
    it "names the Scala classes" do
      expect(fpm::FPGrowth.operator).to eq("org.apache.spark.ml.fpm.FPGrowth")
      expect(fpm::FPGrowthModel.operator).to eq("org.apache.spark.ml.fpm.FPGrowthModel")
    end

    it "matches the Spark 4.3.0 defaults, and leaves numPartitions unset" do
      fp = fpm::FPGrowth.new
      expect(fp.min_support).to eq(0.3)
      expect(fp.min_confidence).to eq(0.8)
      expect(fp.items_col).to eq("items")
      expect(fp.is_defined?(:num_partitions)).to be(false)
    end

    it "sets params through a chain" do
      fp = fpm::FPGrowth.new
      expect(fp.set_min_support(0.5).set_min_confidence(0.6)).to be(fp)
      expect(fp.min_support).to eq(0.5)
    end

    it "fits into a FPGrowthModel and sends the explicit params keyed by Spark name" do
      model = fpm::FPGrowth.new(min_support: 0.5).fit(frame)

      fit = first_ml(client, :fit).fit
      expect(fit.estimator.name).to eq("org.apache.spark.ml.fpm.FPGrowth")
      expect(fit.estimator.type).to eq(:OPERATOR_TYPE_ESTIMATOR)
      expect(fit.params.params.keys).to eq(["minSupport"])
      expect(model).to be_a(fpm::FPGrowthModel)
    end

    it "builds a relation fetch for freqItemsets and associationRules" do
      model = fpm::FPGrowth.new.fit(frame)

      [[:freq_itemsets, "freqItemsets"], [:association_rules, "associationRules"]].each do |ruby, spark|
        fetch = model.public_send(ruby).relation.ml_relation.fetch
        expect(fetch.obj_ref.id).to eq("model-1")
        expect(fetch["methods"].map { |m| m["method"] }).to eq([spark])
      end
    end

    it "builds an MlRelation.transform on the fitted model" do
      model = fpm::FPGrowth.new.fit(frame)

      expect(model.transform(frame).relation.ml_relation.transform.obj_ref.id).to eq("model-1")
    end
  end

  describe "PrefixSpan" do
    let(:prefix_span) { fpm::PrefixSpan.new(min_support: 0.5, max_pattern_length: 3) }

    it "names the Scala class and matches the Spark defaults" do
      expect(fpm::PrefixSpan.operator).to eq("org.apache.spark.ml.fpm.PrefixSpan")
      expect(fpm::PrefixSpan.new.min_support).to eq(0.1)
      expect(fpm::PrefixSpan.new.max_pattern_length).to eq(10)
      expect(fpm::PrefixSpan.new.max_local_proj_db_size).to eq(32_000_000)
      expect(fpm::PrefixSpan.new.sequence_col).to eq("sequence")
    end

    it "sends the helper call with the dataset and the params as arguments" do
      df = prefix_span.find_frequent_sequential_patterns(frame)

      fetch = df.relation.ml_relation.fetch
      expect(fetch.obj_ref.id).to eq(SparkConnect::ML::Rpc::HELPER_ID)
      call = fetch["methods"].first
      expect(call["method"]).to eq("prefixSpanFindFrequentSequentialPatterns")
      args = call["args"]
      expect(args.size).to eq(5)
      expect(args[0].input).not_to be_nil
      expect(args[1].param.double).to eq(0.5)
      expect(args[2].param.integer).to eq(3)
      expect(args[3].param.integer).to eq(32_000_000)
      expect(args[4].param.string).to eq("sequence")
    end
  end

  it "declares an fpm operator on every class" do
    [fpm::FPGrowth, fpm::FPGrowthModel, fpm::PrefixSpan].each do |klass|
      expect(klass.operator).to start_with("org.apache.spark.ml.fpm.")
      expect(fpm.const_get(klass.name.split("::").last)).to eq(klass)
    end
  end
end
