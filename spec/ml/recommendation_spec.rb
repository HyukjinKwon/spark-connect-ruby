# frozen_string_literal: true

require_relative "support"

RSpec.describe SparkConnect::ML::Recommendation do
  include MlSpecSupport

  let(:client) { MlSpecSupport::FakeMlClient.new }
  let(:session) { ml_session(client) }
  let(:frame) { ml_frame(session) }
  let(:recommendation) { described_class }

  describe "ALS" do
    it "names the Scala class" do
      expect(recommendation::ALS.operator).to eq("org.apache.spark.ml.recommendation.ALS")
      expect(recommendation::ALSModel.operator).to eq("org.apache.spark.ml.recommendation.ALSModel")
    end

    it "matches the Spark 4.3.0 defaults" do
      als = recommendation::ALS.new
      expect(als.rank).to eq(10)
      expect(als.reg_param).to eq(0.1)
      expect(als.cold_start_strategy).to eq("nan")
      expect(als.block_size).to eq(4096)
    end

    it "sets params through a chain" do
      als = recommendation::ALS.new
      expect(als.set_rank(4).set_max_iter(3)).to be(als)
      expect(als.rank).to eq(4)
      expect(als.max_iter).to eq(3)
    end

    it "fits into an ALSModel and sends the explicit params keyed by Spark name" do
      model = recommendation::ALS.new(rank: 4).set_max_iter(3).fit(frame)

      fit = first_ml(client, :fit).fit
      expect(fit.estimator.name).to eq("org.apache.spark.ml.recommendation.ALS")
      expect(fit.estimator.type).to eq(:OPERATOR_TYPE_ESTIMATOR)
      expect(fit.params.params.keys).to contain_exactly("rank", "maxIter")
      expect(model).to be_a(recommendation::ALSModel)
    end

    it "sends a Fetch for a model attribute" do
      model = recommendation::ALS.new.fit(frame)
      model.rank

      fetch = client.last_ml_command.fetch
      expect(fetch["methods"].map { |m| m["method"] }).to eq(["rank"])
      expect(fetch.obj_ref.id).to eq("model-1")
    end

    it "builds a relation fetch for recommendForAllUsers with its argument" do
      model = recommendation::ALS.new.fit(frame)
      fetch = model.recommend_for_all_users(3).relation.ml_relation.fetch

      expect(fetch.obj_ref.id).to eq("model-1")
      call = fetch["methods"].first
      expect(call["method"]).to eq("recommendForAllUsers")
      expect(call["args"].first.param.integer).to eq(3)
    end

    it "passes the dataset as an input argument to recommendForUserSubset" do
      model = recommendation::ALS.new.fit(frame)
      call = model.recommend_for_user_subset(frame, 2).relation.ml_relation.fetch["methods"].first

      expect(call["method"]).to eq("recommendForUserSubset")
      expect(call["args"].size).to eq(2)
      expect(call["args"].first.input).not_to be_nil
      expect(call["args"].last.param.integer).to eq(2)
    end

    it "builds an MlRelation.transform on the fitted model" do
      model = recommendation::ALS.new.fit(frame)

      expect(model.transform(frame).relation.ml_relation.transform.obj_ref.id).to eq("model-1")
    end
  end

  it "declares a recommendation operator on every class" do
    [recommendation::ALS, recommendation::ALSModel].each do |klass|
      expect(klass.operator).to start_with("org.apache.spark.ml.recommendation.")
      expect(recommendation.const_get(klass.name.split("::").last)).to eq(klass)
    end
  end
end
