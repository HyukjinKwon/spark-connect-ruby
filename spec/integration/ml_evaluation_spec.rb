# frozen_string_literal: true

# Integration specs for the evaluators ({SparkConnect::ML::Evaluation}). Each
# evaluator runs on a tiny predictions DataFrame on a live Spark Connect server
# and the metric is checked against a value derived by hand.
RSpec.describe SparkConnect::ML::Evaluation, if: ENV.fetch("SPARK_REMOTE", nil) do
  before(:all) do
    skip "set SPARK_REMOTE to run integration specs" unless ENV["SPARK_REMOTE"]
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE", nil)).create
  end

  after(:all) do
    @session&.stop
  end

  let(:session) { @session }
  let(:ev) { described_class }

  describe "RegressionEvaluator" do
    # Errors are 0, 0, 2: SSE = 4, MSE = 4/3, MAE = 2/3, mean label = 2, SST = 2.
    let(:predictions) do
      session.create_data_frame([
                                  { "label" => 1.0, "prediction" => 1.0 },
                                  { "label" => 2.0, "prediction" => 2.0 },
                                  { "label" => 3.0, "prediction" => 5.0 },
                                ])
    end

    it "computes rmse by default" do
      expect(ev::RegressionEvaluator.new.evaluate(predictions)).to be_within(1e-6).of(Math.sqrt(4.0 / 3.0))
    end

    it "computes mse, mae and r2 when selected" do
      evaluator = ev::RegressionEvaluator.new
      expect(evaluator.set_metric_name("mse").evaluate(predictions)).to be_within(1e-6).of(4.0 / 3.0)
      expect(evaluator.set_metric_name("mae").evaluate(predictions)).to be_within(1e-6).of(2.0 / 3.0)
      expect(evaluator.set_metric_name("r2").evaluate(predictions)).to be_within(1e-6).of(-1.0)
    end

    it "is smaller-is-better for rmse and larger-is-better for r2" do
      evaluator = ev::RegressionEvaluator.new
      expect(evaluator.larger_better?).to be(false)
      expect(evaluator.set_metric_name("r2").larger_better?).to be(true)
    end
  end

  describe "BinaryClassificationEvaluator" do
    it "is 1.0 for areaUnderROC when the scores separate the classes" do
      df = session.create_data_frame([
                                       { "rawPrediction" => 0.1, "label" => 0.0 },
                                       { "rawPrediction" => 0.2, "label" => 0.0 },
                                       { "rawPrediction" => 0.8, "label" => 1.0 },
                                       { "rawPrediction" => 0.9, "label" => 1.0 },
                                     ])
      expect(ev::BinaryClassificationEvaluator.new.evaluate(df)).to be_within(1e-9).of(1.0)
    end

    it "is 0.0 for areaUnderROC when the ranking is inverted" do
      df = session.create_data_frame([
                                       { "rawPrediction" => 0.9, "label" => 0.0 },
                                       { "rawPrediction" => 0.8, "label" => 0.0 },
                                       { "rawPrediction" => 0.2, "label" => 1.0 },
                                       { "rawPrediction" => 0.1, "label" => 1.0 },
                                     ])
      expect(ev::BinaryClassificationEvaluator.new.evaluate(df)).to be_within(1e-9).of(0.0)
    end
  end

  describe "MulticlassClassificationEvaluator" do
    # Three of four predictions are right: accuracy 0.75.
    let(:predictions) do
      session.create_data_frame([
                                  { "label" => 0.0, "prediction" => 0.0 },
                                  { "label" => 1.0, "prediction" => 1.0 },
                                  { "label" => 2.0, "prediction" => 1.0 },
                                  { "label" => 2.0, "prediction" => 2.0 },
                                ])
    end

    it "computes accuracy" do
      expect(ev::MulticlassClassificationEvaluator.new.set_metric_name("accuracy").evaluate(predictions))
        .to be_within(1e-9).of(0.75)
    end

    it "reports accuracy as larger-is-better" do
      expect(ev::MulticlassClassificationEvaluator.new.set_metric_name("accuracy").larger_better?).to be(true)
    end
  end

  describe "ClusteringEvaluator" do
    it "is close to 1.0 for silhouette on two far-apart clusters" do
      df = session.create_data_frame([
                                       { "a" => 0.0, "b" => 0.0, "prediction" => 0 },
                                       { "a" => 0.0, "b" => 1.0, "prediction" => 0 },
                                       { "a" => 100.0, "b" => 100.0, "prediction" => 1 },
                                       { "a" => 100.0, "b" => 101.0, "prediction" => 1 },
                                     ])
      features = SparkConnect::ML::FeatureTransformers::VectorAssembler.new
                                                                       .set_input_cols(%w[a b]).set_output_col("features")
                                                                       .transform(df)
      silhouette = ev::ClusteringEvaluator.new.set_features_col("features").evaluate(features)
      # Each point's mean squared distance to its own cluster is 1 (or 1 for
      # its neighbour); the other cluster is about 20000 away, so s is ~0.99995.
      expect(silhouette).to be_within(1e-3).of(1.0)
    end
  end

  describe "RankingEvaluator" do
    # Row 1: relevant {1,2,3,4,5} ranked at 1,3,6,9,10: AP = (1 + 2/3 + 3/6 + 4/9 + 5/10) / 5.
    # Row 2: relevant {1,2} ranked at 1,2: AP = 1. Mean = (0.6222... + 1) / 2.
    let(:predictions) do
      session.create_data_frame([
                                  { "prediction" => [1.0, 6.0, 2.0, 7.0, 8.0, 3.0, 9.0, 10.0, 4.0, 5.0],
                                    "label" => [1.0, 2.0, 3.0, 4.0, 5.0], },
                                  { "prediction" => [1.0, 2.0], "label" => [1.0, 2.0] },
                                ])
    end

    it "computes meanAveragePrecision by default" do
      expected = (((1.0 + (2.0 / 3.0) + (3.0 / 6.0) + (4.0 / 9.0) + (5.0 / 10.0)) / 5.0) + 1.0) / 2.0
      expect(ev::RankingEvaluator.new.evaluate(predictions)).to be_within(1e-6).of(expected)
    end
  end

  describe "MultilabelClassificationEvaluator" do
    # Row 1 is exact. Row 2 predicts {0,1} for labels {0,2}.
    let(:predictions) do
      session.create_data_frame([
                                  { "prediction" => [0.0, 1.0], "label" => [0.0, 1.0] },
                                  { "prediction" => [0.0, 1.0], "label" => [0.0, 2.0] },
                                ])
    end

    it "computes subsetAccuracy (exact label-set matches)" do
      evaluator = ev::MultilabelClassificationEvaluator.new.set_metric_name("subsetAccuracy")
      expect(evaluator.evaluate(predictions)).to be_within(1e-9).of(0.5)
    end

    it "computes accuracy (mean Jaccard index of the label sets)" do
      evaluator = ev::MultilabelClassificationEvaluator.new.set_metric_name("accuracy")
      # Row 2 Jaccard: |{0}| / |{0,1,2}| = 1/3, so (1 + 1/3) / 2.
      expect(evaluator.evaluate(predictions)).to be_within(1e-9).of((1.0 + (1.0 / 3.0)) / 2.0)
    end
  end
end
