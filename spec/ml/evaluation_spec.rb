# frozen_string_literal: true

require_relative "support"

EVAL = SparkConnect::ML::Evaluation

# Every evaluator of the family, keyed by class.
EVAL_CLASSES = {
  EVAL::RegressionEvaluator => "RegressionEvaluator",
  EVAL::BinaryClassificationEvaluator => "BinaryClassificationEvaluator",
  EVAL::MulticlassClassificationEvaluator => "MulticlassClassificationEvaluator",
  EVAL::MultilabelClassificationEvaluator => "MultilabelClassificationEvaluator",
  EVAL::ClusteringEvaluator => "ClusteringEvaluator",
  EVAL::RankingEvaluator => "RankingEvaluator",
}.freeze

# Unit specs for the evaluators. The metric itself is computed by the server
# (see spec/integration/ml_evaluation_spec.rb); here the fake client answers 0.5.
RSpec.describe SparkConnect::ML::Evaluation do
  include MlSpecSupport

  let(:session) { ml_session }
  let(:predictions) { ml_frame(session) }

  it "declares the six allowlisted evaluators" do
    expect(EVAL_CLASSES.size).to eq(6)
    expect(EVAL.constants.map { |c| EVAL.const_get(c) }.grep(Class).select { |k| k < SparkConnect::ML::Evaluator })
      .to match_array(EVAL_CLASSES.keys)
  end

  describe "operator names" do
    it "uses the org.apache.spark.ml.evaluation Scala class names" do
      EVAL_CLASSES.each do |klass, short|
        expect(klass.operator).to eq("org.apache.spark.ml.evaluation.#{short}")
      end
    end

    it "gives every declared class an org.apache.spark.ml. operator" do
      EVAL_CLASSES.each_key do |klass|
        expect(klass.operator).to start_with("org.apache.spark.ml.")
      end
    end
  end

  describe "defaults" do
    it "RegressionEvaluator defaults to rmse with prediction and label columns" do
      evaluator = EVAL::RegressionEvaluator.new
      expect(evaluator.metric_name).to eq("rmse")
      expect(evaluator.prediction_col).to eq("prediction")
      expect(evaluator.label_col).to eq("label")
      expect(evaluator.through_origin).to be(false)
    end

    it "BinaryClassificationEvaluator defaults to areaUnderROC and 1000 bins" do
      evaluator = EVAL::BinaryClassificationEvaluator.new
      expect(evaluator.metric_name).to eq("areaUnderROC")
      expect(evaluator.num_bins).to eq(1000)
      expect(evaluator.raw_prediction_col).to eq("rawPrediction")
    end

    it "MulticlassClassificationEvaluator defaults to f1, beta 1.0 and eps 1e-15" do
      evaluator = EVAL::MulticlassClassificationEvaluator.new
      expect(evaluator.metric_name).to eq("f1")
      expect(evaluator.beta).to eq(1.0)
      expect(evaluator.eps).to eq(1e-15)
      expect(evaluator.probability_col).to eq("probability")
    end

    it "MultilabelClassificationEvaluator defaults to f1Measure" do
      expect(EVAL::MultilabelClassificationEvaluator.new.metric_name).to eq("f1Measure")
    end

    it "ClusteringEvaluator defaults to silhouette with squaredEuclidean" do
      evaluator = EVAL::ClusteringEvaluator.new
      expect(evaluator.metric_name).to eq("silhouette")
      expect(evaluator.distance_measure).to eq("squaredEuclidean")
      expect(evaluator.features_col).to eq("features")
    end

    it "RankingEvaluator defaults to meanAveragePrecision and k 10" do
      evaluator = EVAL::RankingEvaluator.new
      expect(evaluator.metric_name).to eq("meanAveragePrecision")
      expect(evaluator.k).to eq(10)
    end

    it "leaves weightCol unset" do
      expect(EVAL::RegressionEvaluator.new.is_set?(:weight_col)).to be(false)
      expect { EVAL::RegressionEvaluator.new.weight_col }.to raise_error(SparkConnect::IllegalArgumentError)
    end
  end

  describe "set_ chains" do
    it "returns the evaluator from each setter" do
      evaluator = EVAL::RegressionEvaluator.new
      expect(evaluator.set_metric_name("mae").set_label_col("y").set_prediction_col("p")).to be(evaluator)
      expect(evaluator.metric_name).to eq("mae")
      expect(evaluator.label_col).to eq("y")
    end

    it "coerces the metric label to a double" do
      evaluator = EVAL::MulticlassClassificationEvaluator.new.set_metric_label(2)
      expect(evaluator.metric_label).to eq(2.0)
    end
  end

  describe "#larger_better?" do
    it "is false for the default RegressionEvaluator metric rmse" do
      expect(EVAL::RegressionEvaluator.new.larger_better?).to be(false)
    end

    it "is true for r2 and var, false for mse, rmse and mae" do
      evaluator = EVAL::RegressionEvaluator.new
      %w[r2 var].each do |metric|
        expect(evaluator.set_metric_name(metric).larger_better?).to be(true)
      end
      %w[mse rmse mae].each do |metric|
        expect(evaluator.set_metric_name(metric).larger_better?).to be(false)
      end
    end

    it "is true for the binary evaluator" do
      expect(EVAL::BinaryClassificationEvaluator.new.larger_better?).to be(true)
    end

    it "is false for the error metrics of the multiclass and multilabel evaluators" do
      multiclass = EVAL::MulticlassClassificationEvaluator.new
      expect(multiclass.larger_better?).to be(true)
      expect(multiclass.set_metric_name("logLoss").larger_better?).to be(false)
      expect(multiclass.set_metric_name("accuracy").larger_better?).to be(true)

      multilabel = EVAL::MultilabelClassificationEvaluator.new
      expect(multilabel.larger_better?).to be(true)
      expect(multilabel.set_metric_name("hammingLoss").larger_better?).to be(false)
    end

    it "is true for clustering and ranking" do
      expect(EVAL::ClusteringEvaluator.new.larger_better?).to be(true)
      expect(EVAL::RankingEvaluator.new.larger_better?).to be(true)
    end
  end

  describe "#evaluate" do
    it "builds an MlCommand.evaluate with the Spark evaluator name and type" do
      client = MlSpecSupport::FakeMlClient.new
      evaluator = EVAL::RegressionEvaluator.new
      evaluator.evaluate(ml_frame(ml_session(client)))

      command = first_ml(client, :evaluate)
      expect(command).not_to be_nil
      expect(command.evaluate.evaluator.name).to eq("org.apache.spark.ml.evaluation.RegressionEvaluator")
      expect(command.evaluate.evaluator.type).to eq(:OPERATOR_TYPE_EVALUATOR)
      expect(command.evaluate.evaluator.uid).to eq(evaluator.uid)
    end

    it "sends explicitly set params keyed by Spark name" do
      client = MlSpecSupport::FakeMlClient.new
      evaluator = EVAL::MulticlassClassificationEvaluator.new.set_metric_name("accuracy").set_metric_label(1.0)
      evaluator.evaluate(ml_frame(ml_session(client)))

      params = first_ml(client, :evaluate).evaluate.params.params
      expect(params.keys).to contain_exactly("metricName", "metricLabel")
      expect(params["metricName"].string).to eq("accuracy")
      expect(params["metricLabel"].double).to eq(1.0)
    end

    it "returns the metric decoded from the server result" do
      client = MlSpecSupport::FakeMlClient.new
      expect(EVAL::RankingEvaluator.new.evaluate(ml_frame(ml_session(client)))).to eq(0.5)
    end
  end
end
