# frozen_string_literal: true

require_relative "support"

RSpec.describe SparkConnect::ML::Classification do
  include MlSpecSupport

  let(:cls) { described_class }
  let(:proto) { SparkConnect::Proto }

  # Every concrete classification operator: Ruby constant => Spark (Scala) class.
  let(:operators) do
    {
      LogisticRegression: "org.apache.spark.ml.classification.LogisticRegression",
      LogisticRegressionModel: "org.apache.spark.ml.classification.LogisticRegressionModel",
      NaiveBayes: "org.apache.spark.ml.classification.NaiveBayes",
      NaiveBayesModel: "org.apache.spark.ml.classification.NaiveBayesModel",
      LinearSVC: "org.apache.spark.ml.classification.LinearSVC",
      LinearSVCModel: "org.apache.spark.ml.classification.LinearSVCModel",
      FMClassifier: "org.apache.spark.ml.classification.FMClassifier",
      FMClassificationModel: "org.apache.spark.ml.classification.FMClassificationModel",
      MultilayerPerceptronClassifier: "org.apache.spark.ml.classification.MultilayerPerceptronClassifier",
      MultilayerPerceptronClassificationModel: "org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel",
      DecisionTreeClassifier: "org.apache.spark.ml.classification.DecisionTreeClassifier",
      DecisionTreeClassificationModel: "org.apache.spark.ml.classification.DecisionTreeClassificationModel",
      RandomForestClassifier: "org.apache.spark.ml.classification.RandomForestClassifier",
      RandomForestClassificationModel: "org.apache.spark.ml.classification.RandomForestClassificationModel",
      GBTClassifier: "org.apache.spark.ml.classification.GBTClassifier",
      GBTClassificationModel: "org.apache.spark.ml.classification.GBTClassificationModel",
    }
  end

  describe "operator names" do
    it "maps every concrete class to its Spark class, under org.apache.spark.ml." do
      operators.each do |const, scala|
        klass = cls.const_get(const)
        expect(klass.operator).to eq(scala), const.to_s
        expect(klass.operator).to start_with("org.apache.spark.ml.")
      end
    end

    it "declares an operator on every concrete class and none on the Ruby base classes" do
      declared = cls.constants.filter_map do |name|
        klass = cls.const_get(name)
        name if klass.is_a?(Class) && klass <= SparkConnect::ML::Params && klass.operator
      end
      expect(declared).to match_array(operators.keys)
      expect(cls::ClassificationModel.operator).to be_nil
      expect(cls::ProbabilisticClassifier.operator).to be_nil
    end

    it "attaches the estimator to its model class" do
      expect(cls::NaiveBayes.model_class).to eq(cls::NaiveBayesModel)
      expect(cls::GBTClassifier.model_class).to eq(cls::GBTClassificationModel)
    end
  end

  describe "defaults (Spark 4.3.0)" do
    it "matches the Scala setDefault values" do
      expect(cls::NaiveBayes.new.smoothing).to eq(1.0)
      expect(cls::NaiveBayes.new.model_type).to eq("multinomial")
      expect(cls::LinearSVC.new.threshold).to eq(0.0)
      expect(cls::LinearSVC.new.aggregation_depth).to eq(2)
      expect(cls::DecisionTreeClassifier.new.impurity).to eq("gini")
      expect(cls::DecisionTreeClassifier.new.leaf_col).to eq("")
      expect(cls::RandomForestClassifier.new.num_trees).to eq(20)
      expect(cls::RandomForestClassifier.new.feature_subset_strategy).to eq("auto")
      expect(cls::GBTClassifier.new.feature_subset_strategy).to eq("all")
      expect(cls::GBTClassifier.new.validation_tol).to eq(0.01)
      expect(cls::MultilayerPerceptronClassifier.new.solver).to eq("l-bfgs")
      expect(cls::MultilayerPerceptronClassifier.new.block_size).to eq(128)
    end

    it "leaves params without a Spark default unset" do
      expect { cls::MultilayerPerceptronClassifier.new.layers }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { cls::NaiveBayes.new.weight_col }.to raise_error(SparkConnect::IllegalArgumentError)
      expect(cls::NaiveBayes.new.is_set?(:weight_col)).to be(false)
    end

    it "uses Spark's spark_name for camelCase params that camelize would miss" do
      expect(cls::DecisionTreeClassifier.params.fetch(:max_memory_in_mb).spark_name).to eq("maxMemoryInMB")
      expect(cls::LinearSVC.params.fetch(:max_block_size_in_mb).spark_name).to eq("maxBlockSizeInMB")
    end
  end

  describe "setters" do
    it "chains set_ calls" do
      est = cls::RandomForestClassifier.new
                                       .set_num_trees(5)
                                       .set_max_depth(3)
                                       .set_bootstrap(false)
      expect(est).to be_a(cls::RandomForestClassifier)
      expect([est.num_trees, est.max_depth, est.bootstrap]).to eq([5, 3, false])
    end
  end

  describe "fit" do
    it "sends an estimator MlCommand with the explicit params keyed by Spark name" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      cls::LinearSVC.new(max_iter: 7, reg_param: 0.1).fit(ml_frame(session))

      fit = first_ml(client, :fit).fit
      expect(fit.estimator.name).to eq("org.apache.spark.ml.classification.LinearSVC")
      expect(fit.estimator.type).to eq(:OPERATOR_TYPE_ESTIMATOR)
      expect(fit.params.params.keys).to contain_exactly("maxIter", "regParam")
    end

    it "returns a model of the declared model class" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = cls::GBTClassifier.new(max_iter: 3).fit(ml_frame(session))

      expect(model).to be_a(cls::GBTClassificationModel)
      expect(model.ref_id).to eq("model-1")
    end
  end

  describe "model attributes" do
    it "fetches an allowlisted attribute by its Scala method name" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = cls::LinearSVC.new.fit(ml_frame(session))
      model.intercept

      fetch = client.last_ml_command.fetch
      expect(fetch.obj_ref.id).to eq("model-1")
      expect(fetch["methods"].map { |m| m["method"] }).to eq(["intercept"])
    end

    it "reads the training summary through the model's summary chain" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = cls::LinearSVC.new.fit(ml_frame(session))
      expect(model.summary).to be_a(cls::LinearSVCTrainingSummary)
      model.summary.accuracy

      expect(client.last_ml_command.fetch["methods"].map { |m| m["method"] }).to eq(%w[summary accuracy])
    end

    it "evaluates into a summary object and reads its metric" do
      client = MlSpecSupport::FakeMlClient.new
      default = client.responder
      client.responder = lambda do |ml|
        ml.command == :fetch ? proto::MlCommandResult.new(summary: "summary-1") : default.call(ml)
      end
      session = ml_session(client)
      model = cls::LinearSVC.new.fit(ml_frame(session))
      summary = model.evaluate(ml_frame(session))
      expect(summary).to be_a(cls::LinearSVCSummary)
      summary.area_under_roc

      fetch = client.last_ml_command.fetch
      expect(fetch.obj_ref.id).to eq("summary-1")
      expect(fetch["methods"].map { |m| m["method"] }).to eq(["areaUnderROC"])
    end
  end

  describe "transform" do
    it "builds an MlRelation.transform against the model reference" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      df = ml_frame(session)
      model = cls::NaiveBayes.new.fit(df)
      out = model.transform(df)

      transform = out.relation.ml_relation.transform
      expect(transform.obj_ref.id).to eq("model-1")
    end
  end
end
