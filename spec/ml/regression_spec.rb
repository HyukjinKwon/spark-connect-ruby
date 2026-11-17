# frozen_string_literal: true

require_relative "support"

RSpec.describe SparkConnect::ML::Regression do
  include MlSpecSupport

  let(:reg) { described_class }
  let(:proto) { SparkConnect::Proto }

  # Every concrete regression operator: Ruby constant => Spark (Scala) class.
  let(:operators) do
    {
      AFTSurvivalRegression: "org.apache.spark.ml.regression.AFTSurvivalRegression",
      AFTSurvivalRegressionModel: "org.apache.spark.ml.regression.AFTSurvivalRegressionModel",
      IsotonicRegression: "org.apache.spark.ml.regression.IsotonicRegression",
      IsotonicRegressionModel: "org.apache.spark.ml.regression.IsotonicRegressionModel",
      LinearRegression: "org.apache.spark.ml.regression.LinearRegression",
      LinearRegressionModel: "org.apache.spark.ml.regression.LinearRegressionModel",
      GeneralizedLinearRegression: "org.apache.spark.ml.regression.GeneralizedLinearRegression",
      GeneralizedLinearRegressionModel: "org.apache.spark.ml.regression.GeneralizedLinearRegressionModel",
      FMRegressor: "org.apache.spark.ml.regression.FMRegressor",
      FMRegressionModel: "org.apache.spark.ml.regression.FMRegressionModel",
      DecisionTreeRegressor: "org.apache.spark.ml.regression.DecisionTreeRegressor",
      DecisionTreeRegressionModel: "org.apache.spark.ml.regression.DecisionTreeRegressionModel",
      RandomForestRegressor: "org.apache.spark.ml.regression.RandomForestRegressor",
      RandomForestRegressionModel: "org.apache.spark.ml.regression.RandomForestRegressionModel",
      GBTRegressor: "org.apache.spark.ml.regression.GBTRegressor",
      GBTRegressionModel: "org.apache.spark.ml.regression.GBTRegressionModel",
    }
  end

  describe "operator names" do
    it "maps every concrete class to its Spark class, under org.apache.spark.ml." do
      operators.each do |const, scala|
        klass = reg.const_get(const)
        expect(klass.operator).to eq(scala), const.to_s
        expect(klass.operator).to start_with("org.apache.spark.ml.")
      end
    end

    it "declares an operator on every concrete class and none on the summaries" do
      declared = reg.constants.filter_map do |name|
        klass = reg.const_get(name)
        name if klass.is_a?(Class) && klass <= SparkConnect::ML::Params && klass.operator
      end
      expect(declared).to match_array(operators.keys)
    end

    it "attaches the estimator to its model class" do
      expect(reg::LinearRegression.model_class).to eq(reg::LinearRegressionModel)
      expect(reg::IsotonicRegression.model_class).to eq(reg::IsotonicRegressionModel)
    end
  end

  describe "defaults (Spark 4.3.0)" do
    it "matches the Scala setDefault values" do
      expect(reg::LinearRegression.new.loss).to eq("squaredError")
      expect(reg::LinearRegression.new.epsilon).to eq(1.35)
      expect(reg::LinearRegression.new.solver).to eq("auto")
      expect(reg::GeneralizedLinearRegression.new.family).to eq("gaussian")
      expect(reg::GeneralizedLinearRegression.new.solver).to eq("irls")
      expect(reg::GeneralizedLinearRegression.new.max_iter).to eq(25)
      expect(reg::AFTSurvivalRegression.new.censor_col).to eq("censor")
      expect(reg::IsotonicRegression.new.isotonic).to be(true)
      expect(reg::IsotonicRegression.new.feature_index).to eq(0)
      expect(reg::DecisionTreeRegressor.new.impurity).to eq("variance")
      expect(reg::GBTRegressor.new.loss_type).to eq("squared")
    end

    it "uses the Scala validationTol default (0.01), not the PySpark signature value" do
      expect(reg::GBTRegressor.new.validation_tol).to eq(0.01)
    end

    it "leaves params without a Spark default unset" do
      expect { reg::GeneralizedLinearRegression.new.link }.to raise_error(SparkConnect::IllegalArgumentError)
      expect(reg::GeneralizedLinearRegression.new.is_set?(:link)).to be(false)
    end
  end

  describe "setters" do
    it "chains set_ calls" do
      est = reg::LinearRegression.new.set_reg_param(0.1).set_elastic_net_param(0.5).set_max_iter(10)
      expect([est.reg_param, est.elastic_net_param, est.max_iter]).to eq([0.1, 0.5, 10])
    end
  end

  describe "fit" do
    it "sends an estimator MlCommand with the explicit params keyed by Spark name" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      reg::LinearRegression.new(max_iter: 7, elastic_net_param: 0.2).fit(ml_frame(session))

      fit = first_ml(client, :fit).fit
      expect(fit.estimator.name).to eq("org.apache.spark.ml.regression.LinearRegression")
      expect(fit.estimator.type).to eq(:OPERATOR_TYPE_ESTIMATOR)
      expect(fit.params.params.keys).to contain_exactly("maxIter", "elasticNetParam")
    end

    it "fits the isotonic estimator with its own params" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = reg::IsotonicRegression.new(isotonic: false).fit(ml_frame(session))

      expect(model).to be_a(reg::IsotonicRegressionModel)
      expect(first_ml(client, :fit).fit.params.params.keys).to eq(["isotonic"])
    end
  end

  describe "model attributes" do
    it "fetches an allowlisted attribute by its Scala method name" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = reg::AFTSurvivalRegression.new.fit(ml_frame(session))
      model.scale

      expect(client.last_ml_command.fetch["methods"].map { |m| m["method"] }).to eq(["scale"])
    end

    it "reads the linear regression training summary through the summary chain" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = reg::LinearRegression.new.fit(ml_frame(session))
      expect(model.summary).to be_a(reg::LinearRegressionTrainingSummary)
      model.summary.root_mean_squared_error

      expect(client.last_ml_command.fetch["methods"].map { |m| m["method"] }).to eq(%w[summary rootMeanSquaredError])
    end

    it "evaluates a generalized linear model into its summary" do
      client = MlSpecSupport::FakeMlClient.new
      default = client.responder
      client.responder = lambda do |ml|
        ml.command == :fetch ? proto::MlCommandResult.new(summary: "summary-2") : default.call(ml)
      end
      session = ml_session(client)
      model = reg::GeneralizedLinearRegression.new.fit(ml_frame(session))
      summary = model.evaluate(ml_frame(session))
      expect(summary).to be_a(reg::GeneralizedLinearRegressionSummary)
      summary.aic

      expect(client.last_ml_command.fetch.obj_ref.id).to eq("summary-2")
      expect(client.last_ml_command.fetch["methods"].map { |m| m["method"] }).to eq(["aic"])
    end
  end

  describe "transform" do
    it "builds an MlRelation.transform against the model reference" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      df = ml_frame(session)
      model = reg::DecisionTreeRegressor.new.fit(df)
      out = model.transform(df)

      expect(out.relation.ml_relation.transform.obj_ref.id).to eq("model-1")
    end
  end
end
