# frozen_string_literal: true

# Integration specs for the regression operators, against a live Spark Connect server.
# Datasets are exact for the learner under test where possible: y = 2*x0 + 3*x1 + 1 for the linear
# family, a single step (y = 0 for x <= 4, else 10) for the trees, and y = x for isotonic regression.
RSpec.describe SparkConnect::ML::Regression, if: ENV.fetch("SPARK_REMOTE", nil) do
  before(:all) do
    skip "set SPARK_REMOTE to run integration specs" unless ENV["SPARK_REMOTE"]
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE", nil)).create
  end

  after(:all) do
    @session&.stop
  end

  let(:session) { @session }
  let(:reg) { described_class }

  # [label, features]; the linear rows are exactly y = 2*x0 + 3*x1 + 1.
  let(:linear_rows) do
    [[1.0, [0.0, 0.0]], [3.0, [1.0, 0.0]], [4.0, [0.0, 1.0]], [6.0, [1.0, 1.0]],
     [8.0, [2.0, 1.0]], [9.0, [1.0, 2.0]], [10.0, [3.0, 1.0]], [14.0, [2.0, 3.0]],]
  end
  let(:step_rows) { (1..8).map { |x| [x <= 4 ? 0.0 : 10.0, [x.to_f]] } }
  let(:monotone_rows) { (1..6).map { |x| [x.to_f, [x.to_f]] } }

  # A DataFrame with a double `label`, an `arr` array column and a `features` VectorUDT column.
  def vector_frame(labeled, extra = {})
    types = SparkConnect::Types
    fields = [types.field("label", types.double), types.field("arr", types.array(types.double))]
    fields += extra.keys.map { |name| types.field(name, types.double) }
    data = labeled.each_with_index.map do |(label, arr), i|
      { "label" => label, "arr" => arr }.merge(extra.transform_values { |values| values[i] })
    end
    session.create_data_frame(data, types.struct(*fields))
           .with_column("features", SparkConnect::ML::Functions.array_to_vector(SparkConnect::Column.from_name("arr")))
  end

  def predictions_of(df)
    df.collect.map { |r| r["prediction"] }
  end

  describe "LinearRegression" do
    it "recovers the exact coefficients and intercept of a noiseless linear relation" do
      model = reg::LinearRegression.new.fit(vector_frame(linear_rows))

      expect(model.coefficients.to_a[0]).to be_within(1e-6).of(2.0)
      expect(model.coefficients.to_a[1]).to be_within(1e-6).of(3.0)
      expect(model.intercept).to be_within(1e-6).of(1.0)
    end

    it "reports a perfect fit through its training summary" do
      model = reg::LinearRegression.new.fit(vector_frame(linear_rows))
      summary = model.summary

      expect(summary.r2).to be_within(1e-9).of(1.0)
      expect(summary.root_mean_squared_error).to be_within(1e-6).of(0.0)
      expect(summary.num_instances).to eq(8)
      # n - p - 1 with an intercept: 8 - 2 - 1.
      expect(summary.degrees_of_freedom).to eq(5)
      expect(summary.objective_history).not_to be_empty
    end

    it "predicts the training labels" do
      df = vector_frame(linear_rows)
      model = reg::LinearRegression.new.fit(df)
      expect(predictions_of(model.transform(df))).to all(be_a(Float))
      expect(model.transform(df).collect.map { |r| r["prediction"] - r["label"] }).to all(be_within(1e-6).of(0.0))
    end
  end

  describe "GeneralizedLinearRegression" do
    it "fits the Gaussian identity family to the same exact relation" do
      model = reg::GeneralizedLinearRegression.new.fit(vector_frame(linear_rows))

      expect(model.coefficients.to_a[0]).to be_within(1e-4).of(2.0)
      expect(model.coefficients.to_a[1]).to be_within(1e-4).of(3.0)
      expect(model.intercept).to be_within(1e-4).of(1.0)
    end

    it "reports zero residual deviance and the IRLS solver through its summary" do
      model = reg::GeneralizedLinearRegression.new.fit(vector_frame(linear_rows))

      expect(model.summary.deviance).to be_within(1e-6).of(0.0)
      expect(model.summary.solver).to eq("irls")
      expect(model.summary.num_iterations).to be >= 1
    end

    it "predicts the training labels" do
      df = vector_frame(linear_rows)
      model = reg::GeneralizedLinearRegression.new.fit(df)
      errors = model.transform(df).collect.map { |r| (r["prediction"] - r["label"]).abs }
      expect(errors.max).to be < 1e-4
    end
  end

  describe "AFTSurvivalRegression" do
    it "fits a survival model with positive scale and monotone predicted quantiles" do
      times = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
      df = vector_frame(times.map { |t| [t, [t / 8.0]] }, censor: [1.0] * 8)
      model = reg::AFTSurvivalRegression.new(censor_col: "censor").fit(df)

      expect(model.scale).to be > 0
      expect(model.transform(df).collect.map { |r| r["prediction"] }).to all(be_a(Float))
      quantiles = model.predict_quantiles(SparkConnect::ML::Linalg::Vectors.dense([0.5])).to_a
      expect(quantiles.size).to eq(9)
      expect(quantiles).to eq(quantiles.sort)
    end
  end

  describe "IsotonicRegression" do
    it "interpolates the identity relation between the training points" do
      model = reg::IsotonicRegression.new.fit(vector_frame(monotone_rows))

      expect(model.boundaries.to_a.size).to eq(6)
      expect(model.predict(3.5)).to be_within(1e-9).of(3.5)
      expect(predictions_of(model.transform(vector_frame(monotone_rows)))).to eq([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    end
  end

  describe "FMRegressor" do
    it "fits the linear relation to within a unit of the labels" do
      model = reg::FMRegressor.new(max_iter: 500, step_size: 0.5, seed: 3).fit(vector_frame(linear_rows))
      errors = model.transform(vector_frame(linear_rows)).collect.map { |r| (r["prediction"] - r["label"]).abs }

      expect(model.intercept).to be_a(Float)
      expect(errors.max).to be < 1.0
    end
  end

  describe "DecisionTreeRegressor" do
    it "reproduces the step exactly and reports a split tree" do
      df = vector_frame(step_rows)
      model = reg::DecisionTreeRegressor.new(max_depth: 5, seed: 1).fit(df)

      expect(predictions_of(model.transform(df))).to eq(step_rows.map(&:first))
      expect(model.depth).to be >= 1
      expect(model.feature_importances.to_a.size).to eq(1)
    end
  end

  describe "RandomForestRegressor" do
    it "reproduces the step with three unbootstrapped trees" do
      df = vector_frame(step_rows)
      model = reg::RandomForestRegressor.new(num_trees: 3, bootstrap: false, seed: 1).fit(df)

      expect(predictions_of(model.transform(df))).to eq(step_rows.map(&:first))
      expect(model.get_num_trees).to eq(3)
    end
  end

  describe "GBTRegressor" do
    it "reproduces the step, since the first boosted tree (weight 1.0) fits it exactly" do
      df = vector_frame(step_rows)
      model = reg::GBTRegressor.new(max_iter: 30, seed: 1).fit(df)

      expect(predictions_of(model.transform(df))).to eq(step_rows.map(&:first))
      expect(model.get_num_trees).to eq(30)
      losses = model.evaluate_each_iteration(df, "squared")
      expect(losses.size).to eq(30)
      expect(losses.last).to be <= losses.first
    end
  end
end
