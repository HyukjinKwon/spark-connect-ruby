# frozen_string_literal: true

# Integration specs for the classification operators, against a live Spark Connect server.
# The training set is perfectly separable by the sign of x0 - x1 (label 1 has x0 > x1), so an
# exact learner reproduces the labels; iterative learners are checked against tolerances.
RSpec.describe SparkConnect::ML::Classification, if: ENV.fetch("SPARK_REMOTE", nil) do
  before(:all) do
    skip "set SPARK_REMOTE to run integration specs" unless ENV["SPARK_REMOTE"]
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE", nil)).create
  end

  after(:all) do
    @session&.stop
  end

  let(:session) { @session }
  let(:cls) { described_class }
  # [label, features]: label 1 has x0 > x1 in every row, label 0 has x0 < x1.
  let(:rows) do
    [
      [1.0, [2.0, 0.0]], [1.0, [3.0, 1.0]], [1.0, [2.5, 0.5]], [1.0, [4.0, 1.0]],
      [0.0, [0.0, 2.0]], [0.0, [1.0, 3.0]], [0.0, [0.5, 2.5]], [0.0, [1.0, 4.0]],
    ]
  end
  let(:train) { vector_frame(rows) }

  # A DataFrame with a double `label`, an `arr` array column and a `features` VectorUDT column.
  def vector_frame(labeled)
    types = SparkConnect::Types
    schema = types.struct(types.field("label", types.double), types.field("arr", types.array(types.double)))
    data = labeled.map { |label, arr| { "label" => label, "arr" => arr } }
    session.create_data_frame(data, schema)
           .with_column("features", SparkConnect::ML::Functions.array_to_vector(SparkConnect::Column.from_name("arr")))
  end

  # Fraction of rows whose prediction equals the label.
  def accuracy_of(df)
    out = df.collect
    out.count { |r| r["prediction"] == r["label"] }.fdiv(out.size)
  end

  def predictions_of(df)
    df.collect.map { |r| r["prediction"] }
  end

  describe "LogisticRegression" do
    it "separates the classes and reports a perfect ROC area through its binary summary" do
      model = cls::LogisticRegression.new(max_iter: 100).fit(train)
      coefficients = model.coefficients.to_a

      expect(coefficients[0]).to be > 0
      expect(coefficients[1]).to be < 0
      expect(accuracy_of(model.transform(train))).to eq(1.0)
      # Perfectly separable labels rank every positive above every negative: AUC is 1.0.
      expect(model.summary.area_under_roc).to be_within(1e-9).of(1.0)
    end
  end

  describe "NaiveBayes" do
    it "fits and transforms, and the class priors equal the empirical class frequencies" do
      model = cls::NaiveBayes.new.fit(train)
      out = model.transform(train)

      expect(accuracy_of(out)).to eq(1.0)
      expect(out.collect.first["probability"].to_a.sum).to be_within(1e-9).of(1.0)
      # Four rows per class with smoothing 1.0: (4 + 1) / (8 + 2) = 0.5 for each class.
      expect(model.pi.to_a).to all(be_within(1e-9).of(Math.log(0.5)))
    end
  end

  describe "LinearSVC" do
    it "separates the classes with a positive weight on x0 and a negative one on x1" do
      model = cls::LinearSVC.new(max_iter: 200).fit(train)
      coefficients = model.coefficients.to_a

      expect(coefficients[0]).to be > 0
      expect(coefficients[1]).to be < 0
      expect(accuracy_of(model.transform(train))).to eq(1.0)
    end

    it "reports the training accuracy through its summary" do
      model = cls::LinearSVC.new(max_iter: 200).fit(train)

      expect(model.summary.accuracy).to eq(1.0)
      expect(model.summary.objective_history).not_to be_empty
    end
  end

  describe "FMClassifier" do
    it "fits, outputs probabilities, and reaches a high training accuracy" do
      model = cls::FMClassifier.new(max_iter: 300, step_size: 0.5, seed: 42).fit(train)
      out = model.transform(train)

      expect(out.collect.first["probability"].to_a.size).to eq(2)
      expect(model.summary.accuracy).to be >= 0.75
      expect(predictions_of(out)).to all(be_a(Float))
    end
  end

  describe "MultilayerPerceptronClassifier" do
    it "fits a network with a softmax output over the two classes" do
      model = cls::MultilayerPerceptronClassifier.new(layers: [2, 5, 2], max_iter: 300, seed: 7).fit(train)
      out = model.transform(train)

      expect(out.collect.first["probability"].to_a.size).to eq(2)
      expect(model.summary.accuracy).to be >= 0.75
    end
  end

  describe "DecisionTreeClassifier" do
    it "reproduces the labels exactly and reports a split tree" do
      model = cls::DecisionTreeClassifier.new(max_depth: 5, seed: 1).fit(train)

      expect(accuracy_of(model.transform(train))).to eq(1.0)
      expect(model.depth).to be >= 1
      expect(model.num_nodes).to be >= 3
      importances = model.feature_importances.to_a
      expect(importances.size).to eq(2)
      expect(importances.sum).to be_within(1e-9).of(1.0)
    end
  end

  describe "RandomForestClassifier" do
    it "reproduces the labels with five unbootstrapped trees" do
      model = cls::RandomForestClassifier.new(num_trees: 5, bootstrap: false, seed: 1).fit(train)

      expect(accuracy_of(model.transform(train))).to eq(1.0)
      expect(model.get_num_trees).to eq(5)
      expect(model.feature_importances.to_a.size).to eq(2)
    end

    it "reports the training accuracy through its summary" do
      model = cls::RandomForestClassifier.new(num_trees: 5, bootstrap: false, seed: 1).fit(train)
      expect(model.summary.accuracy).to eq(1.0)
    end
  end

  describe "GBTClassifier" do
    it "reproduces the labels after ten boosting rounds, one evaluation per round" do
      model = cls::GBTClassifier.new(max_iter: 10, seed: 1).fit(train)

      expect(accuracy_of(model.transform(train))).to eq(1.0)
      expect(model.get_num_trees).to eq(10)
      expect(model.evaluate_each_iteration(train).size).to eq(10)
    end
  end
end
