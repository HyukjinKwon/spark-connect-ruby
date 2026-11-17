# frozen_string_literal: true

require "fileutils"
require "tmpdir"

# Integration specs for the ML framework against a live Spark 4.3 Connect server:
# vector values in results, save/load round trips, pipelines, model lifecycle,
# statistics and the vector functions. Only runs when SPARK_REMOTE is set.
RSpec.describe "Spark ML (framework)", :integration, if: ENV.fetch("SPARK_REMOTE", nil) do
  let(:ml) { SparkConnect::ML }
  let(:types) { SparkConnect::Types }
  let(:col) { SparkConnect::Column }
  let(:session) { @session }
  let(:separable) do
    rows = [
      { "label" => 1.0, "arr" => [1.0, 2.0] },
      { "label" => 1.0, "arr" => [2.0, 1.0] },
      { "label" => 0.0, "arr" => [-1.0, -2.0] },
      { "label" => 0.0, "arr" => [-2.0, -1.0] },
    ]
    schema = types.struct(types.field("label", types.double), types.field("arr", types.array(types.double)))
    session.create_data_frame(rows, schema).with_column("features", ml::Functions.array_to_vector(col.from_name("arr")))
  end

  before(:all) do
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE")).create
  end

  after(:all) do
    @session&.stop
  end

  describe "vector values" do
    it "decodes VectorUDT columns into vector objects" do
      rows = separable.select("features").collect
      expect(rows.first["features"]).to be_a(ml::Linalg::Vector)
      expect(rows.map { |r| r["features"].to_a }.sort).to eq([[-2.0, -1.0], [-1.0, -2.0], [1.0, 2.0], [2.0, 1.0]])
    end

    it "converts a vector column back to an array" do
      arr = separable.select(ml::Functions.vector_to_array(col.from_name("features")).alias("a")).collect
      expect(arr.map { |r| r["a"] }).to include([-2.0, -1.0])
    end

    it "decodes a DenseMatrix from a matrix literal round trip through a model parameter" do
      matrix = ml::Matrices.dense(2, 1, [0.5, 1.5])
      expect(ml::Serialize.decode(ml::Serialize.literal(matrix)).to_a).to eq([[0.5], [1.5]])
    end
  end

  describe "fit, transform and summaries" do
    let(:lr) { ml::Classification::LogisticRegression.new(max_iter: 20, reg_param: 0.01) }

    it "fits, transforms and reports a training summary" do
      model = lr.fit(separable)
      predictions = model.transform(separable).collect.map { |r| [r["label"], r["prediction"]] }
      expect(predictions).to all(satisfy { |pair| pair[0] == pair[1] })
      expect(model.summary.area_under_roc).to be_within(1e-9).of(1.0)
      expect(model.num_classes).to eq(2)
      expect(model.coefficients).to be_a(ml::Linalg::DenseVector)
    end

    it "returns the evaluation summary of another dataset" do
      model = lr.fit(separable)
      summary = model.evaluate(separable)
      expect(summary.accuracy).to be_within(1e-9).of(1.0)
    end

    it "exposes the training predictions as a DataFrame" do
      model = lr.fit(separable)
      expect(model.summary.predictions.count).to eq(4)
    end
  end

  describe "save and load" do
    let(:dir) { Dir.mktmpdir("ml-core") }

    after { FileUtils.rm_rf(dir) }

    it "round-trips an estimator" do
      path = File.join(dir, "lr")
      ml::Classification::LogisticRegression.new(max_iter: 7).save(path, session: session)
      expect(ml::Classification::LogisticRegression.load(path, session: session).max_iter).to eq(7)
    end

    it "round-trips a model and overwrites an existing path" do
      model = ml::Classification::LogisticRegression.new(max_iter: 20).fit(separable)
      path = File.join(dir, "model")
      model.save(path, session: session)
      model.save(path, overwrite: true, session: session)
      loaded = ml::Classification::LogisticRegressionModel.load(path, session: session)
      diffs = loaded.coefficients.to_a.zip(model.coefficients.to_a).map { |a, b| (a - b).abs }
      expect(diffs).to all(be < 1e-9)
      expect(loaded.transform(separable).collect.size).to eq(4)
    end

    it "round-trips a pipeline model" do
      stage1 = ml::Classification::LogisticRegression.new(max_iter: 5)
      stage2 = ml::Classification::LogisticRegression.new(max_iter: 5, prediction_col: "p2",
                                                          probability_col: "pr2", raw_prediction_col: "rp2")
      pipeline_model = ml::Pipeline.new(stages: [stage1, stage2]).fit(separable)
      path = File.join(dir, "pipeline")
      pipeline_model.save(path, session: session)
      loaded = ml::PipelineModel.load(path, session: session)
      expect(loaded.stages_list.map(&:class)).to eq([ml::Classification::LogisticRegressionModel] * 2)
      expect(loaded.transform(separable).collect.first.to_h.keys).to include("p2")
    end

    it "round-trips a pipeline estimator" do
      path = File.join(dir, "pipeline-est")
      ml::Pipeline.new(stages: [ml::Classification::LogisticRegression.new(max_iter: 3)]).save(path, session: session)
      expect(ml::Pipeline.load(path, session: session).stages_list.first.max_iter).to eq(3)
    end
  end

  describe "pipelines" do
    it "fits every stage in order and applies the fitted pipeline" do
      stage1 = ml::Classification::LogisticRegression.new(max_iter: 10)
      stage2 = ml::Classification::LogisticRegression.new(max_iter: 10, prediction_col: "p2",
                                                          probability_col: "pr2", raw_prediction_col: "rp2")
      model = ml::Pipeline.new(stages: [stage1, stage2]).fit(separable)
      expect(model.transform(separable).collect.size).to eq(4)
    end
  end

  describe "model lifecycle" do
    it "releases a model without error" do
      model = ml::Classification::LogisticRegression.new(max_iter: 3).fit(separable)
      expect { model.release }.not_to raise_error
    end

    it "cleans the server ML cache on Spark 4.1 and later" do
      skip "CleanCache needs Spark 4.1+" unless server_spark_version_at_least?("4.1")

      ml::Classification::LogisticRegression.new(max_iter: 3).fit(separable)
      expect(ml::Rpc.clean_cache(session)).to be_a(Integer)
    end
  end

  describe "statistics" do
    let(:vectors) { separable.select("features", "label") }

    it "computes a Pearson correlation matrix over a vector column" do
      row = ml::Stat::Correlation.corr(vectors, "features").collect.first
      expect(row).not_to be_nil
    end

    it "computes summary statistics with Summarizer" do
      stats = vectors.select(ml::Stat::Summarizer.metrics("mean", "count").summary(col.from_name("features")).alias("s"))
      row = stats.select("s.mean", "s.count").collect.first
      expect(row["count"]).to eq(4)
      expect(row["mean"].to_a).to all(be_within(1e-9).of(0.0))
    end

    it "runs a chi-squared test over a categorical features column" do
      df = session.create_data_frame(
        [{ "label" => 0.0, "f" => 0.0 }, { "label" => 0.0, "f" => 1.0 }, { "label" => 1.0, "f" => 1.0 }, { "label" => 1.0, "f" => 0.0 }]
      ).with_column("f", ml::Functions.array_to_vector(SparkConnect::F.array(col.from_name("f"))))
      expect(ml::Stat::ChiSquareTest.test(df, "f", "label").collect.size).to eq(1)
    end

    it "runs a Kolmogorov-Smirnov test against the normal distribution" do
      df = session.create_data_frame([{ "x" => 0.0 }, { "x" => 0.1 }, { "x" => -0.1 }, { "x" => 0.05 }])
      row = ml::Stat::KolmogorovSmirnovTest.test(df, "x", "norm", 0.0, 1.0).collect.first
      expect(row["pValue"]).to be_between(0.0, 1.0)
    end
  end

  describe "tuning" do
    let(:big) do
      rows = (-20...20).map do |i|
        x = i + 0.5
        { "label" => x.positive? ? 1.0 : 0.0, "arr" => [x, (x * 0.5) + 0.1] }
      end
      schema = types.struct(types.field("label", types.double), types.field("arr", types.array(types.double)))
      session.create_data_frame(rows, schema).with_column("features", ml::Functions.array_to_vector(col.from_name("arr")))
    end
    let(:lr) { ml::Classification::LogisticRegression.new(max_iter: 10) }
    let(:grid) { ml::Tuning::ParamGridBuilder.new.add_grid(lr.param(:reg_param), [0.0, 0.1]).build }
    let(:evaluator) { ml::Evaluation::BinaryClassificationEvaluator.new }

    it "cross-validates, keeps the metrics of every map and refits the best one" do
      model = ml::CrossValidator.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator,
                                     num_folds: 2, seed: 42).fit(big)
      expect(model.avg_metrics.size).to eq(2)
      expect(model.avg_metrics).to all(be_between(0.0, 1.0))
      expect(model.transform(big).collect.size).to eq(40)
    end

    it "runs a train-validation split" do
      model = ml::TrainValidationSplit.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator,
                                           seed: 7).fit(big)
      expect(model.validation_metrics.size).to eq(2)
      expect(model.best_model).to be_a(ml::Classification::LogisticRegressionModel)
    end

    it "saves and loads a cross-validation model" do
      dir = Dir.mktmpdir("ml-cv")
      model = ml::CrossValidator.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator,
                                     num_folds: 2, seed: 42).fit(big)
      path = File.join(dir, "cvm")
      model.save(path, session: session)
      loaded = ml::CrossValidatorModel.load(path, session: session)
      expect(loaded.avg_metrics).to eq(model.avg_metrics)
      expect(loaded.best_model).to be_a(ml::Classification::LogisticRegressionModel)
    ensure
      FileUtils.rm_rf(dir)
    end

    it "saves and loads a cross-validator estimator with its param maps" do
      dir = Dir.mktmpdir("ml-cv-est")
      cv = ml::CrossValidator.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator, num_folds: 3)
      path = File.join(dir, "cv")
      cv.save(path, session: session)
      loaded = ml::CrossValidator.load(path, session: session)
      expect(loaded.num_folds).to eq(3)
      expect(loaded.get(:estimator_param_maps).size).to eq(2)
    ensure
      FileUtils.rm_rf(dir)
    end
  end
end
