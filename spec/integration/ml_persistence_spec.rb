# frozen_string_literal: true

require "fileutils"
require "json"
require "tmpdir"

# Spark's on-disk ML persistence format for the meta-algorithms (pipelines,
# validators, one-vs-rest), checked against a live server: the directory
# layout, the metadata keys, and the round trip. Cross-client checks against
# PySpark are run by hand (see the progress notes) because CI has no PySpark.
RSpec.describe "Spark ML persistence format", :integration, if: ENV.fetch("SPARK_REMOTE", nil) do
  let(:ml) { SparkConnect::ML }
  let(:types) { SparkConnect::Types }
  let(:session) { @session }
  let(:dir) { @dir ||= Dir.mktmpdir("ml-format") }
  # Three separable classes, 30 rows.
  let(:raw) do
    rows = (0...3).flat_map { |c| (0...10).map { |k| { "label" => c.to_f, "x1" => (c * 3) + (k * 0.1), "x2" => -c + (k * 0.2) } } }
    session.create_data_frame(rows)
  end
  let(:featured) { assembler.transform(raw) }
  let(:assembler) { ml::FeatureTransformers::VectorAssembler.new(input_cols: %w[x1 x2], output_col: "features") }
  let(:lr) { ml::Classification::LogisticRegression.new(max_iter: 40) }
  let(:evaluator) { ml::Evaluation::MulticlassClassificationEvaluator.new(metric_name: "accuracy") }

  before(:all) do
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE")).create
  end

  after(:all) do
    @session&.stop
  end

  after { FileUtils.rm_rf(@dir) if @dir }

  def metadata_of(path)
    file = Dir.glob(File.join(path, "metadata", "part-*")).first
    JSON.parse(File.read(file))
  end

  def predictions(df)
    df.collect.map { |r| r["prediction"] }
  end

  describe "pipeline model" do
    it "writes metadata and zero-padded stage directories, and round-trips" do
      model = ml::Pipeline.new(stages: [assembler, lr]).fit(raw)
      path = File.join(dir, "pipeline")
      model.save(path, session: session)

      meta = metadata_of(path)
      expect(meta).to include("class" => "org.apache.spark.ml.PipelineModel", "uid" => model.uid)
      expect(meta.fetch("sparkVersion")).to start_with("4.")
      expect(meta.fetch("timestamp")).to be_a(Integer)
      expect(meta.fetch("paramMap")).to eq("stageUids" => model.stages_list.map(&:uid))
      expect(meta).to have_key("defaultParamMap")
      expect(Dir.children(File.join(path, "stages")).sort).to eq(
        model.stages_list.each_with_index.map { |s, i| "#{i}_#{s.uid}" }.sort
      )

      loaded = ml::PipelineModel.load(path, session: session)
      expect(predictions(loaded.transform(raw))).to eq(predictions(model.transform(raw)))
    end
  end

  describe "cross-validation model" do
    let(:grid) { ml::Tuning::ParamGridBuilder.new.add_grid(lr.param(:reg_param), [0.0, 0.01]).build }

    it "writes the estimator, evaluator and best model with Spark's param map encoding" do
      cv = ml::CrossValidator.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator,
                                  num_folds: 2, seed: 7)
      model = cv.fit(featured)
      path = File.join(dir, "cv-model")
      model.save(path, session: session)

      expect(Dir.children(path).sort).to include("bestModel", "estimator", "evaluator", "metadata")
      meta = metadata_of(path)
      expect(meta.fetch("class")).to eq("org.apache.spark.ml.tuning.CrossValidatorModel")
      expect(meta.fetch("avgMetrics").size).to eq(2)
      expect(meta.fetch("stdMetrics").size).to eq(2)
      expect(meta.fetch("persistSubModels")).to be(false)
      entries = meta.fetch("paramMap").fetch("estimatorParamMaps")
      expect(entries.size).to eq(2)
      expect(entries.first.first).to include("parent" => lr.uid, "name" => "regParam", "isJson" => "true")
      expect(entries.first.first.fetch("value")).to eq("0.0")
      expect(meta.fetch("paramMap")).to include("numFolds" => 2, "seed" => 7)
      expect(meta.fetch("paramMap")).not_to have_key("parallelism")

      loaded = ml::CrossValidatorModel.load(path, session: session)
      expect(loaded.avg_metrics).to eq(model.avg_metrics)
      expect(predictions(loaded.transform(featured))).to eq(predictions(model.transform(featured)))
    end

    it "saves a validator with its param maps and loads it back" do
      cv = ml::CrossValidator.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator, num_folds: 3)
      path = File.join(dir, "cv")
      cv.save(path, session: session)
      meta = metadata_of(path)
      expect(meta.fetch("class")).to eq("org.apache.spark.ml.tuning.CrossValidator")
      expect(Dir.children(path).sort).to include("estimator", "evaluator")

      loaded = ml::CrossValidator.load(path, session: session)
      expect(loaded.num_folds).to eq(3)
      expect(loaded.get(:estimator_param_maps).map { |m| m.values.first }).to eq([0.0, 0.01])
    end

    it "is deterministic with the Spark seed default, and parallel fits give the same metrics" do
      first = ml::CrossValidator.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator, num_folds: 2)
                                .fit(featured).avg_metrics
      second = ml::CrossValidator.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator, num_folds: 2)
                                 .fit(featured).avg_metrics
      parallel = ml::CrossValidator.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator,
                                        num_folds: 2, parallelism: 2).fit(featured).avg_metrics
      expect(second).to eq(first)
      expect(parallel).to eq(first)
    end
  end

  describe "train-validation split model" do
    it "writes the same layout with validationMetrics, and round-trips" do
      grid = ml::Tuning::ParamGridBuilder.new.add_grid(lr.param(:max_iter), [10, 40]).build
      model = ml::TrainValidationSplit.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator,
                                           seed: 7).fit(featured)
      path = File.join(dir, "tvs-model")
      model.save(path, session: session)

      meta = metadata_of(path)
      expect(meta.fetch("class")).to eq("org.apache.spark.ml.tuning.TrainValidationSplitModel")
      expect(meta.fetch("validationMetrics").size).to eq(2)
      expect(meta.fetch("paramMap")).to include("trainRatio" => 0.75, "seed" => 7)
      expect(Dir.children(path).sort).to include("bestModel", "estimator", "evaluator")

      loaded = ml::TrainValidationSplitModel.load(path, session: session)
      expect(predictions(loaded.transform(featured))).to eq(predictions(model.transform(featured)))
    end
  end

  describe "one-vs-rest model" do
    it "trains one binary model per class and writes classifier and model_<i> directories" do
      model = ml::Classification::OneVsRest.new(classifier: lr).fit(featured)
      expect(model.models.size).to eq(3)
      path = File.join(dir, "ovr")
      model.save(path, session: session)

      expect(Dir.children(path).sort).to eq(%w[classifier metadata model_0 model_1 model_2])
      meta = metadata_of(path)
      expect(meta.fetch("class")).to eq("org.apache.spark.ml.classification.OneVsRestModel")
      expect(meta.fetch("numClasses")).to eq(3)
      expect(meta.fetch("paramMap")).not_to have_key("classifier")

      loaded = ml::Classification::OneVsRestModel.load(path, session: session)
      expect(loaded.models.size).to eq(3)
      expect(predictions(loaded.transform(featured))).to eq(predictions(model.transform(featured)))
      expect(predictions(model.transform(featured)).uniq.sort).to eq([0.0, 1.0, 2.0])
    end
  end

  describe "LDA models" do
    it "converts a distributed LDA model to a local one and describes the topics without arguments" do
      skip IntegrationHelpers::ML_MEMORY_CONTROL_SKIP if IntegrationHelpers.ml_memory_control_rejects_fits?

      rows = [[0.0, 1.0, 0.0, 2.0], [1.0, 0.0, 2.0, 0.0], [0.0, 2.0, 0.0, 1.0], [2.0, 0.0, 1.0, 0.0]]
      schema = types.struct(types.field("features", types.array(types.double)))
      docs = session.create_data_frame(rows.map { |v| { "features" => v } }, schema)
                    .with_column("features", ml::Functions.array_to_vector(SparkConnect::Column.from_name("features")))
      model = ml::Clustering::LDA.new(k: 2, max_iter: 5, optimizer: "em", seed: 1).fit(docs)
      expect(model).to be_a(ml::Clustering::DistributedLDAModel)
      expect(model.describe_topics.collect.size).to eq(2)
      expect(model.describe_topics(3).collect.size).to eq(2)

      local = model.to_local
      expect(local).to be_a(ml::Clustering::LocalLDAModel)
      expect(local.describe_topics.collect.size).to eq(2)
    end
  end
end
