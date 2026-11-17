# frozen_string_literal: true

require_relative "support"

# Persistence helpers that need no server: Spark's seed default, the stage
# directory names, the Scala class names, and the param-map encoding of
# validators.
RSpec.describe "Spark ML persistence format (unit)" do
  include MlSpecSupport

  let(:ml) { SparkConnect::ML }

  it "computes Java's String#hashCode, as Spark's HasSeed default does" do
    expect(ml.java_string_hash("hello")).to eq(99_162_322)
    expect(ml.java_string_hash("org.apache.spark.ml.tuning.CrossValidator")).to eq(-1_191_137_437)
  end

  it "uses the class-derived seed default for every seed param" do
    expect(ml::Tuning::CrossValidator.new.get(:seed)).to eq(-1_191_137_437)
    expect(ml::Clustering::LocalLDAModel.params[:seed].default_for(ml::Clustering::LocalLDAModel)).to eq(1_421_925_485)
  end

  it "zero-pads stage directory indexes to the width of the stage count" do
    expect(ml::Persistence.stage_dir_name(2, "LogisticRegression_abc", 12)).to eq("02_LogisticRegression_abc")
    expect(ml::Persistence.stage_dir_name(0, "x", 3)).to eq("0_x")
  end

  it "writes Scala class names for the meta-algorithms" do
    expect(ml::Pipeline.operator).to eq("org.apache.spark.ml.Pipeline")
    expect(ml::PipelineModel.operator).to eq("org.apache.spark.ml.PipelineModel")
    expect(ml::CrossValidatorModel.operator).to eq("org.apache.spark.ml.tuning.CrossValidatorModel")
    expect(ml::Classification::OneVsRestModel.operator).to eq("org.apache.spark.ml.classification.OneVsRestModel")
  end

  it "resolves Spark and PySpark class names to the Ruby classes" do
    expect(ml::Persistence.class_for("org.apache.spark.ml.tuning.CrossValidator")).to eq(ml::CrossValidator)
    expect(ml::Persistence.class_for("pyspark.ml.pipeline.PipelineModel")).to eq(ml::PipelineModel)
    expect { ml::Persistence.class_for("org.example.Unknown") }.to raise_error(SparkConnect::IllegalArgumentError)
  end

  it "excludes object-valued params from the paramMap" do
    lr = ml::Classification::LogisticRegression.new(max_iter: 3)
    cv = ml::CrossValidator.new(estimator: lr, estimator_param_maps: [], evaluator: nil.to_a.first || Object.new, num_folds: 2)
    json = ml::Persistence.extract_json_params(cv, skip: %w[estimator evaluator estimatorParamMaps])
    expect(json).to include("numFolds" => 2, "foldCol" => "")
    expect(json.keys).not_to include("estimator", "evaluator", "estimatorParamMaps")
  end
end
