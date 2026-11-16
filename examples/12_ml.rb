#!/usr/bin/env ruby
# frozen_string_literal: true

# Spark ML over Spark Connect (Spark 4.0+): assemble features, fit a
# pipeline with a logistic regression, score it, tune its regularization with
# cross validation, and save and load the fitted model.
#
# Run with a reachable Spark Connect server (Spark 4.0+):
#   SPARK_REMOTE=sc://localhost:15002 ruby examples/12_ml.rb

require "spark-connect"
require "tmpdir"

ML = SparkConnect::ML
F = SparkConnect::F

spark = SparkConnect::SparkSession.builder
                                  .remote(ENV.fetch("SPARK_REMOTE", "sc://localhost:15002"))
                                  .get_or_create

# A small, linearly separable dataset: the label is 1 when x1 + x2 > 0.
rows = (-10...10).map do |i|
  x1 = i + 0.5
  x2 = (i * 0.3).round(2)
  { "x1" => x1, "x2" => x2, "label" => x1 + x2 > 0 ? 1.0 : 0.0 }
end
data = spark.create_data_frame(rows)

train, test = data.random_split([0.75, 0.25], 42)

assembler = ML::FeatureTransformers::VectorAssembler.new(input_cols: %w[x1 x2], output_col: "features")
lr = ML::Classification::LogisticRegression.new(max_iter: 50, reg_param: 0.01)

# 1. A pipeline: the assembler and the classifier, fit in one call.
pipeline_model = ML::Pipeline.new(stages: [assembler, lr]).fit(train)
predictions = pipeline_model.transform(test)
puts "pipeline predictions:"
predictions.select("label", "prediction").show

evaluator = ML::Evaluation::BinaryClassificationEvaluator.new(metric_name: "areaUnderROC")
puts "test areaUnderROC: #{evaluator.evaluate(predictions)}"

# 2. Model selection: a grid over the regularization strength.
featured_train = assembler.transform(train)
featured_test = assembler.transform(test)
grid = ML::Tuning::ParamGridBuilder.new
                                   .add_grid(lr.param(:reg_param), [0.0, 0.01, 0.1])
                                   .build
cv = ML::CrossValidator.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator,
                            num_folds: 3, seed: 42)
cv_model = cv.fit(featured_train)
puts "cross-validated areaUnderROC per regularization: #{cv_model.avg_metrics.map { |m| m.round(3) }}"
puts "best model test areaUnderROC: #{evaluator.evaluate(cv_model.transform(featured_test))}"

# 3. The fitted model's own attributes and training summary.
model = cv_model.best_model
puts "coefficients: #{model.coefficients}"
puts "training areaUnderROC: #{model.summary.area_under_roc.round(3)}"

# 4. Column statistics on the feature vectors.
stats = featured_train.select(
  ML::Stat::Summarizer.metrics("mean", "count").summary(F.col("features")).alias("s")
).select("s.mean", "s.count")
puts "feature means and count: #{stats.collect.first.to_h}"

# 5. Save the pipeline model and load it back.
Dir.mktmpdir("spark-ml-example") do |dir|
  path = File.join(dir, "pipeline")
  pipeline_model.save(path, session: spark)
  loaded = ML::PipelineModel.load(path, session: spark)
  puts "reloaded pipeline stages: #{loaded.stages_list.map { |s| s.class.name.split('::').last }}"
end

spark.stop
