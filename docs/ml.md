---
title: Machine Learning (Spark ML)
nav_order: 15
---

# Machine Learning (Spark ML)

`spark-connect` runs Spark ML over Spark Connect (the "ML Connect" protocol).
The API mirrors `pyspark.ml`: estimators are fit on a DataFrame, return models,
models transform DataFrames, evaluators compute metrics, and pipelines and
validators compose them. Every fit, transform, evaluation and attribute read
runs on the Spark server; the Ruby objects only hold parameters and references
to the server's objects.

```ruby
require "spark-connect"

ML = SparkConnect::ML
spark = SparkConnect::SparkSession.builder.remote("sc://localhost:15002").get_or_create

train = spark.create_data_frame([
  { "label" => 1.0, "x1" => 2.0, "x2" => 1.0 },
  { "label" => 0.0, "x1" => -1.0, "x2" => -2.0 }
  # ...
])
assembler = ML::FeatureTransformers::VectorAssembler.new(input_cols: %w[x1 x2], output_col: "features")
lr = ML::Classification::LogisticRegression.new(max_iter: 20)
model = ML::Pipeline.new(stages: [assembler, lr]).fit(train)
model.transform(train).collect.map { |row| row["prediction"] }
```

The examples below are shorter; [`examples/12_ml.rb`](https://github.com/HyukjinKwon/spark-connect-ruby/blob/main/examples/12_ml.rb)
is a complete, runnable script.

## Requirements

| Feature | Spark server |
| ------- | ------------ |
| Estimators, transformers, models, evaluators, pipelines, validators, save/load, linear algebra, `ml.stat`, `ml.functions` | 4.0 or newer |
| `ML.clean_cache` and `ML.cache_info` (server ML cache administration) | 4.1 or newer |
| Restoring an offloaded training summary (`model.summary` after the server offloads the model) | 4.1 or newer |

The client raises `SparkConnect::ML::UnsupportedServerError` with the required
version when a feature is called on an older server. Operators and attributes
are the same on 4.0 through 4.3, so every operator below works on any server
from 4.0 on.

## Params

Every operator is a `SparkConnect::ML::Params` with typed parameters, using
PySpark's defaults. Parameters are snake_case Ruby names; the Spark name is
available through `param.spark_name`.

```ruby
lr = ML::Classification::LogisticRegression.new(max_iter: 20, reg_param: 0.01)
lr.max_iter                  # => 20
lr.elastic_net_param         # => 0.0 (the Spark default)
lr.set_family("binomial")    # chainable setter, validated against the param type
lr.is_set?(:max_iter)        # => true
lr.explain_params            # one line per param: doc, default and current value
lr.param(:reg_param)         # the Param object, for grids and maps
lr.copy(reg_param: 0.5)      # a copy with the same uid
```

Only explicitly set parameters are sent to the server, as in PySpark.

## Estimators and models

```ruby
model = lr.fit(train)                  # => ML::Classification::LogisticRegressionModel
predictions = model.transform(test)    # a lazy DataFrame; collect it to run it
predictions.collect.first["prediction"]

model.coefficients                     # => SparkConnect::ML::Linalg::DenseVector
model.intercept                        # => Float
model.summary.area_under_roc           # training summary (binary logistic regression)
model.summary.predictions              # DataFrame of the training predictions
model.evaluate(test).accuracy          # summary of another dataset
```

Models expose the attributes and methods the server allows (the same set as
`MLUtils.invokeMethodAllowed` in Spark). Attribute reads are fetches; DataFrame
attributes such as `summary.predictions` are lazy relations.

Each fitted model holds a server-side object. When a model is no longer needed,
its server object is deleted: call `model.release`, or let the Ruby object be
garbage collected. Copies of a model share the object, which is deleted once the
last copy is released. The server also drops every ML object of a session when
the session is stopped.

## Transformers

Non-model transformers (feature transformers) are used the same way:

```ruby
assembler = ML::FeatureTransformers::VectorAssembler.new(
  input_cols: %w[x1 x2], output_col: "features"
)
assembled = assembler.transform(train)
```

## Evaluators

```ruby
evaluator = ML::Evaluation::BinaryClassificationEvaluator.new(metric_name: "areaUnderROC")
evaluator.evaluate(predictions)        # => Float
evaluator.larger_better?               # => true
```

## Feature and vector columns

`VectorUDT` and `MatrixUDT` columns decode into `SparkConnect::ML::Linalg`
values when rows are collected: `DenseVector`, `SparseVector`, `DenseMatrix` and
`SparseMatrix`. Vectors are accepted as parameters and literals:

```ruby
v = ML::Vectors.dense(1.0, 2.0)
s = ML::Vectors.sparse(4, [1, 3], [5.0, 9.0])
m = ML::Matrices.dense(2, 2, [1, 2, 3, 4])
v.to_a                                 # => [1.0, 2.0]
s[3]                                   # => 9.0

# Build vector columns from arrays, and back again.
with_features = df.with_column("features", ML::Functions.array_to_vector(SparkConnect::F.col("values")))
arrays = with_features.with_column("v", ML::Functions.vector_to_array(SparkConnect::F.col("features")))
```

## Pipelines

```ruby
pipeline = ML::Pipeline.new(stages: [assembler, lr])
pipeline_model = pipeline.fit(train)  # => ML::PipelineModel
pipeline_model.transform(test)
```

`Pipeline#fit` fits each estimator stage on the output of the stages before it,
as PySpark's Connect client does. Each stage is a server operator.

## Model selection

```ruby
grid = ML::ParamGridBuilder.new
                           .add_grid(lr.param(:reg_param), [0.0, 0.1])
                           .add_grid(lr.param(:max_iter), [10, 20])
                           .build

cv = ML::CrossValidator.new(estimator: lr, estimator_param_maps: grid,
                            evaluator: evaluator, num_folds: 3, seed: 42)
cv_model = cv.fit(train)
cv_model.avg_metrics                   # one average per param map
cv_model.best_model                    # the refit best model
cv_model.transform(test)

tvs = ML::TrainValidationSplit.new(estimator: lr, estimator_param_maps: grid,
                                   evaluator: evaluator, train_ratio: 0.8)
tvs.fit(train).validation_metrics
```

Cross validation and train-validation split run on the client, as in PySpark:
each fold and param map is one fit and one evaluation on the server, then the
best map is fit on the whole dataset. Folds are a random split on a `rand`
column seeded by `seed`, or the user's `fold_col`. The `seed` default is
Spark's: the Java hash code of the class name, so repeated fits split the data
the same way. `parallelism` runs that many fits at once on threads.

`ML::Classification::OneVsRest` reduces a multiclass problem to one binary
classifier per class. Like PySpark's, it runs on the client: one fit per class
on relabelled data, and a transform that picks the class with the highest score.

## Saving and loading

```ruby
model.save("/tmp/lr-model", overwrite: true)
loaded = ML::Classification::LogisticRegressionModel.load("/tmp/lr-model")

lr.save("/tmp/lr")
ML::Classification::LogisticRegression.load("/tmp/lr")

pipeline_model.save("/tmp/pipeline")
ML::PipelineModel.load("/tmp/pipeline")

cv_model.save("/tmp/cv-model")
ML::CrossValidatorModel.load("/tmp/cv-model")

one_vs_rest_model.save("/tmp/ovr-model")
ML::Classification::OneVsRestModel.load("/tmp/ovr-model")
```

Models are saved in Spark's standard ML persistence format, the same one
PySpark and Spark Classic read and write, so a model saved from Ruby loads in
PySpark or Scala Spark and the reverse. Paths are on the server's file system.
Operators and models are written by the server. Pipelines, validators and
one-vs-rest models write their `metadata` document (class, timestamp,
sparkVersion, uid, paramMap, defaultParamMap, and `stageUids`, `avgMetrics`,
`estimatorParamMaps`, `numClasses` as applicable), then each child in the
subdirectory Spark uses: `stages/<index>_<uid>`, `estimator`, `evaluator`,
`bestModel`, `subModels`, `classifier` and `model_<index>`. `overwrite: true`
replaces an existing path.

## Statistics

```ruby
ML::Stat::Correlation.corr(df, "features", "pearson").collect
ML::Stat::ChiSquareTest.test(df, "features", "label").collect
ML::Stat::KolmogorovSmirnovTest.test(df, "x", "norm", 0.0, 1.0).collect

df.select(ML::Stat::Summarizer.metrics("mean", "count")
                                .summary(SparkConnect::F.col("features")).alias("s"))
```

`ML::Stat::Summarizer.mean(col)` (and `variance`, `std`, `count`, `num_non_zeros`,
`max`, `min`, `norm_l1`, `norm_l2`, `sum`) returns a Column, for use in `select`
and `agg`.

## Operators

All operators are declared under these modules. Each Ruby class is named after
its Spark class; the Spark name is `operator`.

| Module | Operators |
| ------ | --------- |
| `ML::Classification` | LogisticRegression, NaiveBayes, LinearSVC, FMClassifier, MultilayerPerceptronClassifier, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, and their models |
| `ML::Regression` | LinearRegression, GeneralizedLinearRegression, AFTSurvivalRegression, IsotonicRegression, FMRegressor, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor, and their models |
| `ML::Clustering` | KMeans, BisectingKMeans, GaussianMixture, LDA, PowerIterationClustering, and their models (`DistributedLDAModel#to_local` converts an LDA model to a local one) |
| `ML::Recommendation` | ALS and ALSModel |
| `ML::FPM` | FPGrowth and FPGrowthModel, PrefixSpan |
| `ML::Feature` | StringIndexer, OneHotEncoder, StandardScaler, MinMaxScaler, MaxAbsScaler, RobustScaler, Imputer, PCA, IDF, Word2Vec, CountVectorizer, VectorIndexer, ChiSqSelector, UnivariateFeatureSelector, VarianceThresholdSelector, QuantileDiscretizer, RFormula, TargetEncoder, MinHashLSH, BucketedRandomProjectionLSH, and their models |
| `ML::FeatureTransformers` | Binarizer, Bucketizer, Normalizer, VectorAssembler, VectorSlicer, VectorSizeHint, Tokenizer, RegexTokenizer, SQLTransformer, StopWordsRemover, NGram, HashingTF, FeatureHasher, IndexToString, Interaction, DCT, ElementwiseProduct, PolynomialExpansion |
| `ML::Evaluation` | RegressionEvaluator, BinaryClassificationEvaluator, MulticlassClassificationEvaluator, MultilabelClassificationEvaluator, ClusteringEvaluator, RankingEvaluator |

The complete set of operators the server accepts is listed in
`mllib/src/main/resources/META-INF/services` in the Spark source tree.

## Limitations

- Spark 4.1 servers refuse to fit `FPGrowth`, and `LDA` with `optimizer: "em"`,
  while their ML cache memory control is on, which is the default. Set
  `spark.connect.session.connectML.mlCache.memoryControl.enabled=false` on the
  server to use them there. Spark 4.0 and 4.2 or newer allow both.
- Models and summaries expose the members the server allows. Methods that take
  a variable number of arguments are declared with their common arity.
- Python-only pieces of PySpark ML (`pyspark.ml.connect`, the
  `torch`/`deepspeed` helpers, pandas-based transformers) are not part of this
  client.
