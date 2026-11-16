# frozen_string_literal: true

module SparkConnect
  module ML
    # Evaluators (`org.apache.spark.ml.evaluation`). Each computes a metric over
    # a predictions DataFrame with {Evaluator#evaluate}. Defaults are Spark
    # 4.3.0's `setDefault` values; `larger_better?` mirrors Spark's `isLargerBetter`.
    module Evaluation
      # Regression metrics: rmse (default), mse, r2, mae or var.
      class RegressionEvaluator < Evaluator
        operator "org.apache.spark.ml.evaluation.RegressionEvaluator"

        param :prediction_col, :string, default: "prediction", spark_name: "predictionCol",
                                        doc: "prediction column name"
        param :label_col, :string, default: "label", spark_name: "labelCol", doc: "label column name"
        param :metric_name, :string, default: "rmse", spark_name: "metricName",
                                     doc: "metric name in evaluation: mse, rmse, r2, mae or var"
        param :weight_col, :string, spark_name: "weightCol",
                                    doc: "weight column name; unset means every instance has weight 1.0"
        param :through_origin, :boolean, default: false, spark_name: "throughOrigin",
                                         doc: "whether the regression is through the origin (r2 only)"

        # Only r2 and var are larger-is-better.
        def larger_better? = %w[r2 var].include?(get(:metric_name))
      end

      # Binary classification metrics: areaUnderROC (default) or areaUnderPR.
      class BinaryClassificationEvaluator < Evaluator
        operator "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator"

        param :raw_prediction_col, :string, default: "rawPrediction", spark_name: "rawPredictionCol",
                                            doc: "raw prediction (confidence) column name"
        param :label_col, :string, default: "label", spark_name: "labelCol", doc: "label column name"
        param :metric_name, :string, default: "areaUnderROC", spark_name: "metricName",
                                     doc: "metric name in evaluation: areaUnderROC or areaUnderPR"
        param :weight_col, :string, spark_name: "weightCol",
                                    doc: "weight column name; unset means every instance has weight 1.0"
        param :num_bins, :int, default: 1000, spark_name: "numBins",
                               doc: "number of bins to down-sample the ROC and PR curves (>= 0; 0 means no down-sampling)"

        # Both binary metrics are larger-is-better.
        def larger_better? = true
      end

      # Multiclass classification metrics (f1 by default, also accuracy, logLoss, ...).
      class MulticlassClassificationEvaluator < Evaluator
        operator "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator"

        param :prediction_col, :string, default: "prediction", spark_name: "predictionCol",
                                        doc: "prediction column name"
        param :label_col, :string, default: "label", spark_name: "labelCol", doc: "label column name"
        param :metric_name, :string, default: "f1", spark_name: "metricName",
                                     doc: "metric name in evaluation, e.g. f1, accuracy, weightedPrecision, logLoss"
        param :weight_col, :string, spark_name: "weightCol",
                                    doc: "weight column name; unset means every instance has weight 1.0"
        param :metric_label, :double, default: 0.0, spark_name: "metricLabel",
                                      doc: "the class whose metric is computed by the *ByLabel metrics (>= 0)"
        param :beta, :double, default: 1.0, doc: "the beta value used in f-measure metrics (> 0)"
        param :probability_col, :string, default: "probability", spark_name: "probabilityCol",
                                         doc: "column name for predicted class conditional probabilities"
        param :eps, :double, default: 1e-15, doc: "log-loss clipping epsilon, in (0, 0.5)"

        # Error-type metrics are smaller-is-better; all others are larger-is-better.
        LOWER_IS_BETTER = %w[weightedFalsePositiveRate falsePositiveRateByLabel logLoss hammingLoss].freeze

        def larger_better? = !LOWER_IS_BETTER.include?(get(:metric_name))
      end

      # Multilabel classification metrics (f1Measure by default, also subsetAccuracy, accuracy, hammingLoss, ...).
      class MultilabelClassificationEvaluator < Evaluator
        operator "org.apache.spark.ml.evaluation.MultilabelClassificationEvaluator"

        param :prediction_col, :string, default: "prediction", spark_name: "predictionCol",
                                        doc: "prediction column name"
        param :label_col, :string, default: "label", spark_name: "labelCol", doc: "label column name"
        param :metric_name, :string, default: "f1Measure", spark_name: "metricName",
                                     doc: "metric name in evaluation, e.g. f1Measure, subsetAccuracy, hammingLoss"
        param :metric_label, :double, default: 0.0, spark_name: "metricLabel",
                                      doc: "the class whose metric is computed by the *ByLabel metrics (>= 0)"

        # Only hammingLoss is smaller-is-better.
        def larger_better? = get(:metric_name) != "hammingLoss"
      end

      # Clustering metrics (silhouette, the only one supported, with squaredEuclidean or cosine distance).
      class ClusteringEvaluator < Evaluator
        operator "org.apache.spark.ml.evaluation.ClusteringEvaluator"

        param :prediction_col, :string, default: "prediction", spark_name: "predictionCol",
                                        doc: "prediction column name"
        param :features_col, :string, default: "features", spark_name: "featuresCol",
                                      doc: "features column name"
        param :metric_name, :string, default: "silhouette", spark_name: "metricName", doc: "metric name: silhouette"
        param :distance_measure, :string, default: "squaredEuclidean", spark_name: "distanceMeasure",
                                          doc: "distance measure: squaredEuclidean or cosine"
        param :weight_col, :string, spark_name: "weightCol",
                                    doc: "weight column name; unset means every instance has weight 1.0"

        # Silhouette is larger-is-better.
        def larger_better? = true
      end

      # Ranking metrics over arrays of predicted and relevant items (meanAveragePrecision by default).
      class RankingEvaluator < Evaluator
        operator "org.apache.spark.ml.evaluation.RankingEvaluator"

        param :prediction_col, :string, default: "prediction", spark_name: "predictionCol",
                                        doc: "prediction column name (array of items, ranked)"
        param :label_col, :string, default: "label", spark_name: "labelCol",
                                   doc: "label column name (array of relevant items)"
        param :metric_name, :string, default: "meanAveragePrecision", spark_name: "metricName",
                                     doc: "metric name in evaluation, e.g. meanAveragePrecision, precisionAtK, ndcgAtK"
        param :k, :int, default: 10, doc: "the cutoff used by the @K metrics (> 0)"

        # Every ranking metric is larger-is-better.
        def larger_better? = true
      end
    end
  end
end
