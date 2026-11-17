# frozen_string_literal: true

module SparkConnect
  module ML
    # Spark's PredictorParams: the column params shared by every predictor.
    class Predictor < Estimator
      param :features_col, :string, default: "features", doc: "features column name"
      param :label_col, :string, default: "label", doc: "label column name"
      param :prediction_col, :string, default: "prediction", doc: "prediction column name"
    end

    # Spark's PredictionModel: the prediction columns, `predict` and `numFeatures`. Shared by the
    # classification and regression families (classification loads first).
    class PredictionModel < Model
      param :features_col, :string, default: "features", doc: "features column name"
      param :label_col, :string, default: "label", doc: "label column name"
      param :prediction_col, :string, default: "prediction", doc: "prediction column name"

      attribute :num_features, :int, spark_name: "numFeatures"
      method_call :predict, arg_count: 1, returns: :double
    end

    # Params of Spark's DecisionTreeParams, included by every tree estimator.
    module DecisionTreeParams
      def self.included(base)
        base.param :max_depth, :int, default: 5, doc: "maximum depth of the tree (>= 0)"
        base.param :max_bins, :int, default: 32, doc: "maximum number of bins used for discretizing continuous features"
        base.param :min_instances_per_node, :int, default: 1, doc: "minimum number of instances each child must have after a split"
        base.param :min_info_gain, :double, default: 0.0, doc: "minimum information gain for a split to be considered"
        base.param :min_weight_fraction_per_node, :double, default: 0.0, doc: "minimum fraction of the weighted sample each child must have"
        base.param :max_memory_in_mb, :int, default: 256, spark_name: "maxMemoryInMB", doc: "maximum memory in MB for stats aggregation"
        base.param :cache_node_ids, :boolean, default: false, doc: "whether to cache node ids for each instance"
        base.param :checkpoint_interval, :int, default: 10, doc: "checkpoint interval (>= 1), or -1 to disable checkpointing"
        base.param :seed, :long, default: SEED_DEFAULT, doc: "random seed; unset uses a class-derived default on the server"
        base.param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        base.param :leaf_col, :string, default: "", doc: "leaf indices column name; empty disables the column"
      end
    end

    # Params of Spark's TreeEnsembleParams (random forest and GBT), included after {DecisionTreeParams}.
    module TreeEnsembleParams
      def self.included(base)
        base.param :subsampling_rate, :double, default: 1.0, doc: "fraction of the training data used for learning each tree, in (0, 1]"
        base.param :feature_subset_strategy, :string, default: "auto",
                                                      doc: "number of features to consider for splits at each tree node"
      end
    end

    # Allowlisted members of Spark's DecisionTreeModel trait (a single tree).
    module DecisionTreeModelMembers
      def self.included(base)
        base.attribute :feature_importances, :vector, spark_name: "featureImportances"
        base.attribute :num_nodes, :int, spark_name: "numNodes"
        base.attribute :depth, :int
        base.attribute :to_debug_string, :string, spark_name: "toDebugString"
        base.method_call :predict_leaf, arg_count: 1, returns: :double, spark_name: "predictLeaf"
      end
    end

    # Allowlisted members of Spark's TreeEnsembleModel trait (a forest or boosted ensemble).
    module TreeEnsembleModelMembers
      def self.included(base)
        base.attribute :feature_importances, :vector, spark_name: "featureImportances"
        base.attribute :tree_weights, :double_array, spark_name: "treeWeights"
        base.attribute :get_num_trees, :int, spark_name: "getNumTrees"
        base.attribute :total_num_nodes, :int, spark_name: "totalNumNodes"
        base.attribute :to_debug_string, :string, spark_name: "toDebugString"
        base.method_call :predict_leaf, arg_count: 1, returns: :vector, spark_name: "predictLeaf"
      end
    end

    # Allowlisted members of Spark's TrainingSummary trait.
    module TrainingSummaryMembers
      def self.included(base)
        base.attribute :objective_history, :double_array, spark_name: "objectiveHistory"
        base.attribute :total_iterations, :int, spark_name: "totalIterations"
      end
    end

    # Classification operators (`org.apache.spark.ml.classification`).
    module Classification
      # Binary and multinomial logistic regression.
      class LogisticRegression < Estimator
        operator "org.apache.spark.ml.classification.LogisticRegression"
        model -> { LogisticRegressionModel }

        param :reg_param, :double, default: 0.0, doc: "regularization parameter (>= 0)"
        param :elastic_net_param, :double, default: 0.0, spark_name: "elasticNetParam",
                                           doc: "the ElasticNet mixing parameter, in [0, 1]"
        param :max_iter, :int, default: 100, doc: "max number of iterations (>= 0)"
        param :tol, :double, default: 1e-6, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :fit_intercept, :boolean, default: true, doc: "whether to fit an intercept term"
        param :threshold, :double, default: 0.5, doc: "threshold in binary classification, in [0, 1]"
        param :standardization, :boolean, default: true, doc: "whether to standardize the training features"
        param :aggregation_depth, :int, default: 2, doc: "suggested depth for treeAggregate (>= 2)"
        param :family, :string, default: "auto", doc: "the label distribution family: auto, binomial or multinomial"
        param :features_col, :string, default: "features", doc: "features column name"
        param :label_col, :string, default: "label", doc: "label column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
        param :probability_col, :string, default: "probability", doc: "predicted class conditional probabilities column"
        param :raw_prediction_col, :string, default: "rawPrediction", doc: "raw prediction (confidence) column name"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :max_block_size_in_mb, :double, default: 0.0, spark_name: "maxBlockSizeInMB",
                                              doc: "maximum memory in MB for stacking input data into blocks"
      end

      # Summary of a logistic regression fit or evaluation.
      class LogisticRegressionSummary < Summary
        attribute :probability_col, :string, spark_name: "probabilityCol"
        attribute :features_col, :string, spark_name: "featuresCol"
        attribute :predictions, :dataframe
        attribute :prediction_col, :string, spark_name: "predictionCol"
        attribute :label_col, :string, spark_name: "labelCol"
        attribute :weight_col, :string, spark_name: "weightCol"
        attribute :labels, :double_array
        attribute :accuracy, :double
        attribute :weighted_precision, :double, spark_name: "weightedPrecision"
        attribute :weighted_recall, :double, spark_name: "weightedRecall"
        attribute :weighted_f_measure, :double, spark_name: "weightedFMeasure"
        attribute :weighted_true_positive_rate, :double, spark_name: "weightedTruePositiveRate"
        attribute :weighted_false_positive_rate, :double, spark_name: "weightedFalsePositiveRate"
        attribute :precision_by_label, :double_array, spark_name: "precisionByLabel"
        attribute :recall_by_label, :double_array, spark_name: "recallByLabel"
        attribute :f_measure_by_label, :double_array, spark_name: "fMeasureByLabel"
        attribute :true_positive_rate_by_label, :double_array, spark_name: "truePositiveRateByLabel"
        attribute :false_positive_rate_by_label, :double_array, spark_name: "falsePositiveRateByLabel"
      end

      # A fitted {LogisticRegression}.
      class LogisticRegressionModel < Model
        operator "org.apache.spark.ml.classification.LogisticRegressionModel"
        summary_class -> { LogisticRegressionTrainingSummary }

        param :reg_param, :double, default: 0.0, doc: "regularization parameter (>= 0)"
        param :threshold, :double, default: 0.5, doc: "threshold in binary classification, in [0, 1]"
        param :features_col, :string, default: "features", doc: "features column name"
        param :label_col, :string, default: "label", doc: "label column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
        param :probability_col, :string, default: "probability", doc: "predicted class conditional probabilities column"
        param :raw_prediction_col, :string, default: "rawPrediction", doc: "raw prediction (confidence) column name"

        attribute :coefficients, :vector
        attribute :intercept, :double
        attribute :intercept_vector, :vector, spark_name: "interceptVector"
        attribute :coefficient_matrix, :matrix, spark_name: "coefficientMatrix"
        attribute :num_features, :int, spark_name: "numFeatures"
        attribute :num_classes, :int, spark_name: "numClasses"
        method_call :predict_raw, arg_count: 1, returns: :vector, spark_name: "predictRaw"
        method_call :predict_probability, arg_count: 1, returns: :vector, spark_name: "predictProbability"
        method_call :predict, arg_count: 1, returns: :double
        method_call :evaluate, arg_count: 1, returns: LogisticRegressionSummary
      end

      # Binary logistic regression training summary (adds the ROC/PR curves).
      class BinaryLogisticRegressionTrainingSummary < LogisticRegressionSummary
        attribute :score_col, :string, spark_name: "scoreCol"
        attribute :roc, :dataframe
        attribute :area_under_roc, :double, spark_name: "areaUnderROC"
        attribute :pr, :dataframe
        attribute :f_measure_by_threshold, :dataframe, spark_name: "fMeasureByThreshold"
        attribute :precision_by_threshold, :dataframe, spark_name: "precisionByThreshold"
        attribute :recall_by_threshold, :dataframe, spark_name: "recallByThreshold"
        attribute :objective_history, :double_array, spark_name: "objectiveHistory"
        attribute :total_iterations, :int, spark_name: "totalIterations"
      end

      # The training summary class of a logistic regression model.
      LogisticRegressionTrainingSummary = BinaryLogisticRegressionTrainingSummary

      # Spark's Classifier: adds the raw prediction column to the predictor params.
      class Classifier < Predictor
        param :raw_prediction_col, :string, default: "rawPrediction", doc: "raw prediction (confidence) column name"
      end

      # Spark's ProbabilisticClassifier: adds the probability column and per-class thresholds.
      class ProbabilisticClassifier < Classifier
        param :probability_col, :string, default: "probability", doc: "predicted class conditional probabilities column"
        param :thresholds, :double_array, doc: "per-class thresholds that adjust the predicted class probabilities"
      end

      # Spark's ClassificationModel: `predictRaw` and `numClasses`.
      class ClassificationModel < PredictionModel
        param :raw_prediction_col, :string, default: "rawPrediction", doc: "raw prediction (confidence) column name"

        attribute :num_classes, :int, spark_name: "numClasses"
        method_call :predict_raw, arg_count: 1, returns: :vector, spark_name: "predictRaw"
      end

      # Spark's ProbabilisticClassificationModel: `predictProbability`.
      class ProbabilisticClassificationModel < ClassificationModel
        param :probability_col, :string, default: "probability", doc: "predicted class conditional probabilities column"
        param :thresholds, :double_array, doc: "per-class thresholds that adjust the predicted class probabilities"

        method_call :predict_probability, arg_count: 1, returns: :vector, spark_name: "predictProbability"
      end

      # Spark's ClassificationSummary: metrics of a classifier on a dataset.
      class ClassificationSummary < Summary
        attribute :predictions, :dataframe
        attribute :prediction_col, :string, spark_name: "predictionCol"
        attribute :label_col, :string, spark_name: "labelCol"
        attribute :weight_col, :string, spark_name: "weightCol"
        attribute :labels, :double_array
        attribute :accuracy, :double
        attribute :weighted_precision, :double, spark_name: "weightedPrecision"
        attribute :weighted_recall, :double, spark_name: "weightedRecall"
        attribute :weighted_f_measure, :double, spark_name: "weightedFMeasure"
        attribute :weighted_true_positive_rate, :double, spark_name: "weightedTruePositiveRate"
        attribute :weighted_false_positive_rate, :double, spark_name: "weightedFalsePositiveRate"
        attribute :precision_by_label, :double_array, spark_name: "precisionByLabel"
        attribute :recall_by_label, :double_array, spark_name: "recallByLabel"
        attribute :f_measure_by_label, :double_array, spark_name: "fMeasureByLabel"
        attribute :true_positive_rate_by_label, :double_array, spark_name: "truePositiveRateByLabel"
        attribute :false_positive_rate_by_label, :double_array, spark_name: "falsePositiveRateByLabel"
      end

      # Spark's BinaryClassificationSummary: adds the ROC and PR curves.
      class BinaryClassificationSummary < ClassificationSummary
        attribute :score_col, :string, spark_name: "scoreCol"
        attribute :roc, :dataframe
        attribute :area_under_roc, :double, spark_name: "areaUnderROC"
        attribute :pr, :dataframe
        attribute :f_measure_by_threshold, :dataframe, spark_name: "fMeasureByThreshold"
        attribute :precision_by_threshold, :dataframe, spark_name: "precisionByThreshold"
        attribute :recall_by_threshold, :dataframe, spark_name: "recallByThreshold"
      end

      # Summary of a LinearSVC evaluation.
      class LinearSVCSummary < BinaryClassificationSummary
      end

      # Summary of a LinearSVC training run.
      class LinearSVCTrainingSummary < LinearSVCSummary
        include TrainingSummaryMembers
      end

      # Summary of a factorization machines classifier evaluation.
      class FMClassificationSummary < BinaryClassificationSummary
      end

      # Summary of a factorization machines classifier training run.
      class FMClassificationTrainingSummary < FMClassificationSummary
        include TrainingSummaryMembers
      end

      # Summary of a multilayer perceptron evaluation.
      class MultilayerPerceptronClassificationSummary < ClassificationSummary
      end

      # Summary of a multilayer perceptron training run.
      class MultilayerPerceptronClassificationTrainingSummary < MultilayerPerceptronClassificationSummary
        include TrainingSummaryMembers
      end

      # Summary of a random forest classifier evaluation.
      class RandomForestClassificationSummary < ClassificationSummary
      end

      # Summary of a random forest classifier training run.
      class RandomForestClassificationTrainingSummary < RandomForestClassificationSummary
        include TrainingSummaryMembers
      end

      # Naive Bayes classifier (multinomial, bernoulli, gaussian or complement).
      class NaiveBayes < ProbabilisticClassifier
        operator "org.apache.spark.ml.classification.NaiveBayes"
        model -> { NaiveBayesModel }

        param :smoothing, :double, default: 1.0, doc: "the smoothing parameter (>= 0)"
        param :model_type, :string, default: "multinomial", doc: "the model type: multinomial, bernoulli, gaussian or complement"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
      end

      # A fitted {NaiveBayes}.
      class NaiveBayesModel < ProbabilisticClassificationModel
        operator "org.apache.spark.ml.classification.NaiveBayesModel"

        param :model_type, :string, default: "multinomial", doc: "the model type: multinomial, bernoulli, gaussian or complement"

        attribute :pi, :vector
        attribute :theta, :matrix
        attribute :sigma, :matrix
      end

      # Linear support vector machine classifier.
      class LinearSVC < Classifier
        operator "org.apache.spark.ml.classification.LinearSVC"
        model -> { LinearSVCModel }

        param :reg_param, :double, default: 0.0, doc: "regularization parameter (>= 0)"
        param :max_iter, :int, default: 100, doc: "max number of iterations (>= 0)"
        param :tol, :double, default: 1e-6, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :fit_intercept, :boolean, default: true, doc: "whether to fit an intercept term"
        param :standardization, :boolean, default: true, doc: "whether to standardize the training features"
        param :threshold, :double, default: 0.0, doc: "threshold applied to the raw prediction in binary classification"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :aggregation_depth, :int, default: 2, doc: "suggested depth for treeAggregate (>= 2)"
        param :max_block_size_in_mb, :double, default: 0.0, spark_name: "maxBlockSizeInMB",
                                              doc: "maximum memory in MB for stacking input data into blocks"
      end

      # A fitted {LinearSVC}.
      class LinearSVCModel < ClassificationModel
        operator "org.apache.spark.ml.classification.LinearSVCModel"
        summary_class -> { LinearSVCTrainingSummary }

        param :threshold, :double, default: 0.0, doc: "threshold applied to the raw prediction in binary classification"

        attribute :intercept, :double
        attribute :coefficients, :vector
        method_call :evaluate, arg_count: 1, returns: LinearSVCSummary
      end

      # Factorization machines classifier.
      class FMClassifier < ProbabilisticClassifier
        operator "org.apache.spark.ml.classification.FMClassifier"
        model -> { FMClassificationModel }

        param :factor_size, :int, default: 8, doc: "dimensionality of the factors (> 0)"
        param :fit_intercept, :boolean, default: true, doc: "whether to fit an intercept term"
        param :fit_linear, :boolean, default: true, doc: "whether to fit the linear term (1-way interactions)"
        param :reg_param, :double, default: 0.0, doc: "regularization parameter (>= 0)"
        param :mini_batch_fraction, :double, default: 1.0, doc: "fraction of the input data used for each mini-batch, in (0, 1]"
        param :init_std, :double, default: 0.01, doc: "standard deviation of the initial coefficients"
        param :max_iter, :int, default: 100, doc: "max number of iterations (>= 0)"
        param :step_size, :double, default: 1.0, doc: "step size for each iteration (> 0)"
        param :tol, :double, default: 1e-6, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :solver, :string, default: "adamW", doc: "the optimization solver: adamW or gd"
        param :seed, :long, default: SEED_DEFAULT, doc: "random seed; unset uses a class-derived default on the server"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
      end

      # A fitted {FMClassifier}.
      class FMClassificationModel < ProbabilisticClassificationModel
        operator "org.apache.spark.ml.classification.FMClassificationModel"
        summary_class -> { FMClassificationTrainingSummary }

        attribute :intercept, :double
        attribute :linear, :vector
        attribute :factors, :matrix
        method_call :evaluate, arg_count: 1, returns: FMClassificationSummary
      end

      # Multilayer perceptron classifier (feed-forward network with a softmax output layer).
      class MultilayerPerceptronClassifier < ProbabilisticClassifier
        operator "org.apache.spark.ml.classification.MultilayerPerceptronClassifier"
        model -> { MultilayerPerceptronClassificationModel }

        param :max_iter, :int, default: 100, doc: "max number of iterations (>= 0)"
        param :tol, :double, default: 1e-6, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :seed, :long, default: SEED_DEFAULT, doc: "random seed; unset uses a class-derived default on the server"
        param :layers, :int_array, doc: "sizes of the layers from input to output, including input and output (> 1 entries)"
        param :block_size, :int, default: 128, doc: "block size for stacking input data in matrices"
        param :step_size, :double, default: 0.03, doc: "step size for each iteration (> 0)"
        param :solver, :string, default: "l-bfgs", doc: "the optimization solver: l-bfgs or gd"
        param :initial_weights, :vector, doc: "the initial weights of the model"
      end

      # A fitted {MultilayerPerceptronClassifier}.
      class MultilayerPerceptronClassificationModel < ProbabilisticClassificationModel
        operator "org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel"
        summary_class -> { MultilayerPerceptronClassificationTrainingSummary }

        attribute :weights, :vector
        method_call :evaluate, arg_count: 1, returns: MultilayerPerceptronClassificationSummary
      end

      # Decision tree classifier.
      class DecisionTreeClassifier < ProbabilisticClassifier
        include DecisionTreeParams

        operator "org.apache.spark.ml.classification.DecisionTreeClassifier"
        model -> { DecisionTreeClassificationModel }

        param :impurity, :string, default: "gini", doc: "criterion used for information gain calculation: gini or entropy"
        param :prune_tree, :boolean, default: true, doc: "whether to collapse unnecessary subtrees after training"
      end

      # A fitted {DecisionTreeClassifier}.
      class DecisionTreeClassificationModel < ProbabilisticClassificationModel
        include DecisionTreeModelMembers

        operator "org.apache.spark.ml.classification.DecisionTreeClassificationModel"

        param :leaf_col, :string, default: "", doc: "leaf indices column name; empty disables the column"
      end

      # Random forest classifier.
      class RandomForestClassifier < ProbabilisticClassifier
        include TreeEnsembleParams
        include DecisionTreeParams

        operator "org.apache.spark.ml.classification.RandomForestClassifier"
        model -> { RandomForestClassificationModel }

        param :impurity, :string, default: "gini", doc: "criterion used for information gain calculation: gini or entropy"
        param :prune_tree, :boolean, default: true, doc: "whether to collapse unnecessary subtrees after training"
        param :num_trees, :int, default: 20, doc: "number of trees to train (>= 1)"
        param :bootstrap, :boolean, default: true, doc: "whether bootstrap samples are used when building trees"
      end

      # A fitted {RandomForestClassifier}.
      class RandomForestClassificationModel < ProbabilisticClassificationModel
        include TreeEnsembleModelMembers

        operator "org.apache.spark.ml.classification.RandomForestClassificationModel"
        summary_class -> { RandomForestClassificationTrainingSummary }

        param :leaf_col, :string, default: "", doc: "leaf indices column name; empty disables the column"

        method_call :evaluate, arg_count: 1, returns: RandomForestClassificationSummary
      end

      # Gradient-boosted trees classifier (binary, logistic loss).
      class GBTClassifier < ProbabilisticClassifier
        include TreeEnsembleParams
        include DecisionTreeParams

        operator "org.apache.spark.ml.classification.GBTClassifier"
        model -> { GBTClassificationModel }

        param :feature_subset_strategy, :string, default: "all",
                                                 doc: "number of features to consider for splits at each tree node"
        param :impurity, :string, default: "variance", doc: "criterion used for information gain calculation (variance)"
        param :loss_type, :string, default: "logistic", doc: "loss function to be optimized: logistic"
        param :max_iter, :int, default: 20, doc: "max number of iterations (>= 0), i.e. the number of trees"
        param :step_size, :double, default: 0.1, doc: "step size (learning rate) for each iteration (> 0)"
        param :validation_tol, :double, default: 0.01, doc: "threshold for stopping early when fit with validation is used"
        param :validation_indicator_col, :string, doc: "name of the column that marks each row as training (false) or validation (true)"
      end

      # A fitted {GBTClassifier}.
      class GBTClassificationModel < ProbabilisticClassificationModel
        include TreeEnsembleModelMembers

        operator "org.apache.spark.ml.classification.GBTClassificationModel"

        param :leaf_col, :string, default: "", doc: "leaf indices column name; empty disables the column"

        method_call :evaluate_each_iteration, arg_count: 1, returns: :double_array
      end
    end
  end
end
