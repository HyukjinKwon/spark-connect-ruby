# frozen_string_literal: true

module SparkConnect
  module ML
    # Regression operators (`org.apache.spark.ml.regression`). The shared predictor bases
    # ({Predictor}, {PredictionModel}) and the tree mixins live in classification.rb, which loads first.
    module Regression
      # Summary of a linear regression evaluation.
      class LinearRegressionSummary < Summary
        attribute :predictions, :dataframe
        attribute :prediction_col, :string, spark_name: "predictionCol"
        attribute :label_col, :string, spark_name: "labelCol"
        attribute :features_col, :string, spark_name: "featuresCol"
        attribute :explained_variance, :double, spark_name: "explainedVariance"
        attribute :mean_absolute_error, :double, spark_name: "meanAbsoluteError"
        attribute :mean_squared_error, :double, spark_name: "meanSquaredError"
        attribute :root_mean_squared_error, :double, spark_name: "rootMeanSquaredError"
        attribute :r2, :double
        attribute :r2adj, :double
        attribute :residuals, :dataframe
        attribute :num_instances, :long, spark_name: "numInstances"
        attribute :degrees_of_freedom, :long, spark_name: "degreesOfFreedom"
        attribute :deviance_residuals, :double_array, spark_name: "devianceResiduals"
        attribute :coefficient_standard_errors, :double_array, spark_name: "coefficientStandardErrors"
        attribute :t_values, :double_array, spark_name: "tValues"
        attribute :p_values, :double_array, spark_name: "pValues"
      end

      # Summary of a linear regression training run.
      class LinearRegressionTrainingSummary < LinearRegressionSummary
        include TrainingSummaryMembers
      end

      # Summary of a generalized linear regression evaluation.
      class GeneralizedLinearRegressionSummary < Summary
        attribute :aic, :double
        attribute :degrees_of_freedom, :long, spark_name: "degreesOfFreedom"
        attribute :deviance, :double
        attribute :dispersion, :double
        attribute :null_deviance, :double, spark_name: "nullDeviance"
        attribute :num_instances, :long, spark_name: "numInstances"
        attribute :prediction_col, :string, spark_name: "predictionCol"
        attribute :predictions, :dataframe
        attribute :rank, :long
        attribute :residual_degree_of_freedom, :long, spark_name: "residualDegreeOfFreedom"
        attribute :residual_degree_of_freedom_null, :long, spark_name: "residualDegreeOfFreedomNull"
        attribute :residuals, :dataframe
      end

      # Summary of a generalized linear regression training run.
      class GeneralizedLinearRegressionTrainingSummary < GeneralizedLinearRegressionSummary
        attribute :num_iterations, :int, spark_name: "numIterations"
        attribute :solver, :string
        attribute :t_values, :double_array, spark_name: "tValues"
        attribute :p_values, :double_array, spark_name: "pValues"
        attribute :coefficient_standard_errors, :double_array, spark_name: "coefficientStandardErrors"
      end

      # Accelerated failure time (survival) regression.
      class AFTSurvivalRegression < Predictor
        operator "org.apache.spark.ml.regression.AFTSurvivalRegression"
        model -> { AFTSurvivalRegressionModel }

        param :fit_intercept, :boolean, default: true, doc: "whether to fit an intercept term"
        param :max_iter, :int, default: 100, doc: "max number of iterations (>= 0)"
        param :tol, :double, default: 1e-6, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :censor_col, :string, default: "censor", doc: "censor column name; the value 1 means the event occurred"
        param :quantile_probabilities, :double_array, default: [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99],
                                                      doc: "quantile probabilities of the predicted quantiles, in (0, 1)"
        param :quantiles_col, :string, doc: "quantiles column name; unset disables the column"
        param :aggregation_depth, :int, default: 2, doc: "suggested depth for treeAggregate (>= 2)"
        param :max_block_size_in_mb, :double, default: 0.0, spark_name: "maxBlockSizeInMB",
                                              doc: "maximum memory in MB for stacking input data into blocks"
      end

      # A fitted {AFTSurvivalRegression}.
      class AFTSurvivalRegressionModel < PredictionModel
        operator "org.apache.spark.ml.regression.AFTSurvivalRegressionModel"

        param :quantile_probabilities, :double_array, default: [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99],
                                                      doc: "quantile probabilities of the predicted quantiles, in (0, 1)"
        param :quantiles_col, :string, doc: "quantiles column name; unset disables the column"

        attribute :intercept, :double
        attribute :coefficients, :vector
        attribute :scale, :double
        method_call :predict_quantiles, arg_count: 1, returns: :vector, spark_name: "predictQuantiles"
      end

      # Isotonic (monotonic) regression.
      class IsotonicRegression < Estimator
        operator "org.apache.spark.ml.regression.IsotonicRegression"
        model -> { IsotonicRegressionModel }

        param :features_col, :string, default: "features", doc: "features column name"
        param :label_col, :string, default: "label", doc: "label column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :isotonic, :boolean, default: true, doc: "whether the output sequence should be isotonic (increasing) or antitonic"
        param :feature_index, :int, default: 0, doc: "index of the feature if the features column is a vector"
      end

      # A fitted {IsotonicRegression}.
      class IsotonicRegressionModel < Model
        operator "org.apache.spark.ml.regression.IsotonicRegressionModel"

        param :features_col, :string, default: "features", doc: "features column name"
        param :label_col, :string, default: "label", doc: "label column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
        param :isotonic, :boolean, default: true, doc: "whether the output sequence should be isotonic (increasing) or antitonic"
        param :feature_index, :int, default: 0, doc: "index of the feature if the features column is a vector"

        attribute :boundaries, :vector
        attribute :predictions, :vector
        attribute :num_features, :int, spark_name: "numFeatures"
        method_call :predict, arg_count: 1, returns: :double
      end

      # Linear regression with elastic net, ridge, lasso or no regularization.
      class LinearRegression < Predictor
        operator "org.apache.spark.ml.regression.LinearRegression"
        model -> { LinearRegressionModel }

        param :reg_param, :double, default: 0.0, doc: "regularization parameter (>= 0)"
        param :elastic_net_param, :double, default: 0.0, spark_name: "elasticNetParam",
                                           doc: "the ElasticNet mixing parameter, in [0, 1]"
        param :max_iter, :int, default: 100, doc: "max number of iterations (>= 0)"
        param :tol, :double, default: 1e-6, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :fit_intercept, :boolean, default: true, doc: "whether to fit an intercept term"
        param :standardization, :boolean, default: true, doc: "whether to standardize the training features"
        param :solver, :string, default: "auto", doc: "the solver algorithm: auto, normal or l-bfgs"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :aggregation_depth, :int, default: 2, doc: "suggested depth for treeAggregate (>= 2)"
        param :loss, :string, default: "squaredError", doc: "the loss function: squaredError or huber"
        param :epsilon, :double, default: 1.35, doc: "the shape parameter of the huber loss, controlling outliers"
        param :max_block_size_in_mb, :double, default: 0.0, spark_name: "maxBlockSizeInMB",
                                              doc: "maximum memory in MB for stacking input data into blocks"
      end

      # A fitted {LinearRegression}.
      class LinearRegressionModel < PredictionModel
        operator "org.apache.spark.ml.regression.LinearRegressionModel"
        summary_class -> { LinearRegressionTrainingSummary }

        attribute :intercept, :double
        attribute :coefficients, :vector
        attribute :scale, :double
        method_call :evaluate, arg_count: 1, returns: LinearRegressionSummary
      end

      # Generalized linear regression (Gaussian, binomial, poisson, gamma or tweedie families).
      class GeneralizedLinearRegression < Predictor
        operator "org.apache.spark.ml.regression.GeneralizedLinearRegression"
        model -> { GeneralizedLinearRegressionModel }

        param :family, :string, default: "gaussian", doc: "the error distribution family"
        param :link, :string, doc: "the link function; unset uses the canonical link of the family"
        param :fit_intercept, :boolean, default: true, doc: "whether to fit an intercept term"
        param :max_iter, :int, default: 25, doc: "max number of iterations (>= 0)"
        param :tol, :double, default: 1e-6, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :reg_param, :double, default: 0.0, doc: "regularization parameter (>= 0)"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :solver, :string, default: "irls", doc: "the solver algorithm: irls"
        param :link_prediction_col, :string, doc: "link prediction (linear predictor) column name"
        param :variance_power, :double, default: 0.0, doc: "the power in the variance function of the tweedie family"
        param :link_power, :double, doc: "the index in the power link function of the tweedie family"
        param :offset_col, :string, doc: "the offset column name; unset means no offset"
        param :aggregation_depth, :int, default: 2, doc: "suggested depth for treeAggregate (>= 2)"
      end

      # A fitted {GeneralizedLinearRegression}.
      class GeneralizedLinearRegressionModel < PredictionModel
        operator "org.apache.spark.ml.regression.GeneralizedLinearRegressionModel"
        summary_class -> { GeneralizedLinearRegressionTrainingSummary }

        param :family, :string, default: "gaussian", doc: "the error distribution family"
        param :link, :string, doc: "the link function; unset uses the canonical link of the family"
        param :link_prediction_col, :string, doc: "link prediction (linear predictor) column name"
        param :variance_power, :double, default: 0.0, doc: "the power in the variance function of the tweedie family"
        param :link_power, :double, doc: "the index in the power link function of the tweedie family"
        param :offset_col, :string, doc: "the offset column name; unset means no offset"

        attribute :intercept, :double
        attribute :coefficients, :vector
        method_call :evaluate, arg_count: 1, returns: GeneralizedLinearRegressionSummary
      end

      # Factorization machines regressor.
      class FMRegressor < Predictor
        operator "org.apache.spark.ml.regression.FMRegressor"
        model -> { FMRegressionModel }

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

      # A fitted {FMRegressor}.
      class FMRegressionModel < PredictionModel
        operator "org.apache.spark.ml.regression.FMRegressionModel"

        attribute :intercept, :double
        attribute :linear, :vector
        attribute :factors, :matrix
      end

      # Decision tree regressor.
      class DecisionTreeRegressor < Predictor
        include DecisionTreeParams

        operator "org.apache.spark.ml.regression.DecisionTreeRegressor"
        model -> { DecisionTreeRegressionModel }

        param :impurity, :string, default: "variance", doc: "criterion used for information gain calculation (variance)"
        param :variance_col, :string, doc: "column name for the biased sample variance of the prediction"
      end

      # A fitted {DecisionTreeRegressor}.
      class DecisionTreeRegressionModel < PredictionModel
        include DecisionTreeModelMembers

        operator "org.apache.spark.ml.regression.DecisionTreeRegressionModel"

        param :leaf_col, :string, default: "", doc: "leaf indices column name; empty disables the column"
        param :variance_col, :string, doc: "column name for the biased sample variance of the prediction"
      end

      # Random forest regressor.
      class RandomForestRegressor < Predictor
        include TreeEnsembleParams
        include DecisionTreeParams

        operator "org.apache.spark.ml.regression.RandomForestRegressor"
        model -> { RandomForestRegressionModel }

        param :impurity, :string, default: "variance", doc: "criterion used for information gain calculation (variance)"
        param :num_trees, :int, default: 20, doc: "number of trees to train (>= 1)"
        param :bootstrap, :boolean, default: true, doc: "whether bootstrap samples are used when building trees"
      end

      # A fitted {RandomForestRegressor}.
      class RandomForestRegressionModel < PredictionModel
        include TreeEnsembleModelMembers

        operator "org.apache.spark.ml.regression.RandomForestRegressionModel"

        param :leaf_col, :string, default: "", doc: "leaf indices column name; empty disables the column"
      end

      # Gradient-boosted trees regressor (squared or absolute loss).
      class GBTRegressor < Predictor
        include TreeEnsembleParams
        include DecisionTreeParams

        operator "org.apache.spark.ml.regression.GBTRegressor"
        model -> { GBTRegressionModel }

        param :feature_subset_strategy, :string, default: "all",
                                                 doc: "number of features to consider for splits at each tree node"
        param :impurity, :string, default: "variance", doc: "criterion used for information gain calculation (variance)"
        param :loss_type, :string, default: "squared", doc: "loss function to be optimized: squared or absolute"
        param :max_iter, :int, default: 20, doc: "max number of iterations (>= 0), i.e. the number of trees"
        param :step_size, :double, default: 0.1, doc: "step size (learning rate) for each iteration (> 0)"
        param :validation_tol, :double, default: 0.01, doc: "threshold for stopping early when fit with validation is used"
        param :validation_indicator_col, :string, doc: "name of the column that marks each row as training (false) or validation (true)"
      end

      # A fitted {GBTRegressor}.
      class GBTRegressionModel < PredictionModel
        include TreeEnsembleModelMembers

        operator "org.apache.spark.ml.regression.GBTRegressionModel"

        param :leaf_col, :string, default: "", doc: "leaf indices column name; empty disables the column"

        method_call :evaluate_each_iteration, arg_count: 2, returns: :double_array
      end
    end
  end
end
