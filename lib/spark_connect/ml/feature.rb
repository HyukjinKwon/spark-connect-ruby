# frozen_string_literal: true

module SparkConnect
  module ML
    # Feature operators (`org.apache.spark.ml.feature`): scalers, indexers,
    # encoders, selectors, text vectorizers, dimensionality reduction and LSH.
    # Defaults mirror Spark 4.3.0 (PySpark `__init__` keyword defaults and the
    # `setDefault` calls of the Scala classes).
    module Feature
      # Discretizes a continuous column into quantile buckets. Fitting produces a
      # {BucketizerModel}.
      class QuantileDiscretizer < Estimator
        operator "org.apache.spark.ml.feature.QuantileDiscretizer"
        model -> { BucketizerModel }

        param :num_buckets, :int, default: 2, doc: "number of buckets (quantiles), >= 2"
        param :num_buckets_array, :int_array, doc: "number of buckets per column for multi-column input"
        param :relative_error, :double, default: 0.001, doc: "relative target precision of the quantile algorithm"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :input_cols, :string_array, doc: "input column names"
        param :output_cols, :string_array, doc: "output column names"
      end

      # A fitted {QuantileDiscretizer}: a Bucketizer whose splits are the fitted quantiles.
      class BucketizerModel < Model
        operator "org.apache.spark.ml.feature.Bucketizer"

        param :splits, :double_array, doc: "split points for bucketing, strictly increasing"
        param :splits_array, :double_array_array, spark_name: "splitsArray",
                                                  doc: "split points per column for multi-column input"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :input_cols, :string_array, doc: "input column names"
        param :output_cols, :string_array, doc: "output column names"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"

        attribute :get_splits, :double_array, spark_name: "getSplits"
        attribute :get_splits_array, :any, spark_name: "getSplitsArray"
      end

      # Builds an R-style model formula (`y ~ x1 + x2`) into a feature vector and label.
      class RFormula < Estimator
        operator "org.apache.spark.ml.feature.RFormula"
        model -> { RFormulaModel }

        param :formula, :string, doc: "the R model formula, e.g. \"y ~ x1 + x2\""
        param :features_col, :string, default: "features", doc: "features column name"
        param :label_col, :string, default: "label", doc: "label column name"
        param :force_index_label, :boolean, default: false, doc: "whether to index a numeric label"
        param :string_indexer_order_type, :string, default: "frequencyDesc",
                                                   doc: "category ordering for string features"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"
      end

      # A fitted {RFormula}.
      class RFormulaModel < Model
        operator "org.apache.spark.ml.feature.RFormulaModel"

        param :formula, :string, doc: "the R model formula"
        param :features_col, :string, default: "features", doc: "features column name"
        param :label_col, :string, default: "label", doc: "label column name"
        param :force_index_label, :boolean, default: false, doc: "whether to index a numeric label"
        param :string_indexer_order_type, :string, default: "frequencyDesc",
                                                   doc: "category ordering for string features"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"

        attribute :resolved_formula_string, :string, spark_name: "resolvedFormulaString"
      end

      # Replaces missing values with the mean, median or mode of each column.
      class Imputer < Estimator
        operator "org.apache.spark.ml.feature.Imputer"
        model -> { ImputerModel }

        param :strategy, :string, default: "mean", doc: "imputation strategy: mean, median or mode"
        param :missing_value, :double, default: Float::NAN, spark_name: "missingValue",
                                       doc: "the placeholder for missing values (NaN by default)"
        param :relative_error, :double, default: 0.001, doc: "relative error of the approximate median"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :input_cols, :string_array, doc: "input column names"
        param :output_cols, :string_array, doc: "output column names"
      end

      # A fitted {Imputer}: the surrogate values per column.
      class ImputerModel < Model
        operator "org.apache.spark.ml.feature.ImputerModel"

        param :strategy, :string, default: "mean", doc: "imputation strategy: mean, median or mode"
        param :missing_value, :double, default: Float::NAN, spark_name: "missingValue",
                                       doc: "the placeholder for missing values (NaN by default)"
        param :relative_error, :double, default: 0.001, doc: "relative error of the approximate median"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :input_cols, :string_array, doc: "input column names"
        param :output_cols, :string_array, doc: "output column names"

        attribute :surrogate_df, :dataframe, spark_name: "surrogateDF"
      end

      # Scales a vector column to unit standard deviation and optionally zero mean.
      class StandardScaler < Estimator
        operator "org.apache.spark.ml.feature.StandardScaler"
        model -> { StandardScalerModel }

        param :with_mean, :boolean, default: false, doc: "whether to center the data with mean before scaling"
        param :with_std, :boolean, default: true, doc: "whether to scale the data to unit standard deviation"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {StandardScaler}.
      class StandardScalerModel < Model
        operator "org.apache.spark.ml.feature.StandardScalerModel"

        param :with_mean, :boolean, default: false, doc: "whether to center the data with mean before scaling"
        param :with_std, :boolean, default: true, doc: "whether to scale the data to unit standard deviation"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"

        attribute :mean, :vector
        attribute :std, :vector
      end

      # Scales each feature to [-1, 1] by its maximum absolute value.
      class MaxAbsScaler < Estimator
        operator "org.apache.spark.ml.feature.MaxAbsScaler"
        model -> { MaxAbsScalerModel }

        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {MaxAbsScaler}.
      class MaxAbsScalerModel < Model
        operator "org.apache.spark.ml.feature.MaxAbsScalerModel"

        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"

        attribute :max_abs, :vector, spark_name: "maxAbs"
      end

      # Rescales each feature to the range [min, max].
      class MinMaxScaler < Estimator
        operator "org.apache.spark.ml.feature.MinMaxScaler"
        model -> { MinMaxScalerModel }

        param :min, :double, default: 0.0, doc: "lower bound of the output range"
        param :max, :double, default: 1.0, doc: "upper bound of the output range"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {MinMaxScaler}.
      class MinMaxScalerModel < Model
        operator "org.apache.spark.ml.feature.MinMaxScalerModel"

        param :min, :double, default: 0.0, doc: "lower bound of the output range"
        param :max, :double, default: 1.0, doc: "upper bound of the output range"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"

        attribute :original_max, :vector, spark_name: "originalMax"
        attribute :original_min, :vector, spark_name: "originalMin"
      end

      # Scales features with quantile statistics, which are robust to outliers.
      class RobustScaler < Estimator
        operator "org.apache.spark.ml.feature.RobustScaler"
        model -> { RobustScalerModel }

        param :lower, :double, default: 0.25, doc: "lower quantile of the range, in [0, 1]"
        param :upper, :double, default: 0.75, doc: "upper quantile of the range, in [0, 1]"
        param :with_centering, :boolean, default: false, doc: "whether to center the data with the median"
        param :with_scaling, :boolean, default: true, doc: "whether to scale to the quantile range"
        param :relative_error, :double, default: 0.001, doc: "relative error of the approximate quantiles"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {RobustScaler}.
      class RobustScalerModel < Model
        operator "org.apache.spark.ml.feature.RobustScalerModel"

        param :lower, :double, default: 0.25, doc: "lower quantile of the range, in [0, 1]"
        param :upper, :double, default: 0.75, doc: "upper quantile of the range, in [0, 1]"
        param :with_centering, :boolean, default: false, doc: "whether to center the data with the median"
        param :with_scaling, :boolean, default: true, doc: "whether to scale to the quantile range"
        param :relative_error, :double, default: 0.001, doc: "relative error of the approximate quantiles"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"

        attribute :range, :vector
        attribute :median, :vector
      end

      # Base of the fitted feature selectors: the indices of the selected features.
      class SelectorModelBase < Model
        attribute :selected_features, :int_array, spark_name: "selectedFeatures"
      end

      # Chi-squared feature selection for categorical features and labels.
      class ChiSqSelector < Estimator
        operator "org.apache.spark.ml.feature.ChiSqSelector"
        model -> { ChiSqSelectorModel }

        param :num_top_features, :int, default: 50, spark_name: "numTopFeatures",
                                       doc: "number of top features to keep (selectorType numTopFeatures)"
        param :percentile, :double, default: 0.1, doc: "fraction of top features to keep (selectorType percentile)"
        param :fpr, :double, default: 0.05, doc: "highest p-value of features to keep (selectorType fpr)"
        param :fdr, :double, default: 0.05, doc: "upper bound of the false discovery rate (selectorType fdr)"
        param :fwe, :double, default: 0.05, doc: "upper bound of the family-wise error rate (selectorType fwe)"
        param :selector_type, :string, default: "numTopFeatures", spark_name: "selectorType",
                                       doc: "numTopFeatures, percentile, fpr, fdr or fwe"
        param :features_col, :string, default: "features", doc: "features column name"
        param :label_col, :string, default: "label", doc: "label column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {ChiSqSelector}.
      class ChiSqSelectorModel < SelectorModelBase
        operator "org.apache.spark.ml.feature.ChiSqSelectorModel"

        param :num_top_features, :int, default: 50, spark_name: "numTopFeatures", doc: "number of top features"
        param :percentile, :double, default: 0.1, doc: "fraction of top features"
        param :fpr, :double, default: 0.05, doc: "highest p-value of features"
        param :fdr, :double, default: 0.05, doc: "upper bound of the false discovery rate"
        param :fwe, :double, default: 0.05, doc: "upper bound of the family-wise error rate"
        param :selector_type, :string, default: "numTopFeatures", spark_name: "selectorType", doc: "selector type"
        param :features_col, :string, default: "features", doc: "features column name"
        param :label_col, :string, default: "label", doc: "label column name"
        param :output_col, :string, doc: "output column name"
      end

      # Feature selection from a univariate statistical test, chosen by feature and label types.
      class UnivariateFeatureSelector < Estimator
        operator "org.apache.spark.ml.feature.UnivariateFeatureSelector"
        model -> { UnivariateFeatureSelectorModel }

        param :feature_type, :string, doc: "feature type: categorical or continuous"
        param :label_type, :string, doc: "label type: categorical or continuous"
        param :selection_mode, :string, default: "numTopFeatures",
                                        doc: "numTopFeatures, percentile, fpr, fdr, fwe"
        param :selection_threshold, :double, doc: "the threshold for the selection mode"
        param :features_col, :string, default: "features", doc: "features column name"
        param :label_col, :string, default: "label", doc: "label column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {UnivariateFeatureSelector}.
      class UnivariateFeatureSelectorModel < SelectorModelBase
        operator "org.apache.spark.ml.feature.UnivariateFeatureSelectorModel"

        param :feature_type, :string, doc: "feature type: categorical or continuous"
        param :label_type, :string, doc: "label type: categorical or continuous"
        param :selection_mode, :string, default: "numTopFeatures", doc: "selection mode"
        param :selection_threshold, :double, doc: "the threshold for the selection mode"
        param :features_col, :string, default: "features", doc: "features column name"
        param :label_col, :string, default: "label", doc: "label column name"
        param :output_col, :string, doc: "output column name"
      end

      # Removes features whose variance is at or below a threshold.
      class VarianceThresholdSelector < Estimator
        operator "org.apache.spark.ml.feature.VarianceThresholdSelector"
        model -> { VarianceThresholdSelectorModel }

        param :variance_threshold, :double, default: 0.0, doc: "features with variance <= this are removed"
        param :features_col, :string, default: "features", doc: "features column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {VarianceThresholdSelector}.
      class VarianceThresholdSelectorModel < SelectorModelBase
        operator "org.apache.spark.ml.feature.VarianceThresholdSelectorModel"

        param :variance_threshold, :double, default: 0.0, doc: "features with variance <= this are removed"
        param :features_col, :string, default: "features", doc: "features column name"
        param :output_col, :string, doc: "output column name"
      end

      # Maps string labels to indices, most frequent first by default.
      class StringIndexer < Estimator
        operator "org.apache.spark.ml.feature.StringIndexer"
        model -> { StringIndexerModel }

        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :input_cols, :string_array, doc: "input column names"
        param :output_cols, :string_array, doc: "output column names"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"
        param :string_order_type, :string, default: "frequencyDesc", spark_name: "stringOrderType",
                                           doc: "label ordering: frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc"
      end

      # A fitted {StringIndexer}: the labels in index order.
      class StringIndexerModel < Model
        operator "org.apache.spark.ml.feature.StringIndexerModel"

        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :input_cols, :string_array, doc: "input column names"
        param :output_cols, :string_array, doc: "output column names"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"
        param :string_order_type, :string, default: "frequencyDesc", spark_name: "stringOrderType",
                                           doc: "label ordering: frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc"

        attribute :labels, :string_array
        attribute :labels_array, :any, spark_name: "labelsArray"
      end

      # Automatically identifies categorical features of a vector column and indexes them.
      class VectorIndexer < Estimator
        operator "org.apache.spark.ml.feature.VectorIndexer"
        model -> { VectorIndexerModel }

        param :max_categories, :int, default: 20, doc: "features with at most this many values are categorical"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"
      end

      # A fitted {VectorIndexer}.
      class VectorIndexerModel < Model
        operator "org.apache.spark.ml.feature.VectorIndexerModel"

        param :max_categories, :int, default: 20, doc: "features with at most this many values are categorical"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"

        attribute :num_features, :int, spark_name: "numFeatures"
        attribute :category_maps_df, :dataframe, spark_name: "categoryMapsDF"
      end

      # Principal component analysis: projects vectors onto their top `k` components.
      class PCA < Estimator
        operator "org.apache.spark.ml.feature.PCA"
        model -> { PCAModel }

        param :k, :int, doc: "the number of principal components (> 0)"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {PCA}.
      class PCAModel < Model
        operator "org.apache.spark.ml.feature.PCAModel"

        param :k, :int, doc: "the number of principal components (> 0)"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"

        attribute :pc, :matrix
        attribute :explained_variance, :vector, spark_name: "explainedVariance"
      end

      # Inverse document frequency weighting of term-count vectors.
      class IDF < Estimator
        operator "org.apache.spark.ml.feature.IDF"
        model -> { IDFModel }

        param :min_doc_freq, :int, default: 0, spark_name: "minDocFreq",
                                   doc: "minimum number of documents a term must appear in"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {IDF}.
      class IDFModel < Model
        operator "org.apache.spark.ml.feature.IDFModel"

        param :min_doc_freq, :int, default: 0, spark_name: "minDocFreq",
                                   doc: "minimum number of documents a term must appear in"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"

        attribute :idf, :vector
        attribute :doc_freq, :any, spark_name: "docFreq"
        attribute :num_docs, :long, spark_name: "numDocs"
      end

      # Learns distributed vector representations of words (skip-gram).
      class Word2Vec < Estimator
        operator "org.apache.spark.ml.feature.Word2Vec"
        model -> { Word2VecModel }

        param :vector_size, :int, default: 100, doc: "dimension of the word vectors"
        param :window_size, :int, default: 5, doc: "the window size (context words from each side)"
        param :num_partitions, :int, default: 1, doc: "number of partitions for the training"
        param :min_count, :int, default: 5, doc: "minimum number of occurrences of a token"
        param :max_sentence_length, :int, default: 1000, doc: "maximum length of a sentence, longer ones are split"
        param :step_size, :double, default: 0.025, doc: "the learning rate"
        param :max_iter, :int, default: 1, doc: "maximum number of iterations"
        param :seed, :long, default: SEED_DEFAULT, doc: "the random seed"
        param :input_col, :string, doc: "input column name (array of strings)"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {Word2Vec}.
      class Word2VecModel < Model
        operator "org.apache.spark.ml.feature.Word2VecModel"

        param :vector_size, :int, default: 100, doc: "dimension of the word vectors"
        param :window_size, :int, default: 5, doc: "the window size"
        param :num_partitions, :int, default: 1, doc: "number of partitions"
        param :min_count, :int, default: 5, doc: "minimum number of occurrences of a token"
        param :max_sentence_length, :int, default: 1000, doc: "maximum length of a sentence"
        param :step_size, :double, default: 0.025, doc: "the learning rate"
        param :max_iter, :int, default: 1, doc: "maximum number of iterations"
        param :seed, :long, default: SEED_DEFAULT, doc: "the random seed"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"

        attribute :get_vectors, :dataframe, spark_name: "getVectors"
        method_call :find_synonyms, arg_count: 2, returns: :dataframe, spark_name: "findSynonyms"
      end

      # Converts documents (arrays of tokens) to term-count vectors over a learned vocabulary.
      class CountVectorizer < Estimator
        operator "org.apache.spark.ml.feature.CountVectorizer"
        model -> { CountVectorizerModel }

        param :min_tf, :double, default: 1.0, spark_name: "minTF",
                                doc: "minimum term frequency within a document (fraction if < 1)"
        param :min_df, :double, default: 1.0, spark_name: "minDF",
                                doc: "minimum number of documents a term must appear in (fraction if < 1)"
        param :max_df, :double, default: 9_223_372_036_854_775_807.0, spark_name: "maxDF",
                                doc: "maximum number of documents a term may appear in (fraction if < 1)"
        param :vocab_size, :int, default: 2**18, spark_name: "vocabSize", doc: "maximum size of the vocabulary"
        param :binary, :boolean, default: false, doc: "whether all non-zero counts are set to 1"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {CountVectorizer}.
      class CountVectorizerModel < Model
        operator "org.apache.spark.ml.feature.CountVectorizerModel"

        param :min_tf, :double, default: 1.0, spark_name: "minTF", doc: "minimum term frequency within a document"
        param :min_df, :double, default: 1.0, spark_name: "minDF", doc: "minimum document frequency"
        param :max_df, :double, default: 9_223_372_036_854_775_807.0, spark_name: "maxDF", doc: "maximum document frequency"
        param :vocab_size, :int, default: 2**18, spark_name: "vocabSize", doc: "maximum size of the vocabulary"
        param :binary, :boolean, default: false, doc: "whether all non-zero counts are set to 1"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"

        attribute :vocabulary, :string_array
      end

      # Maps category indices to one-hot (sparse) vectors.
      class OneHotEncoder < Estimator
        operator "org.apache.spark.ml.feature.OneHotEncoder"
        model -> { OneHotEncoderModel }

        param :input_cols, :string_array, doc: "input column names"
        param :output_cols, :string_array, doc: "output column names"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"
        param :drop_last, :boolean, default: true, doc: "whether to drop the last category"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {OneHotEncoder}.
      class OneHotEncoderModel < Model
        operator "org.apache.spark.ml.feature.OneHotEncoderModel"

        param :input_cols, :string_array, doc: "input column names"
        param :output_cols, :string_array, doc: "output column names"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"
        param :drop_last, :boolean, default: true, doc: "whether to drop the last category"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"

        attribute :category_sizes, :int_array, spark_name: "categorySizes"
      end

      # Encodes categories with the smoothed mean of the label (target) per category.
      class TargetEncoder < Estimator
        operator "org.apache.spark.ml.feature.TargetEncoder"
        model -> { TargetEncoderModel }

        param :input_cols, :string_array, doc: "input column names"
        param :output_cols, :string_array, doc: "output column names"
        param :label_col, :string, default: "label", doc: "label column name"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"
        param :target_type, :string, default: "binary", doc: "label type: binary or continuous"
        param :smoothing, :double, default: 0.0, doc: "smoothing factor blending class and overall estimates"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
      end

      # A fitted {TargetEncoder}.
      class TargetEncoderModel < Model
        operator "org.apache.spark.ml.feature.TargetEncoderModel"

        param :input_cols, :string_array, doc: "input column names"
        param :output_cols, :string_array, doc: "output column names"
        param :label_col, :string, default: "label", doc: "label column name"
        param :handle_invalid, :string, default: "error", doc: "invalid entries: skip, error or keep"
        param :target_type, :string, default: "binary", doc: "label type: binary or continuous"
        param :smoothing, :double, default: 0.0, doc: "smoothing factor blending class and overall estimates"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
      end

      # Base of the fitted LSH models: nearest-neighbour search and similarity joins.
      # `distCol` is required (PySpark always passes it, defaulting to "distCol").
      class LshModelBase < Model
        method_call :approx_nearest_neighbors, arg_count: 4, returns: :dataframe,
                                               spark_name: "approxNearestNeighbors"
        method_call :approx_similarity_join, arg_count: 4, returns: :dataframe, spark_name: "approxSimilarityJoin"
      end

      # Bucketed random projection LSH for Euclidean distance.
      class BucketedRandomProjectionLSH < Estimator
        operator "org.apache.spark.ml.feature.BucketedRandomProjectionLSH"
        model -> { BucketedRandomProjectionLSHModel }

        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :seed, :long, default: SEED_DEFAULT, doc: "the random seed"
        param :num_hash_tables, :int, default: 1, spark_name: "numHashTables",
                                      doc: "number of hash tables (increasing it lowers the false negative rate)"
        param :bucket_length, :double, doc: "the length of each hash bucket (> 0)"
      end

      # A fitted {BucketedRandomProjectionLSH}.
      class BucketedRandomProjectionLSHModel < LshModelBase
        operator "org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel"

        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :num_hash_tables, :int, default: 1, spark_name: "numHashTables", doc: "number of hash tables"
        param :bucket_length, :double, doc: "the length of each hash bucket (> 0)"
      end

      # MinHash LSH for Jaccard distance over sets.
      class MinHashLSH < Estimator
        operator "org.apache.spark.ml.feature.MinHashLSH"
        model -> { MinHashLSHModel }

        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :seed, :long, default: SEED_DEFAULT, doc: "the random seed"
        param :num_hash_tables, :int, default: 1, spark_name: "numHashTables",
                                      doc: "number of hash tables (increasing it lowers the false negative rate)"
      end

      # A fitted {MinHashLSH}.
      class MinHashLSHModel < LshModelBase
        operator "org.apache.spark.ml.feature.MinHashLSHModel"

        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name"
        param :num_hash_tables, :int, default: 1, spark_name: "numHashTables", doc: "number of hash tables"
      end
    end
  end
end
