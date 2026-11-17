# frozen_string_literal: true

module SparkConnect
  module ML
    # Feature transformers (`org.apache.spark.ml.feature`) that are not models.
    #
    # Defaults are Spark 4.3.0's `setDefault` values. Params that Spark leaves
    # unset, or that Spark derives at run time (`outputCol` defaults to
    # `<uid>__output`, `stopWords` to the English list, `locale` to the JVM
    # locale), are declared without a `default:`.
    module FeatureTransformers
      # Discrete cosine transform of a vector (`org.apache.spark.ml.feature.DCT`).
      class DCT < Transformer
        operator "org.apache.spark.ml.feature.DCT"

        param :inverse, :boolean, default: false, doc: "whether to perform the inverse DCT (true) or forward DCT (false)"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
      end

      # Converts an array of strings into an array of n-grams (`NGram`).
      class NGram < Transformer
        operator "org.apache.spark.ml.feature.NGram"

        param :n, :int, default: 2, doc: "number of elements per n-gram (>= 1)"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
      end

      # Normalizes vectors to unit p-norm (`Normalizer`).
      class Normalizer < Transformer
        operator "org.apache.spark.ml.feature.Normalizer"

        param :p, :double, default: 2.0, doc: "the p norm value (>= 1)"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
      end

      # Multiplies the features of several columns together (`Interaction`).
      class Interaction < Transformer
        operator "org.apache.spark.ml.feature.Interaction"

        param :input_cols, :string_array, spark_name: "inputCols", doc: "input column names"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
      end

      # Thresholds numerical features to binary values (`Binarizer`).
      class Binarizer < Transformer
        operator "org.apache.spark.ml.feature.Binarizer"

        param :threshold, :double, default: 0.0, doc: "values above the threshold map to 1.0, others to 0.0"
        param :thresholds, :double_array, doc: "per-column thresholds for multi-column binarization"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
        param :input_cols, :string_array, doc: "input column names (multi-column mode)"
        param :output_cols, :string_array, doc: "output column names (multi-column mode)"
      end

      # Maps continuous columns to bucket indices given split points (`Bucketizer`).
      class Bucketizer < Transformer
        operator "org.apache.spark.ml.feature.Bucketizer"

        param :splits, :double_array, doc: "split points for mapping continuous features into buckets"
        param :splits_array, :double_array_array, spark_name: "splitsArray",
                                                  doc: "split points per input column (multi-column mode)"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
        param :input_cols, :string_array, doc: "input column names (multi-column mode)"
        param :output_cols, :string_array, doc: "output column names (multi-column mode)"
        param :handle_invalid, :string, default: "error", spark_name: "handleInvalid",
                                        doc: "how to handle NaN and out-of-range values: error, skip or keep"
      end

      # Merges several columns into one vector column (`VectorAssembler`).
      class VectorAssembler < Transformer
        operator "org.apache.spark.ml.feature.VectorAssembler"

        param :input_cols, :string_array, doc: "input column names"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
        param :handle_invalid, :string, default: "error", spark_name: "handleInvalid",
                                        doc: "how to handle invalid entries: error, skip or keep"
      end

      # Selects a subset of the elements of a vector column (`VectorSlicer`).
      class VectorSlicer < Transformer
        operator "org.apache.spark.ml.feature.VectorSlicer"

        param :indices, :int_array, default: [].freeze, doc: "indices to take from the input vector"
        param :names, :string_array, default: [].freeze, doc: "names of the features to take from the input vector"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
      end

      # Adds the vector size to a column's metadata so it can be used downstream (`VectorSizeHint`).
      class VectorSizeHint < Transformer
        operator "org.apache.spark.ml.feature.VectorSizeHint"

        param :size, :int, doc: "size of the vectors in the column (>= 0)"
        param :input_col, :string, doc: "input column name"
        param :handle_invalid, :string, default: "error", spark_name: "handleInvalid",
                                        doc: "how to handle invalid entries: error, skip or optimistic"
      end

      # Splits a string column into lowercase tokens on whitespace (`Tokenizer`).
      class Tokenizer < Transformer
        operator "org.apache.spark.ml.feature.Tokenizer"

        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
      end

      # Tokenizes a string column with a regex, either splitting on it or matching it (`RegexTokenizer`).
      class RegexTokenizer < Transformer
        operator "org.apache.spark.ml.feature.RegexTokenizer"

        param :min_token_length, :int, default: 1, spark_name: "minTokenLength",
                                       doc: "minimum token length (>= 0)"
        param :gaps, :boolean, default: true, doc: "whether the regex splits on gaps (true) or matches tokens (false)"
        param :pattern, :string, default: "\\s+", doc: "the regex pattern used for tokenizing"
        param :to_lowercase, :boolean, default: true, spark_name: "toLowercase",
                                       doc: "whether to convert all characters to lowercase before tokenizing"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
      end

      # Transforms the input dataset with a SQL statement; the input is `__THIS__` (`SQLTransformer`).
      class SQLTransformer < Transformer
        operator "org.apache.spark.ml.feature.SQLTransformer"

        param :statement, :string, doc: "SQL statement, e.g. SELECT ... FROM __THIS__"
      end

      # Filters stop words out of token arrays (`StopWordsRemover`).
      # The default `stopWords` is the server's English list.
      class StopWordsRemover < Transformer
        operator "org.apache.spark.ml.feature.StopWordsRemover"

        param :stop_words, :string_array, spark_name: "stopWords",
                                          doc: "the words to be filtered out (server default: English stop words)"
        param :case_sensitive, :boolean, default: false, spark_name: "caseSensitive",
                                         doc: "whether to do a case-sensitive comparison over the stop words"
        param :locale, :string, doc: "locale of the input for case insensitive matching (server default: JVM locale)"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
        param :input_cols, :string_array, doc: "input column names (multi-column mode)"
        param :output_cols, :string_array, doc: "output column names (multi-column mode)"
      end

      # Hashes several columns into one vector with the hashing trick (`FeatureHasher`).
      class FeatureHasher < Transformer
        operator "org.apache.spark.ml.feature.FeatureHasher"

        param :num_features, :int, default: 1 << 18, spark_name: "numFeatures",
                                   doc: "number of features, the size of the output vector (> 0)"
        param :input_cols, :string_array, doc: "input column names"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
        param :categorical_cols, :string_array, spark_name: "categoricalCols",
                                                doc: "numeric columns to treat as categorical"
      end

      # Multiplies each vector element by a fixed scaling vector (`ElementwiseProduct`).
      class ElementwiseProduct < Transformer
        operator "org.apache.spark.ml.feature.ElementwiseProduct"

        param :scaling_vec, :vector, spark_name: "scalingVec", doc: "the vector to multiply with input vectors"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
      end

      # Maps a sequence of terms to term frequencies with the hashing trick (`HashingTF`).
      class HashingTF < Transformer
        operator "org.apache.spark.ml.feature.HashingTF"

        param :num_features, :int, default: 1 << 18, spark_name: "numFeatures",
                                   doc: "number of features, the size of the output vector (> 0)"
        param :binary, :boolean, default: false, doc: "whether all non-zero counts are set to 1.0"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
      end

      # Maps indices back to the original string labels (`IndexToString`).
      class IndexToString < Transformer
        operator "org.apache.spark.ml.feature.IndexToString"

        param :input_col, :string, doc: "input column name (numeric label indices)"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
        param :labels, :string_array, doc: "the index-to-string mapping; unset means the input column's metadata is used"
      end

      # Expands vectors into all polynomial terms up to `degree` (`PolynomialExpansion`).
      class PolynomialExpansion < Transformer
        operator "org.apache.spark.ml.feature.PolynomialExpansion"

        param :degree, :int, default: 2, doc: "the polynomial degree to expand (>= 1)"
        param :input_col, :string, doc: "input column name"
        param :output_col, :string, doc: "output column name (server default: <uid>__output)"
      end
    end
  end
end
