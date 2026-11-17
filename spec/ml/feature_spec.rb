# frozen_string_literal: true

require_relative "support"

# Unit specs for the feature operator declarations (no server needed).
RSpec.describe SparkConnect::ML::Feature do
  include MlSpecSupport

  FeatureOps = described_class

  def expect_defaults(klass, expected)
    expected.each do |name, value|
      declared = klass.params.fetch(name)
      expect(declared.default).to eq(value), "#{klass.name}##{name} default"
    end
  end

  describe "operator names" do
    {
      FeatureOps::QuantileDiscretizer => "org.apache.spark.ml.feature.QuantileDiscretizer",
      FeatureOps::BucketizerModel => "org.apache.spark.ml.feature.Bucketizer",
      FeatureOps::RFormula => "org.apache.spark.ml.feature.RFormula",
      FeatureOps::RFormulaModel => "org.apache.spark.ml.feature.RFormulaModel",
      FeatureOps::Imputer => "org.apache.spark.ml.feature.Imputer",
      FeatureOps::ImputerModel => "org.apache.spark.ml.feature.ImputerModel",
      FeatureOps::StandardScaler => "org.apache.spark.ml.feature.StandardScaler",
      FeatureOps::StandardScalerModel => "org.apache.spark.ml.feature.StandardScalerModel",
      FeatureOps::MaxAbsScaler => "org.apache.spark.ml.feature.MaxAbsScaler",
      FeatureOps::MaxAbsScalerModel => "org.apache.spark.ml.feature.MaxAbsScalerModel",
      FeatureOps::MinMaxScaler => "org.apache.spark.ml.feature.MinMaxScaler",
      FeatureOps::MinMaxScalerModel => "org.apache.spark.ml.feature.MinMaxScalerModel",
      FeatureOps::RobustScaler => "org.apache.spark.ml.feature.RobustScaler",
      FeatureOps::RobustScalerModel => "org.apache.spark.ml.feature.RobustScalerModel",
      FeatureOps::ChiSqSelector => "org.apache.spark.ml.feature.ChiSqSelector",
      FeatureOps::ChiSqSelectorModel => "org.apache.spark.ml.feature.ChiSqSelectorModel",
      FeatureOps::UnivariateFeatureSelector => "org.apache.spark.ml.feature.UnivariateFeatureSelector",
      FeatureOps::UnivariateFeatureSelectorModel => "org.apache.spark.ml.feature.UnivariateFeatureSelectorModel",
      FeatureOps::VarianceThresholdSelector => "org.apache.spark.ml.feature.VarianceThresholdSelector",
      FeatureOps::VarianceThresholdSelectorModel => "org.apache.spark.ml.feature.VarianceThresholdSelectorModel",
      FeatureOps::StringIndexer => "org.apache.spark.ml.feature.StringIndexer",
      FeatureOps::StringIndexerModel => "org.apache.spark.ml.feature.StringIndexerModel",
      FeatureOps::VectorIndexer => "org.apache.spark.ml.feature.VectorIndexer",
      FeatureOps::VectorIndexerModel => "org.apache.spark.ml.feature.VectorIndexerModel",
      FeatureOps::PCA => "org.apache.spark.ml.feature.PCA",
      FeatureOps::PCAModel => "org.apache.spark.ml.feature.PCAModel",
      FeatureOps::IDF => "org.apache.spark.ml.feature.IDF",
      FeatureOps::IDFModel => "org.apache.spark.ml.feature.IDFModel",
      FeatureOps::Word2Vec => "org.apache.spark.ml.feature.Word2Vec",
      FeatureOps::Word2VecModel => "org.apache.spark.ml.feature.Word2VecModel",
      FeatureOps::CountVectorizer => "org.apache.spark.ml.feature.CountVectorizer",
      FeatureOps::CountVectorizerModel => "org.apache.spark.ml.feature.CountVectorizerModel",
      FeatureOps::OneHotEncoder => "org.apache.spark.ml.feature.OneHotEncoder",
      FeatureOps::OneHotEncoderModel => "org.apache.spark.ml.feature.OneHotEncoderModel",
      FeatureOps::TargetEncoder => "org.apache.spark.ml.feature.TargetEncoder",
      FeatureOps::TargetEncoderModel => "org.apache.spark.ml.feature.TargetEncoderModel",
      FeatureOps::BucketedRandomProjectionLSH => "org.apache.spark.ml.feature.BucketedRandomProjectionLSH",
      FeatureOps::BucketedRandomProjectionLSHModel => "org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel",
      FeatureOps::MinHashLSH => "org.apache.spark.ml.feature.MinHashLSH",
      FeatureOps::MinHashLSHModel => "org.apache.spark.ml.feature.MinHashLSHModel",
    }.each do |klass, name|
      it "#{klass.name.split('::').last} wraps #{name}" do
        expect(klass.operator).to eq(name)
      end
    end

    it "declares an operator on every concrete class, under org.apache.spark.ml.feature" do
      concrete = FeatureOps.constants.map { |c| FeatureOps.const_get(c) }
                           .select { |k| k.is_a?(Class) && k < SparkConnect::ML::Params }
                           .reject { |k| k.name.end_with?("Base") }
      expect(concrete.size).to eq(40)
      concrete.each do |klass|
        expect(klass.operator).to start_with("org.apache.spark.ml."), "#{klass.name} has no operator"
      end
    end

    it "resolves each operator's Ruby constant in the Feature module" do
      FeatureOps.constants.each do |c|
        klass = FeatureOps.const_get(c)
        next unless klass.is_a?(Class) && klass < SparkConnect::ML::Params && !klass.name.end_with?("Base")

        expect(FeatureOps.const_get(klass.name.split("::").last)).to eq(klass)
        expect(klass.operator).to match(/\Aorg\.apache\.spark\.ml\.feature\.\w+\z/)
      end
    end
  end

  describe "defaults (Spark 4.3.0)" do
    it "QuantileDiscretizer" do
      expect_defaults(FeatureOps::QuantileDiscretizer, num_buckets: 2, relative_error: 0.001, handle_invalid: "error")
      expect(FeatureOps::QuantileDiscretizer.params.fetch(:num_buckets_array).default?).to be(false)
    end

    it "Imputer and ImputerModel" do
      expect_defaults(FeatureOps::Imputer, strategy: "mean", relative_error: 0.001)
      expect(FeatureOps::Imputer.params.fetch(:missing_value).default).to be_nan
      expect_defaults(FeatureOps::ImputerModel, strategy: "mean")
    end

    it "RFormula" do
      expect_defaults(FeatureOps::RFormula, features_col: "features", label_col: "label", force_index_label: false,
                                            string_indexer_order_type: "frequencyDesc", handle_invalid: "error")
      expect(FeatureOps::RFormula.params.fetch(:formula).default?).to be(false)
    end

    it "StandardScaler, MinMaxScaler and RobustScaler" do
      expect_defaults(FeatureOps::StandardScaler, with_mean: false, with_std: true)
      expect_defaults(FeatureOps::MinMaxScaler, min: 0.0, max: 1.0)
      expect_defaults(FeatureOps::RobustScaler, lower: 0.25, upper: 0.75, with_centering: false, with_scaling: true,
                                                relative_error: 0.001)
    end

    it "CountVectorizer" do
      expect_defaults(FeatureOps::CountVectorizer, min_tf: 1.0, min_df: 1.0, vocab_size: 2**18, binary: false)
      expect(FeatureOps::CountVectorizer.params.fetch(:max_df).default).to eq(9_223_372_036_854_775_807.0)
    end

    it "Word2Vec" do
      expect_defaults(FeatureOps::Word2Vec, vector_size: 100, window_size: 5, min_count: 5, max_iter: 1,
                                            step_size: 0.025, max_sentence_length: 1000)
      expect(FeatureOps::Word2Vec.params.fetch(:seed).default?).to be(true)
    end

    it "StringIndexer, OneHotEncoder, TargetEncoder and VectorIndexer" do
      expect_defaults(FeatureOps::StringIndexer, handle_invalid: "error", string_order_type: "frequencyDesc")
      expect_defaults(FeatureOps::OneHotEncoder, drop_last: true, handle_invalid: "error")
      expect_defaults(FeatureOps::TargetEncoder, target_type: "binary", smoothing: 0.0, label_col: "label")
      expect_defaults(FeatureOps::VectorIndexer, max_categories: 20)
    end

    it "selectors" do
      expect_defaults(FeatureOps::ChiSqSelector, num_top_features: 50, percentile: 0.1, fpr: 0.05, fdr: 0.05,
                                                 fwe: 0.05, selector_type: "numTopFeatures")
      expect_defaults(FeatureOps::UnivariateFeatureSelector, selection_mode: "numTopFeatures")
      expect_defaults(FeatureOps::VarianceThresholdSelector, variance_threshold: 0.0)
    end

    it "LSH classes" do
      expect_defaults(FeatureOps::MinHashLSH, num_hash_tables: 1)
      expect_defaults(FeatureOps::BucketedRandomProjectionLSH, num_hash_tables: 1)
      expect(FeatureOps::BucketedRandomProjectionLSH.params.fetch(:bucket_length).default?).to be(false)
    end
  end

  describe "param names and spark names" do
    it "uses the Spark camelCase names on the wire" do
      expect(FeatureOps::CountVectorizer.params.fetch(:min_df).spark_name).to eq("minDF")
      expect(FeatureOps::CountVectorizer.params.fetch(:max_df).spark_name).to eq("maxDF")
      expect(FeatureOps::CountVectorizer.params.fetch(:vocab_size).spark_name).to eq("vocabSize")
      expect(FeatureOps::IDF.params.fetch(:min_doc_freq).spark_name).to eq("minDocFreq")
      expect(FeatureOps::MinMaxScaler.params.fetch(:min).spark_name).to eq("min")
      expect(FeatureOps::BucketedRandomProjectionLSH.params.fetch(:num_hash_tables).spark_name).to eq("numHashTables")
    end

    it "declares inherited shared params on the classes" do
      expect(FeatureOps::StringIndexer.params.keys).to include(:input_cols, :output_cols, :handle_invalid)
      expect(FeatureOps::Imputer.params.keys).to include(:relative_error, :input_cols, :output_cols)
    end
  end

  describe "chainable setters" do
    it "sets params with the set_ chain" do
      est = FeatureOps::StringIndexer.new.set_input_col("cat").set_output_col("idx")
                                     .set_string_order_type("alphabetAsc")
      expect(est.input_col).to eq("cat")
      expect(est.output_col).to eq("idx")
      expect(est.string_order_type).to eq("alphabetAsc")
    end

    it "returns self from set_params" do
      est = FeatureOps::StandardScaler.new
      expect(est.set_params(with_mean: true, input_col: "x")).to be(est)
      expect(est.with_mean).to be(true)
    end

    it "keeps defaults when unset" do
      expect(FeatureOps::StandardScaler.new.with_std).to be(true)
    end
  end

  describe "fit" do
    let(:client) { MlSpecSupport::FakeMlClient.new }
    let(:session) { ml_session(client) }

    it "sends a fit command with the estimator name, estimator type and explicit params keyed by Spark name" do
      est = FeatureOps::StringIndexer.new(input_col: "cat", string_order_type: "alphabetAsc")
      model = est.fit(ml_frame(session))

      fit = first_ml(client, :fit).fit
      expect(fit.estimator.name).to eq("org.apache.spark.ml.feature.StringIndexer")
      expect(fit.estimator.type).to eq(:OPERATOR_TYPE_ESTIMATOR)
      expect(fit.params.params.keys).to contain_exactly("inputCol", "stringOrderType")
      expect(SparkConnect::ML::Serialize.decode(fit.params.params["stringOrderType"])).to eq("alphabetAsc")
      expect(model).to be_a(FeatureOps::StringIndexerModel)
      model.release
    end

    it "sends only explicit params, not defaults" do
      est = FeatureOps::MinMaxScaler.new(input_col: "x")
      est.fit(ml_frame(session)).release
      expect(first_ml(client, :fit).fit.params.params.keys).to contain_exactly("inputCol")
    end

    it "maps the NaN-missing-value and Spark names for Imputer" do
      FeatureOps::Imputer.new(input_cols: %w[a b], output_cols: %w[a_out b_out], strategy: "median")
                         .fit(ml_frame(session)).release
      expect(first_ml(client, :fit).fit.params.params.keys).to contain_exactly("inputCols", "outputCols", "strategy")
    end

    it "fits every estimator and returns the declared model class" do
      estimators = [
        FeatureOps::QuantileDiscretizer, FeatureOps::RFormula, FeatureOps::Imputer, FeatureOps::StandardScaler,
        FeatureOps::MaxAbsScaler, FeatureOps::MinMaxScaler, FeatureOps::RobustScaler, FeatureOps::ChiSqSelector,
        FeatureOps::UnivariateFeatureSelector, FeatureOps::VarianceThresholdSelector, FeatureOps::StringIndexer,
        FeatureOps::VectorIndexer, FeatureOps::PCA, FeatureOps::IDF, FeatureOps::Word2Vec, FeatureOps::CountVectorizer,
        FeatureOps::OneHotEncoder, FeatureOps::TargetEncoder, FeatureOps::BucketedRandomProjectionLSH, FeatureOps::MinHashLSH,
      ]
      estimators.each do |klass|
        est = klass.new
        model = est.fit(ml_frame(session))
        expect(model).to be_a(klass.model)
        expect(client.ml_commands.reverse.find(&:fit).fit.estimator.name).to eq(klass.operator)
        model.release
      end
    end

    it "QuantileDiscretizer fits into a Bucketizer model" do
      model = FeatureOps::QuantileDiscretizer.new(num_buckets: 4).fit(ml_frame(session))
      expect(model).to be_a(FeatureOps::BucketizerModel)
      expect(FeatureOps::BucketizerModel.operator).to eq("org.apache.spark.ml.feature.Bucketizer")
      model.release
    end
  end

  describe "model attributes" do
    let(:client) { MlSpecSupport::FakeMlClient.new }
    let(:session) { ml_session(client) }

    def attached(klass)
      klass.new.tap { |m| m.attach(session, "model-42") }
    end

    {
      [FeatureOps::StandardScalerModel, :mean] => "mean",
      [FeatureOps::StandardScalerModel, :std] => "std",
      [FeatureOps::MaxAbsScalerModel, :max_abs] => "maxAbs",
      [FeatureOps::MinMaxScalerModel, :original_max] => "originalMax",
      [FeatureOps::RobustScalerModel, :median] => "median",
      [FeatureOps::PCAModel, :explained_variance] => "explainedVariance",
      [FeatureOps::PCAModel, :pc] => "pc",
      [FeatureOps::IDFModel, :idf] => "idf",
      [FeatureOps::IDFModel, :num_docs] => "numDocs",
      [FeatureOps::CountVectorizerModel, :vocabulary] => "vocabulary",
      [FeatureOps::OneHotEncoderModel, :category_sizes] => "categorySizes",
      [FeatureOps::StringIndexerModel, :labels] => "labels",
      [FeatureOps::RFormulaModel, :resolved_formula_string] => "resolvedFormulaString",
      [FeatureOps::ChiSqSelectorModel, :selected_features] => "selectedFeatures",
      [FeatureOps::UnivariateFeatureSelectorModel, :selected_features] => "selectedFeatures",
      [FeatureOps::VarianceThresholdSelectorModel, :selected_features] => "selectedFeatures",
      [FeatureOps::VectorIndexerModel, :num_features] => "numFeatures",
      [FeatureOps::BucketizerModel, :get_splits] => "getSplits",
      [FeatureOps::BucketizerModel, :get_splits_array] => "getSplitsArray",
    }.each do |(klass, attr), spark_method|
      it "#{klass.name.split('::').last}##{attr} fetches #{spark_method}" do
        attached(klass).public_send(attr)
        fetch = client.last_ml_command.fetch
        expect(fetch.obj_ref.id).to eq("model-42")
        expect(fetch["methods"].map { |m| m["method"] }).to eq([spark_method])
      end
    end

    it "find_synonyms sends findSynonyms with two arguments" do
      model = attached(FeatureOps::Word2VecModel)
      df = model.find_synonyms("spark", 3)
      expect(df).to be_a(SparkConnect::DataFrame)
      fetch = df.relation.ml_relation.fetch
      expect(fetch["methods"].map { |m| m["method"] }).to eq(["findSynonyms"])
      expect(fetch["methods"].first["args"].size).to eq(2)
    end

    it "find_synonyms rejects the wrong number of arguments" do
      expect { attached(FeatureOps::Word2VecModel).find_synonyms("spark") }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "LSH models expose approximate nearest neighbours and joins with four arguments" do
      model = attached(FeatureOps::MinHashLSHModel)
      df = model.approx_nearest_neighbors(ml_frame(session), SparkConnect::ML::Linalg::Vectors.dense([1.0]), 2, "d")
      expect(df.relation.ml_relation.fetch["methods"].first["method"]).to eq("approxNearestNeighbors")
      join = model.approx_similarity_join(ml_frame(session), ml_frame(session), 0.5, "d")
      expect(join.relation.ml_relation.fetch["methods"].first["method"]).to eq("approxSimilarityJoin")
      expect(join.relation.ml_relation.fetch["methods"].first["args"].size).to eq(4)
    end
  end
end
