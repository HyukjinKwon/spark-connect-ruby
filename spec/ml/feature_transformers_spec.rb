# frozen_string_literal: true

require_relative "support"

FT = SparkConnect::ML::FeatureTransformers

# Every transformer of the family, keyed by class.
FT_CLASSES = {
  FT::DCT => "DCT",
  FT::NGram => "NGram",
  FT::Normalizer => "Normalizer",
  FT::Interaction => "Interaction",
  FT::Binarizer => "Binarizer",
  FT::Bucketizer => "Bucketizer",
  FT::VectorAssembler => "VectorAssembler",
  FT::VectorSlicer => "VectorSlicer",
  FT::VectorSizeHint => "VectorSizeHint",
  FT::Tokenizer => "Tokenizer",
  FT::RegexTokenizer => "RegexTokenizer",
  FT::SQLTransformer => "SQLTransformer",
  FT::StopWordsRemover => "StopWordsRemover",
  FT::FeatureHasher => "FeatureHasher",
  FT::ElementwiseProduct => "ElementwiseProduct",
  FT::HashingTF => "HashingTF",
  FT::IndexToString => "IndexToString",
  FT::PolynomialExpansion => "PolynomialExpansion",
}.freeze

# Unit specs for the feature transformers. No server: transform only builds a
# lazy MlRelation, which these specs inspect.
RSpec.describe SparkConnect::ML::FeatureTransformers do
  include MlSpecSupport

  let(:session) { ml_session }
  let(:frame) { ml_frame(session) }

  it "declares the 18 allowlisted transformers" do
    expect(FT_CLASSES.size).to eq(18)
    expect(FT.constants.map { |c| FT.const_get(c) }.grep(Class).select { |k| k < SparkConnect::ML::Transformer })
      .to match_array(FT_CLASSES.keys)
  end

  describe "operator names" do
    it "uses the org.apache.spark.ml.feature Scala class names" do
      FT_CLASSES.each do |klass, short|
        expect(klass.operator).to eq("org.apache.spark.ml.feature.#{short}")
      end
    end

    it "gives every declared class an org.apache.spark.ml. operator" do
      FT_CLASSES.each_key do |klass|
        expect(klass.operator).to start_with("org.apache.spark.ml.")
      end
    end
  end

  describe "defaults" do
    it "NGram defaults n to 2" do
      expect(FT::NGram.new.n).to eq(2)
    end

    it "Normalizer defaults p to 2.0" do
      expect(FT::Normalizer.new.p).to eq(2.0)
    end

    it "Binarizer defaults threshold to 0.0 and leaves thresholds unset" do
      transformer = FT::Binarizer.new
      expect(transformer.threshold).to eq(0.0)
      expect(transformer.is_set?(:thresholds)).to be(false)
    end

    it "HashingTF defaults numFeatures to 2^18 and binary to false" do
      transformer = FT::HashingTF.new
      expect(transformer.num_features).to eq(262_144)
      expect(transformer.binary).to be(false)
      expect(transformer.param(:num_features).spark_name).to eq("numFeatures")
    end

    it "VectorAssembler and Bucketizer default handleInvalid to error" do
      expect(FT::VectorAssembler.new.handle_invalid).to eq("error")
      expect(FT::Bucketizer.new.handle_invalid).to eq("error")
    end

    it "RegexTokenizer defaults pattern, gaps and toLowercase" do
      transformer = FT::RegexTokenizer.new
      expect(transformer.pattern).to eq("\\s+")
      expect(transformer.gaps).to be(true)
      expect(transformer.to_lowercase).to be(true)
      expect(transformer.min_token_length).to eq(1)
    end

    it "PolynomialExpansion defaults degree to 2" do
      expect(FT::PolynomialExpansion.new.degree).to eq(2)
    end

    it "params without a Spark default raise when read unset" do
      expect { FT::SQLTransformer.new.statement }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "uses Spark names for the camelCase params" do
      expect(FT::Bucketizer.new.param(:splits_array).spark_name).to eq("splitsArray")
      expect(FT::RegexTokenizer.new.param(:to_lowercase).spark_name).to eq("toLowercase")
      expect(FT::ElementwiseProduct.new.param(:scaling_vec).spark_name).to eq("scalingVec")
      expect(FT::FeatureHasher.new.param(:categorical_cols).spark_name).to eq("categoricalCols")
    end
  end

  describe "set_ chains" do
    it "returns the transformer from each setter" do
      transformer = FT::Binarizer.new
      expect(transformer.set_threshold(0.5).set_input_col("x").set_output_col("b")).to be(transformer)
      expect(transformer.threshold).to eq(0.5)
      expect(transformer.input_col).to eq("x")
      expect(transformer.output_col).to eq("b")
    end

    it "chains multi-column and vector setters" do
      assembler = FT::VectorAssembler.new.set_input_cols(%w[a b]).set_output_col("features")
      expect(assembler.input_cols).to eq(%w[a b])
      slicer = FT::VectorSlicer.new.set_indices([0, 2]).set_names(%w[x y z])
      expect(slicer.indices).to eq([0, 2])
      expect(slicer.names).to eq(%w[x y z])
    end
  end

  describe "#transform" do
    it "builds an MlRelation.transform with the Spark transformer name and type" do
      df = FT::Tokenizer.new.set_input_col("text").set_output_col("words").transform(frame)
      transform = df.relation.ml_relation.transform
      expect(transform.transformer.name).to eq("org.apache.spark.ml.feature.Tokenizer")
      expect(transform.transformer.type).to eq(:OPERATOR_TYPE_TRANSFORMER)
    end

    it "keys the transform params by Spark name and sends only explicit values" do
      transformer = FT::RegexTokenizer.new.set_min_token_length(2).set_to_lowercase(false)
      transform = transformer.transform(frame).relation.ml_relation.transform
      expect(transform.params.params.keys).to contain_exactly("minTokenLength", "toLowercase")
      expect(transform.params.params["minTokenLength"].integer).to eq(2)
      expect(transform.params.params["toLowercase"].boolean).to be(false)
    end

    it "keeps the input relation" do
      transform = FT::NGram.new.transform(frame).relation.ml_relation.transform
      expect(transform.input).to eq(frame.relation)
    end

    it "is lazy: transform runs no command on the client" do
      client = MlSpecSupport::FakeMlClient.new
      FT::DCT.new.transform(ml_frame(ml_session(client)))
      expect(client.ml_commands).to be_empty
    end
  end
end
