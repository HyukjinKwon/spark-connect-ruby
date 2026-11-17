# frozen_string_literal: true

# Integration specs for the feature transformers ({SparkConnect::ML::FeatureTransformers}).
# Each transformer runs on a tiny DataFrame on a live Spark Connect server and
# the output values are checked against values derived by hand. Vector inputs
# are built with VectorAssembler (vectors cannot be created from Ruby rows).
RSpec.describe SparkConnect::ML::FeatureTransformers, if: ENV.fetch("SPARK_REMOTE", nil) do
  # Numeric array params (splits, indices) are sent as SPECIALIZED_ARRAY literals,

  before(:all) do
    skip "set SPARK_REMOTE to run integration specs" unless ENV["SPARK_REMOTE"]
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE", nil)).create
  end

  after(:all) do
    @session&.stop
  end

  let(:session) { @session }
  let(:ft) { described_class }

  # A DataFrame with columns a, b, c (doubles) and v = [a, b, c] as a vector.
  let(:vector_frame) do
    rows = [{ "a" => 1.0, "b" => 2.0, "c" => 3.0 }]
    ft::VectorAssembler.new.set_input_cols(%w[a b c]).set_output_col("v").transform(session.create_data_frame(rows))
  end

  # Asserts the dense values of a vector cell against +expected+ within +tol+.
  def expect_vector(actual, expected, tol: 1e-9)
    values = actual.to_a
    expect(values.size).to eq(expected.size)
    expected.zip(values).each { |want, got| expect(got).to be_within(tol).of(want) }
  end

  describe "VectorAssembler" do
    it "concatenates the input columns into one vector" do
      rows = session.create_data_frame([{ "a" => 1.0, "b" => 2.0, "c" => 3.0 }])
      out = ft::VectorAssembler.new.set_input_cols(%w[a b c]).set_output_col("v").transform(rows).collect
      expect_vector(out.first["v"], [1.0, 2.0, 3.0])
    end
  end

  describe "Binarizer" do
    it "maps values above the threshold to 1.0 and the rest to 0.0" do
      df = session.create_data_frame([{ "x" => 0.2 }, { "x" => 0.7 }, { "x" => 1.5 }])
      out = ft::Binarizer.new.set_input_col("x").set_output_col("b").set_threshold(0.5).transform(df)
      expect(out.collect.map { |r| r["b"] }).to eq([0.0, 1.0, 1.0])
    end

    it "uses the default threshold 0.0" do
      df = session.create_data_frame([{ "x" => -1.0 }, { "x" => 0.0 }, { "x" => 2.0 }])
      out = ft::Binarizer.new.set_input_col("x").set_output_col("b").transform(df)
      expect(out.collect.map { |r| r["b"] }).to eq([0.0, 0.0, 1.0])
    end
  end

  describe "Bucketizer" do
    it "maps each value to the index of its bucket" do
      df = session.create_data_frame([{ "x" => 5.0 }, { "x" => 15.0 }, { "x" => 10.0 }])
      out = ft::Bucketizer.new.set_splits([0.0, 10.0, 20.0]).set_input_col("x").set_output_col("b").transform(df)
      expect(out.collect.map { |r| r["b"] }).to eq([0.0, 1.0, 1.0])
    end
  end

  describe "VectorSlicer" do
    it "keeps the requested indices of the vector" do
      out = ft::VectorSlicer.new.set_indices([0, 2]).set_input_col("v").set_output_col("s").transform(vector_frame)
      expect_vector(out.collect.first["s"], [1.0, 3.0])
    end
  end

  describe "VectorSizeHint" do
    it "keeps rows whose vectors have the hinted size" do
      out = ft::VectorSizeHint.new.set_size(3).set_input_col("v").transform(vector_frame)
      expect_vector(out.collect.first["v"], [1.0, 2.0, 3.0])
    end

    it "rejects a size that conflicts with the size already in the column metadata" do
      hint = ft::VectorSizeHint.new.set_size(2).set_input_col("v")
      expect { hint.transform(vector_frame).collect }
        .to raise_error(SparkConnect::SparkConnectError, /size already set to 3/)
    end
  end

  describe "Normalizer" do
    it "scales each vector to unit p-norm (p = 1 divides by the sum of absolute values)" do
      out = ft::Normalizer.new.set_p(1.0).set_input_col("v").set_output_col("n").transform(vector_frame)
      expect_vector(out.collect.first["n"], [1.0 / 6, 2.0 / 6, 3.0 / 6])
    end
  end

  describe "ElementwiseProduct" do
    it "multiplies each element by the scaling vector" do
      scaled = ft::ElementwiseProduct.new.set_scaling_vec(
        SparkConnect::ML::Linalg::Vectors.dense(2.0, 0.5, 10.0)
      ).set_input_col("v").set_output_col("p")
      expect_vector(scaled.transform(vector_frame).collect.first["p"], [2.0, 1.0, 30.0])
    end
  end

  describe "DCT" do
    let(:constant_frame) do
      rows = [{ "a" => 1.0, "b" => 1.0, "c" => 1.0, "d" => 1.0 }]
      ft::VectorAssembler.new.set_input_cols(%w[a b c d]).set_output_col("v").transform(session.create_data_frame(rows))
    end

    it "transforms a constant vector to a single DC coefficient (orthonormal DCT-II)" do
      # sum(1,1,1,1) / sqrt(4) = 2; the other coefficients vanish.
      out = ft::DCT.new.set_input_col("v").set_output_col("dct").transform(constant_frame)
      expect_vector(out.collect.first["dct"], [2.0, 0.0, 0.0, 0.0], tol: 1e-9)
    end

    it "inverts the forward transform when inverse is set" do
      forward = ft::DCT.new.set_input_col("v").set_output_col("dct").transform(constant_frame)
      back = ft::DCT.new.set_inverse(true).set_input_col("dct").set_output_col("back").transform(forward)
      expect_vector(back.collect.first["back"], [1.0, 1.0, 1.0, 1.0])
    end
  end

  describe "Interaction" do
    it "multiplies the numeric input columns into a one-element vector" do
      rows = [{ "a" => 2.0, "b" => 3.0 }]
      out = ft::Interaction.new.set_input_cols(%w[a b]).set_output_col("i").transform(session.create_data_frame(rows))
      expect_vector(out.collect.first["i"], [6.0])
    end
  end

  describe "Tokenizer" do
    it "lowercases the text and splits it on whitespace" do
      df = session.create_data_frame([{ "text" => "Hello World foo" }])
      out = ft::Tokenizer.new.set_input_col("text").set_output_col("words").transform(df)
      expect(out.collect.first["words"]).to eq(%w[hello world foo])
    end
  end

  describe "RegexTokenizer" do
    it "splits on the pattern and drops tokens shorter than min_token_length" do
      df = session.create_data_frame([{ "text" => "A,BB;C" }])
      out = ft::RegexTokenizer.new.set_pattern("[,;]").set_min_token_length(2)
                              .set_input_col("text").set_output_col("words").transform(df)
      expect(out.collect.first["words"]).to eq(%w[bb])
    end

    it "splits on whitespace by default and lowercases" do
      df = session.create_data_frame([{ "text" => "Alpha BETA" }])
      out = ft::RegexTokenizer.new.set_input_col("text").set_output_col("words").transform(df)
      expect(out.collect.first["words"]).to eq(%w[alpha beta])
    end
  end

  describe "NGram" do
    it "produces the bigrams of an array of tokens" do
      df = session.create_data_frame([{ "t" => %w[a b c] }])
      out = ft::NGram.new.set_input_col("t").set_output_col("g").transform(df)
      expect(out.collect.first["g"]).to eq(["a b", "b c"])
    end

    it "produces one trigram when n is 3" do
      df = session.create_data_frame([{ "t" => %w[a b c] }])
      out = ft::NGram.new.set_n(3).set_input_col("t").set_output_col("g").transform(df)
      expect(out.collect.first["g"]).to eq(["a b c"])
    end
  end

  describe "StopWordsRemover" do
    let(:df) { session.create_data_frame([{ "t" => %w[The cat the sat] }]) }

    it "removes stop words case-insensitively by default" do
      remover = ft::StopWordsRemover.new.set_stop_words(["the"]).set_input_col("t").set_output_col("f")
      expect(remover.transform(df).collect.first["f"]).to eq(%w[cat sat])
    end

    it "keeps the case of the stop word when case_sensitive is set" do
      remover = ft::StopWordsRemover.new.set_stop_words(["the"]).set_case_sensitive(true)
                                    .set_input_col("t").set_output_col("f")
      expect(remover.transform(df).collect.first["f"]).to eq(%w[The cat sat])
    end
  end

  describe "SQLTransformer" do
    it "runs the statement with __THIS__ as the input" do
      sql = ft::SQLTransformer.new.set_statement("SELECT id, id * 2 AS doubled FROM __THIS__ WHERE id > 0")
      rows = sql.transform(session.range(4)).collect.map(&:to_h)
      expect(rows).to eq([{ "id" => 1, "doubled" => 2 }, { "id" => 2, "doubled" => 4 }, { "id" => 3, "doubled" => 6 }])
    end
  end

  describe "FeatureHasher" do
    it "hashes numeric and string columns into a vector of numFeatures" do
      df = session.create_data_frame([{ "a" => 2.0, "s" => "red" }])
      out = ft::FeatureHasher.new.set_input_cols(%w[a s]).set_num_features(16).set_output_col("h").transform(df)
      values = out.collect.first["h"].to_a
      expect(values.size).to eq(16)
      # The numeric value 2.0 and the one-hot "s=red" (1.0) add up to 3.0.
      expect(values.sum).to be_within(1e-9).of(3.0)
    end

    it "one-hot encodes the categorical columns" do
      df = session.create_data_frame([{ "n" => 2.0 }])
      hasher = ft::FeatureHasher.new.set_input_cols(["n"]).set_categorical_cols(["n"])
                                .set_num_features(16).set_output_col("h")
      expect(hasher.transform(df).collect.first["h"].to_a.sum).to be_within(1e-9).of(1.0)
    end
  end

  describe "HashingTF" do
    let(:terms) { session.create_data_frame([{ "t" => %w[a b a] }]) }

    it "counts each term into a vector of numFeatures" do
      out = ft::HashingTF.new.set_num_features(1000).set_input_col("t").set_output_col("h").transform(terms)
      values = out.collect.first["h"].to_a
      expect(values.size).to eq(1000)
      expect(values.sum).to be_within(1e-9).of(3.0)
    end

    it "sets counts to at most 1.0 when binary" do
      out = ft::HashingTF.new.set_num_features(1000).set_binary(true).set_input_col("t").set_output_col("h")
                         .transform(terms)
      values = out.collect.first["h"].to_a
      expect(values.max).to be <= 1.0
      expect(values.sum).to be_between(1.0, 2.0)
    end
  end

  describe "IndexToString" do
    it "maps the numeric indices back to the labels" do
      df = session.create_data_frame([{ "i" => 0.0 }, { "i" => 2.0 }, { "i" => 1.0 }])
      out = ft::IndexToString.new.set_labels(%w[x y z]).set_input_col("i").set_output_col("s").transform(df)
      expect(out.collect.map { |r| r["s"] }).to eq(%w[x z y])
    end
  end

  describe "PolynomialExpansion" do
    it "expands a two-element vector to the degree-2 terms" do
      # Terms of (a, b) at degree 2: a, a^2, b, ab, b^2 with a = 1, b = 2.
      out = ft::PolynomialExpansion.new.set_input_col("v").set_output_col("p").transform(
        ft::VectorAssembler.new.set_input_cols(%w[a b]).set_output_col("v").transform(
          session.create_data_frame([{ "a" => 1.0, "b" => 2.0 }])
        )
      )
      values = out.collect.first["p"].to_a
      expect(values.size).to eq(5)
      expect(values.sort).to eq([1.0, 1.0, 2.0, 2.0, 4.0])
    end
  end
end
