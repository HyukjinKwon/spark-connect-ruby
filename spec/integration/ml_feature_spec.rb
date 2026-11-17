# frozen_string_literal: true

# Integration specs for the feature operators (`SparkConnect::ML::Feature`). Each
# estimator is fitted and transformed on a tiny dataset whose outputs are derived
# by hand. Only runs when SPARK_REMOTE is set.
RSpec.describe "ML feature operators (integration)", :integration, if: ENV.fetch("SPARK_REMOTE", nil) do
  before(:all) do
    skip "set SPARK_REMOTE to run integration specs" unless ENV["SPARK_REMOTE"]
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE", nil)).create
  end

  after(:all) do
    @session&.stop
  end

  let(:session) { @session }
  let(:vectors) { SparkConnect::ML::Linalg::Vectors }
  let(:functions) { SparkConnect::ML::Functions }

  # A DataFrame with an `id` and a `features` VectorUDT column built from `rows`
  # (arrays of doubles).
  def vectors_df(rows)
    df = session.create_data_frame(rows.each_with_index.map { |row, i| { "id" => i, "arr" => row } })
    df.select(SparkConnect::F.col("id"), functions.array_to_vector("arr").alias("features"))
  end

  # The values of a vector column, ordered by `id`, as Ruby arrays.
  def vector_values(df, col)
    df.order_by("id")
      .select(SparkConnect::F.col("id"), functions.vector_to_array(col).alias("v"))
      .collect.map { |r| r["v"].to_a }
  end

  # The values of a scalar column, ordered by `id`.
  def scalar_values(df, col)
    df.order_by("id").collect.map { |r| r[col] }
  end

  def expect_close(actual, expected, tolerance = 1e-9)
    expect(actual.size).to eq(expected.size)
    actual.zip(expected).each { |a, e| expect(a).to be_within(tolerance).of(e) }
  end

  describe "scalers" do
    it "StandardScaler divides by the unbiased standard deviation (withMean false)" do
      df = vectors_df([[1.0], [2.0], [3.0], [4.0]])
      model = SparkConnect::ML::Feature::StandardScaler.new(input_col: "features", output_col: "scaled").fit(df)
      expect_close(model.mean.to_a, [2.5])
      # sample variance = (2.25 + 0.25 + 0.25 + 2.25) / 3 = 5 / 3
      expect_close(model.std.to_a, [Math.sqrt(5.0 / 3.0)])
      out = model.transform(df)
      expect_close(vector_values(out, "scaled").flatten, [1, 2, 3, 4].map { |v| v / Math.sqrt(5.0 / 3.0) })
    end

    it "MinMaxScaler rescales to the requested [min, max] range" do
      df = vectors_df([[0.0], [5.0], [10.0]])
      model = SparkConnect::ML::Feature::MinMaxScaler.new(input_col: "features", output_col: "scaled", min: -1.0, max: 1.0).fit(df)
      expect_close(model.original_min.to_a, [0.0])
      expect_close(model.original_max.to_a, [10.0])
      expect_close(vector_values(model.transform(df), "scaled").flatten, [-1.0, 0.0, 1.0])
    end

    it "MaxAbsScaler divides by the maximum absolute value" do
      df = vectors_df([[-4.0], [2.0], [1.0]])
      model = SparkConnect::ML::Feature::MaxAbsScaler.new(input_col: "features", output_col: "scaled").fit(df)
      expect_close(model.max_abs.to_a, [4.0])
      expect_close(vector_values(model.transform(df), "scaled").flatten, [-1.0, 0.5, 0.25])
    end

    it "RobustScaler scales by the interquartile range of 1..5 (Q1 = 2, median = 3, Q3 = 4)" do
      df = vectors_df([[1.0], [2.0], [3.0], [4.0], [5.0]])
      model = SparkConnect::ML::Feature::RobustScaler.new(input_col: "features", output_col: "scaled").fit(df)
      expect_close(model.range.to_a, [2.0])
      expect_close(model.median.to_a, [3.0])
      expect_close(vector_values(model.transform(df), "scaled").flatten, [0.5, 1.0, 1.5, 2.0, 2.5])
    end
  end

  describe "imputation and transforms" do
    it "Imputer fills NaN with the mean of the observed values" do
      df = session.create_data_frame([
                                       { "id" => 0, "a" => 1.0 },
                                       { "id" => 1, "a" => Float::NAN },
                                       { "id" => 2, "a" => 3.0 },
                                       { "id" => 3, "a" => 5.0 },
                                     ])
      model = SparkConnect::ML::Feature::Imputer.new(input_cols: ["a"], output_cols: ["a_out"]).fit(df)
      expect(model.surrogate_df.collect.first["a"]).to be_within(1e-9).of(3.0)
      expect(scalar_values(model.transform(df), "a_out")).to eq([1.0, 3.0, 3.0, 5.0])
    end

    it "RFormula builds the features from the formula terms and keeps the label" do
      df = session.create_data_frame([
                                       { "id" => 0, "x1" => 1.0, "x2" => 0.0, "label" => 0.0 },
                                       { "id" => 1, "x1" => 2.0, "x2" => 1.0, "label" => 1.0 },
                                       { "id" => 2, "x1" => 3.0, "x2" => 0.0, "label" => 0.0 },
                                     ])
      model = SparkConnect::ML::Feature::RFormula.new(formula: "label ~ x1 + x2").fit(df)
      expect(model.resolved_formula_string).to include("label=label", "terms=[x1,x2]")
      out = model.transform(df)
      expect(vector_values(out, "features")).to eq([[1.0, 0.0], [2.0, 1.0], [3.0, 0.0]])
      expect(scalar_values(out, "label")).to eq([0.0, 1.0, 0.0])
    end

    it "QuantileDiscretizer fits two buckets whose model is a Bucketizer" do
      df = session.create_data_frame((1..10).map { |i| { "id" => i, "x" => i.to_f } })
      model = SparkConnect::ML::Feature::QuantileDiscretizer.new(num_buckets: 2, input_col: "x", output_col: "bucket").fit(df)
      expect(model).to be_a(SparkConnect::ML::Feature::BucketizerModel)
      splits = model.get_splits
      expect(splits.size).to eq(3)
      expect(splits.first).to eq(-Float::INFINITY)
      expect(splits.last).to eq(Float::INFINITY)
      buckets = scalar_values(model.transform(df), "bucket")
      expect(buckets.uniq.sort).to eq([0.0, 1.0])
      expect(buckets).to eq(buckets.sort)
    end
  end

  describe "selectors" do
    # Feature 0 equals the label; feature 1 is balanced within each label, so it is independent.
    let(:selection_df) do
      labels = [0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0]
      other = [0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0]
      rows = labels.zip(other).map { |l, o| [l, o] }
      session.create_data_frame(rows.each_with_index.map { |(l, o), i| { "id" => i, "arr" => [l, o], "label" => l } })
             .select(SparkConnect::F.col("id"), SparkConnect::F.col("label"),
                     functions.array_to_vector("arr").alias("features"))
    end

    it "ChiSqSelector keeps the top feature" do
      model = SparkConnect::ML::Feature::ChiSqSelector.new(num_top_features: 1, output_col: "selected").fit(selection_df)
      expect(model.selected_features).to eq([0])
      expect(vector_values(model.transform(selection_df), "selected").flatten).to eq([0.0, 0.0, 0.0, 0.0, 1.0, 1.0,
                                                                                      1.0, 1.0,])
    end

    it "UnivariateFeatureSelector selects the feature related to the label" do
      model = SparkConnect::ML::Feature::UnivariateFeatureSelector.new(
        feature_type: "categorical", label_type: "categorical", selection_mode: "numTopFeatures",
        selection_threshold: 1, output_col: "selected"
      ).fit(selection_df)
      expect(model.selected_features).to eq([0])
    end

    it "VarianceThresholdSelector drops the constant feature" do
      df = vectors_df([[1.0, 0.0], [2.0, 0.0], [3.0, 0.0], [4.0, 0.0]])
      model = SparkConnect::ML::Feature::VarianceThresholdSelector.new(output_col: "selected").fit(df)
      expect(model.selected_features).to eq([0])
      expect(vector_values(model.transform(df), "selected").flatten).to eq([1.0, 2.0, 3.0, 4.0])
    end
  end

  describe "indexers and encoders" do
    it "StringIndexer orders labels by descending frequency" do
      df = session.create_data_frame([
                                       { "id" => 0, "cat" => "a" }, { "id" => 1, "cat" => "b" },
                                       { "id" => 2, "cat" => "a" }, { "id" => 3, "cat" => "c" },
                                       { "id" => 4, "cat" => "a" }, { "id" => 5, "cat" => "b" },
                                     ])
      model = SparkConnect::ML::Feature::StringIndexer.new(input_col: "cat", output_col: "idx").fit(df)
      expect(model.labels).to eq(%w[a b c])
      expect(scalar_values(model.transform(df), "idx")).to eq([0.0, 1.0, 0.0, 2.0, 0.0, 1.0])
    end

    it "VectorIndexer indexes the feature with two values and leaves the continuous one alone" do
      df = vectors_df([[-1.0, 0.5], [1.0, 1.5], [-1.0, 2.5], [1.0, 3.5]])
      model = SparkConnect::ML::Feature::VectorIndexer.new(input_col: "features", output_col: "indexed", max_categories: 2).fit(df)
      expect(model.num_features).to eq(2)
      expect(model.category_maps_df.count).to eq(2)
      expect(vector_values(model.transform(df), "indexed")).to eq([[0.0, 0.5], [1.0, 1.5], [0.0, 2.5], [1.0, 3.5]])
    end

    it "OneHotEncoder drops the last category by default" do
      df = session.create_data_frame([{ "id" => 0, "idx" => 0 }, { "id" => 1, "idx" => 1 },
                                      { "id" => 2, "idx" => 2 }, { "id" => 3, "idx" => 0 },])
      model = SparkConnect::ML::Feature::OneHotEncoder.new(input_cols: ["idx"], output_cols: ["oh"]).fit(df)
      expect(model.category_sizes).to eq([3])
      expect(vector_values(model.transform(df), "oh")).to eq([[1.0, 0.0], [0.0, 1.0], [0.0, 0.0], [1.0, 0.0]])
    end

    it "TargetEncoder encodes each category by the share of label 1" do
      df = session.create_data_frame([
                                       { "id" => 0, "cat" => 0, "label" => 1.0 },
                                       { "id" => 1, "cat" => 0, "label" => 0.0 },
                                       { "id" => 2, "cat" => 1, "label" => 1.0 },
                                       { "id" => 3, "cat" => 1, "label" => 1.0 },
                                     ])
      model = SparkConnect::ML::Feature::TargetEncoder.new(input_col: "cat", output_col: "te", label_col: "label").fit(df)
      expect(scalar_values(model.transform(df), "te")).to eq([0.5, 0.5, 1.0, 1.0])
    end

    it "CountVectorizer builds the vocabulary by descending corpus frequency" do
      df = session.create_data_frame([
                                       { "id" => 0, "words" => %w[a a b] },
                                       { "id" => 1, "words" => %w[a b] },
                                       { "id" => 2, "words" => %w[a c] },
                                     ])
      model = SparkConnect::ML::Feature::CountVectorizer.new(input_col: "words", output_col: "counts").fit(df)
      expect(model.vocabulary).to eq(%w[a b c])
      expect(vector_values(model.transform(df), "counts")).to eq([[2.0, 1.0, 0.0], [1.0, 1.0, 0.0], [1.0, 0.0, 1.0]])
    end

    it "Word2Vec learns one vector per distinct word" do
      df = session.create_data_frame([
                                       { "id" => 0, "words" => %w[a b c] },
                                       { "id" => 1, "words" => %w[a b d] },
                                       { "id" => 2, "words" => %w[b c d] },
                                       { "id" => 3, "words" => %w[a c d] },
                                     ])
      model = SparkConnect::ML::Feature::Word2Vec.new(input_col: "words", output_col: "vec", vector_size: 3, min_count: 1, seed: 42)
                                                 .fit(df)
      expect(model.get_vectors.count).to eq(4)
      vectors = vector_values(model.transform(df), "vec")
      expect(vectors.size).to eq(4)
      expect(vectors.map(&:size).uniq).to eq([3])
    end

    it "IDF weights terms by log((numDocs + 1) / (docFreq + 1))" do
      df = vectors_df([[1.0, 0.0], [1.0, 1.0], [0.0, 1.0]])
      model = SparkConnect::ML::Feature::IDF.new(input_col: "features", output_col: "tfidf").fit(df)
      expect(model.doc_freq).to eq([2, 2])
      expect(model.num_docs).to eq(3)
      idf = Math.log(4.0 / 3.0)
      expect_close(model.idf.to_a, [idf, idf])
      expect_close(vector_values(model.transform(df), "tfidf").first, [idf, 0.0])
    end

    it "PCA keeps the top components and reports their explained variance" do
      df = vectors_df([[2.0, 0.0], [-2.0, 0.0], [0.0, 1.0], [0.0, -1.0]])
      model = SparkConnect::ML::Feature::PCA.new(input_col: "features", output_col: "pca", k: 1).fit(df)
      # variance along x = 8 / 3, along y = 2 / 3, so the first component explains 0.8
      expect_close(model.explained_variance.to_a, [0.8])
      projected = vector_values(model.transform(df), "pca").flatten
      expect_close(projected.map(&:abs), [2.0, 2.0, 0.0, 0.0])
    end
  end

  describe "LSH" do
    let(:points) do
      vectors_df([[0.0, 0.0], [1.0, 0.0], [0.0, 1.0], [5.0, 5.0], [6.0, 5.0]])
    end

    it "BucketedRandomProjectionLSH finds the nearest neighbour and self-joins with distance 0" do
      model = SparkConnect::ML::Feature::BucketedRandomProjectionLSH.new(input_col: "features", output_col: "hashes",
                                                                         bucket_length: 2.0, num_hash_tables: 2, seed: 1)
                                                                    .fit(points)
      expect(model.transform(points).count).to eq(5)

      nearest = model.approx_nearest_neighbors(points, vectors.dense([0.0, 0.0]), 2, "dist").collect
      expect(nearest.size).to be_between(1, 2)
      expect(nearest.map { |r| r["dist"] }.min).to eq(0.0)

      joined = model.approx_similarity_join(points, points, 1.5, "dist").collect
      expect(joined.size).to be >= 5
      expect(joined.map { |r| r["dist"] }.min).to eq(0.0)
      expect(joined.map { |r| r["dist"] }.max).to be <= 1.5
    end

    it "MinHashLSH returns the exact set as its nearest neighbour and joins identical sets" do
      df = vectors_df([[1.0, 1.0, 0.0, 0.0], [1.0, 0.0, 1.0, 0.0], [0.0, 0.0, 1.0, 1.0], [0.0, 0.0, 0.0, 1.0]])
      model = SparkConnect::ML::Feature::MinHashLSH.new(input_col: "features", output_col: "hashes", num_hash_tables: 3, seed: 1)
                                                   .fit(df)
      expect(model.transform(df).count).to eq(4)

      nearest = model.approx_nearest_neighbors(df, vectors.dense([1.0, 1.0, 0.0, 0.0]), 1, "dist").collect
      expect(nearest.size).to eq(1)
      expect(nearest.first["dist"]).to eq(0.0)

      joined = model.approx_similarity_join(df, df, 0.6, "dist").collect
      expect(joined.size).to be >= 4
      expect(joined.map { |r| r["dist"] }.max).to be <= 0.6
    end
  end
end
