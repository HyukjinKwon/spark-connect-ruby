# frozen_string_literal: true

# Integration specs for the clustering operators, against a live Spark Connect server.
# Two far-apart blobs of three points each: the expected clusters are known, and the
# optimal k=2 cost is 8/3 (each blob's points sit 1/3 and 2/3 from its centroid).
RSpec.describe SparkConnect::ML::Clustering, if: ENV.fetch("SPARK_REMOTE", nil) do
  before(:all) do
    skip "set SPARK_REMOTE to run integration specs" unless ENV["SPARK_REMOTE"]
    @session = SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE", nil)).create
  end

  after(:all) do
    @session&.stop
  end

  let(:session) { @session }
  let(:clustering) { described_class }
  let(:vectors) { SparkConnect::ML::Linalg::Vectors }
  let(:points) do
    session.sql("SELECT * FROM VALUES (0, array(0.0D, 0.0D)), (1, array(0.0D, 1.0D)), (2, array(1.0D, 0.0D)), " \
                "(3, array(10.0D, 10.0D)), (4, array(10.0D, 11.0D)), (5, array(11.0D, 10.0D)) AS t(id, features)")
  end

  # The cluster of each point id, from a transform output.
  def assignments(df)
    df.collect.to_h { |r| [r["id"], r["prediction"]] }
  end

  # Checks that ids 0-2 share one cluster, ids 3-5 another, and the two differ.
  def expect_two_blobs(assigned)
    expect(assigned[0]).to eq(assigned[1]).and eq(assigned[2])
    expect(assigned[3]).to eq(assigned[4]).and eq(assigned[5])
    expect(assigned[0]).not_to eq(assigned[3])
  end

  describe "KMeans" do
    let(:model) { clustering::KMeans.new(k: 2, seed: 1).fit(points) }

    it "separates the two blobs and reports the optimal training cost" do
      expect(model).to be_a(clustering::KMeansModel)
      expect_two_blobs(assignments(model.transform(points)))
      expect(model.summary.training_cost).to be_within(1e-6).of(8.0 / 3)
    end

    it "reports the model shape, the summary sizes and the cluster centers" do
      expect(model.num_features).to eq(2)
      expect(model.cluster_center_matrix.num_rows).to eq(2)
      expect(model.summary.k).to eq(2)
      expect(model.summary.cluster_sizes.sort).to eq([3, 3])
      expect(model.summary.cluster.collect.size).to eq(6)
    end

    it "predicts the cluster of a new point near the first blob" do
      first = assignments(model.transform(points))[0]

      expect(model.predict(vectors.dense([0.0, 0.5]))).to eq(first)
    end
  end

  describe "BisectingKMeans" do
    let(:model) { clustering::BisectingKMeans.new(k: 2, seed: 1).fit(points) }

    it "separates the two blobs and computes the cost" do
      expect(model).to be_a(clustering::BisectingKMeansModel)
      expect_two_blobs(assignments(model.transform(points)))
      expect(model.compute_cost(points)).to be_within(1e-6).of(8.0 / 3)
      expect(model.summary.training_cost).to be_within(1e-6).of(8.0 / 3)
    end
  end

  describe "GaussianMixture" do
    let(:model) { clustering::GaussianMixture.new(k: 2, seed: 1).fit(points) }

    it "splits the blobs into two equally weighted Gaussians" do
      expect(model).to be_a(clustering::GaussianMixtureModel)
      expect_two_blobs(assignments(model.transform(points)))
      expect(model.weights.sort.map { |w| w.round(3) }).to eq([0.5, 0.5])
      expect(model.num_features).to eq(2)
      expect(model.gaussians_df.collect.size).to eq(2)
    end

    it "reports the summary probabilities and the log-likelihood" do
      expect(model.summary.log_likelihood).to be_a(Float)
      expect(model.summary.probability.collect.size).to eq(6)
      expect(model.summary.probability_col).to eq("probability")
    end
  end

  describe "LDA" do
    # Documents 0-1 use terms 0-1, documents 2-3 use terms 2-3.
    let(:docs) do
      session.sql("SELECT * FROM VALUES (0, array(3.0D, 2.0D, 0.0D, 0.0D)), (1, array(2.0D, 3.0D, 0.0D, 0.0D)), " \
                  "(2, array(0.0D, 0.0D, 4.0D, 3.0D)), (3, array(0.0D, 0.0D, 3.0D, 4.0D)) AS t(id, features)")
    end

    # The index of the largest topic weight of each document, by id.
    def dominant_topics(df)
      df.collect.to_h do |r|
        weights = r["topicDistribution"].to_a
        [r["id"], weights.index(weights.max)]
      end
    end

    it "fits the default online optimizer into a LocalLDAModel that separates the documents" do
      model = clustering::LDA.new(k: 2, max_iter: 30, seed: 1, subsampling_rate: 1.0).fit(docs)

      expect(model).to be_a(clustering::LocalLDAModel)
      expect(model.is_distributed).to be(false)
      expect(model.vocab_size).to eq(4)
      expect(model.topics_matrix.num_rows).to eq(4)
      expect(model.topics_matrix.num_cols).to eq(2)
      expect(model.describe_topics(2).collect.size).to eq(2)
      expect(model.estimated_doc_concentration.to_a.size).to eq(2)

      topics = dominant_topics(model.transform(docs))
      expect(topics[0]).to eq(topics[1])
      expect(topics[2]).to eq(topics[3])
      expect(topics[0]).not_to eq(topics[2])
    end

    it "fits the em optimizer into a DistributedLDAModel with the distributed members" do
      model = clustering::LDA.new(k: 2, max_iter: 30, seed: 1, optimizer: "em", checkpoint_interval: -1).fit(docs)

      expect(model).to be_a(clustering::DistributedLDAModel)
      expect(model.is_distributed).to be(true)
      expect(model.training_log_likelihood).to be_a(Float).and be < 0
      expect(model.log_prior).to be_a(Float)
      expect(model.checkpoint_files).to be_an(Array)
    end
  end

  describe "PowerIterationClustering" do
    it "assigns the vertices of two disconnected triangles to two clusters" do
      edges = session.sql("SELECT * FROM VALUES (0L, 1L, 1.0D), (1L, 2L, 1.0D), (0L, 2L, 1.0D), " \
                          "(3L, 4L, 1.0D), (4L, 5L, 1.0D), (3L, 5L, 1.0D) AS t(src, dst, weight)")
      pic = clustering::PowerIterationClustering.new(k: 2, max_iter: 20, weight_col: "weight")

      clusters = pic.assign_clusters(edges).collect.to_h { |r| [r["id"], r["cluster"]] }
      expect(clusters.keys.sort).to eq([0, 1, 2, 3, 4, 5])
      expect_two_blobs(clusters)
    end
  end
end
