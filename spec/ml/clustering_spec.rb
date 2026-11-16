# frozen_string_literal: true

require_relative "support"

RSpec.describe SparkConnect::ML::Clustering do
  include MlSpecSupport

  let(:client) { MlSpecSupport::FakeMlClient.new }
  let(:session) { ml_session(client) }
  let(:frame) { ml_frame(session) }
  let(:clustering) { described_class }

  def concrete_operators
    [clustering::KMeans, clustering::KMeansModel, clustering::BisectingKMeans, clustering::BisectingKMeansModel,
     clustering::GaussianMixture, clustering::GaussianMixtureModel, clustering::LDA, clustering::DistributedLDAModel,
     clustering::LocalLDAModel, clustering::PowerIterationClustering,]
  end

  describe "operator names" do
    it "names the Scala class of each operator" do
      expect(clustering::KMeans.operator).to eq("org.apache.spark.ml.clustering.KMeans")
      expect(clustering::KMeansModel.operator).to eq("org.apache.spark.ml.clustering.KMeansModel")
      expect(clustering::BisectingKMeans.operator).to eq("org.apache.spark.ml.clustering.BisectingKMeans")
      expect(clustering::DistributedLDAModel.operator).to eq("org.apache.spark.ml.clustering.DistributedLDAModel")
    end

    it "declares a clustering operator on every concrete class, resolvable by constant" do
      concrete_operators.each do |klass|
        expect(klass.operator).to start_with("org.apache.spark.ml.clustering.")
        expect(clustering.const_get(klass.name.split("::").last)).to eq(klass)
      end
    end
  end

  describe "defaults" do
    it "matches the Spark 4.3.0 setDefault values" do
      expect(clustering::KMeans.new.k).to eq(2)
      expect(clustering::KMeans.new.init_mode).to eq("k-means||")
      expect(clustering::KMeans.new.tol).to eq(1e-4)
      expect(clustering::BisectingKMeans.new.k).to eq(4)
      expect(clustering::BisectingKMeans.new.min_divisible_cluster_size).to eq(1.0)
      expect(clustering::GaussianMixture.new.max_iter).to eq(100)
      expect(clustering::LDA.new.optimizer).to eq("online")
      expect(clustering::LDA.new.learning_offset).to eq(1024.0)
    end

    it "has no default for a param Spark leaves unset" do
      expect(clustering::KMeans.new.is_defined?(:weight_col)).to be(false)
      expect { clustering::KMeans.new.weight_col }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "uses the Spark name for params whose camelCase differs" do
      expect(clustering::KMeans.params.fetch(:max_block_size_in_mb).spark_name).to eq("maxBlockSizeInMB")
      expect(clustering::GaussianMixture.params.fetch(:probability_col).spark_name).to eq("probabilityCol")
    end
  end

  describe "set chain" do
    it "sets params and returns the operator" do
      kmeans = clustering::KMeans.new
      expect(kmeans.set_k(3).set_max_iter(5)).to be(kmeans)
      expect(kmeans.k).to eq(3)
      expect(kmeans.max_iter).to eq(5)
    end
  end

  describe "fit" do
    it "sends an MlCommand.fit with the explicit params keyed by Spark name" do
      clustering::KMeans.new(k: 3).set_max_iter(5).fit(frame)

      fit = first_ml(client, :fit).fit
      expect(fit.estimator.name).to eq("org.apache.spark.ml.clustering.KMeans")
      expect(fit.estimator.type).to eq(:OPERATOR_TYPE_ESTIMATOR)
      expect(fit.params.params.keys).to contain_exactly("k", "maxIter")
    end

    it "returns a KMeansModel with its training summary" do
      model = clustering::KMeans.new.fit(frame)

      expect(model).to be_a(clustering::KMeansModel)
      expect(model.summary).to be_a(clustering::KMeansSummary)
    end

    it "fits GaussianMixture into a GaussianMixtureModel" do
      model = clustering::GaussianMixture.new(k: 2).fit(frame)

      expect(model).to be_a(clustering::GaussianMixtureModel)
      expect(model.summary).to be_a(clustering::GaussianMixtureSummary)
    end

    it "picks the LDA model class from the optimizer" do
      expect(clustering::LDA.new.model_class).to eq(clustering::LocalLDAModel)
      expect(clustering::LDA.new(optimizer: "em").model_class).to eq(clustering::DistributedLDAModel)
    end
  end

  describe "model attributes and methods" do
    let(:model) { clustering::KMeans.new.fit(frame) }

    it "sends a Fetch with the Spark method name for an attribute" do
      model.num_features

      fetch = client.last_ml_command.fetch
      expect(fetch["methods"].map { |m| m["method"] }).to eq(["numFeatures"])
      expect(fetch.obj_ref.id).to eq("model-1")
    end

    it "sends the summary chain for a summary attribute" do
      model.summary.training_cost

      expect(client.last_ml_command.fetch["methods"].map { |m| m["method"] }).to eq(%w[summary trainingCost])
    end
  end

  describe "transform" do
    it "builds an MlRelation.transform on the fitted model" do
      model = clustering::KMeans.new.fit(frame)

      transform = model.transform(frame).relation.ml_relation.transform
      expect(transform.obj_ref.id).to eq("model-1")
    end
  end

  describe "PowerIterationClustering" do
    subject(:pic) { clustering::PowerIterationClustering.new(k: 3) }

    let(:edges) { ml_frame(session, 2) }

    it "is a Transformer over the PowerIterationClusteringWrapper operator" do
      expect(clustering::PowerIterationClustering.operator).to eq("org.apache.spark.ml.clustering.PowerIterationClusteringWrapper")
      expect(clustering::PowerIterationClustering.ancestors).to include(SparkConnect::ML::Transformer)
    end

    it "rejects transform and points at assign_clusters" do
      expect { pic.transform(edges) }.to raise_error(SparkConnect::NotImplementedError, /assign_clusters/)
    end

    it "builds a helper fetch for assign_clusters with the params as arguments" do
      df = pic.assign_clusters(edges)

      fetch = df.relation.ml_relation.fetch
      expect(fetch.obj_ref.id).to eq(SparkConnect::ML::Rpc::HELPER_ID)
      call = fetch["methods"].first
      expect(call["method"]).to eq("powerIterationClusteringAssignClusters")
      expect(call.args.size).to eq(7)
      expect(call.args[0].input).not_to be_nil
      expect(call.args[1].param.integer).to eq(3)
      expect(call.args[3].param.string).to eq("random")
      expect(call.args[4].param.string).to eq("src")
      expect(call.args[6].param.string).to eq("")
    end

    it "passes the weight column when it is set" do
      pic.set_weight_col("w")
      call = pic.assign_clusters(edges).relation.ml_relation.fetch["methods"].first

      expect(call.args[6].param.string).to eq("w")
    end
  end
end
