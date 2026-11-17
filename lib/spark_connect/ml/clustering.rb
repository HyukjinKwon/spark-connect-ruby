# frozen_string_literal: true

module SparkConnect
  module ML
    # Clustering operators (`org.apache.spark.ml.clustering`).
    #
    # Defaults come from the Scala `setDefault` calls and `Has*` traits of Spark 4.3.0.
    # The `seed` defaults are `this.getClass.getName.hashCode` of the Scala class.
    module Clustering
      # Summary shared by every clustering model (`ClusteringSummary`).
      class ClusteringSummary < Summary
        attribute :predictions, :dataframe
        attribute :prediction_col, :string, spark_name: "predictionCol"
        attribute :features_col, :string, spark_name: "featuresCol"
        attribute :k, :int
        attribute :num_iter, :int, spark_name: "numIter"
        attribute :cluster, :dataframe
        attribute :cluster_sizes, :any, spark_name: "clusterSizes"
      end

      # Training summary of a {KMeansModel}.
      class KMeansSummary < ClusteringSummary
        attribute :training_cost, :double, spark_name: "trainingCost"
      end

      # Training summary of a {BisectingKMeansModel}.
      class BisectingKMeansSummary < ClusteringSummary
        attribute :training_cost, :double, spark_name: "trainingCost"
      end

      # Training summary of a {GaussianMixtureModel}.
      class GaussianMixtureSummary < ClusteringSummary
        attribute :probability, :dataframe
        attribute :probability_col, :string, spark_name: "probabilityCol"
        attribute :log_likelihood, :double, spark_name: "logLikelihood"
      end

      # K-means clustering.
      class KMeans < Estimator
        operator "org.apache.spark.ml.clustering.KMeans"
        model -> { KMeansModel }

        param :k, :int, default: 2, doc: "the number of clusters to create (> 1)"
        param :init_mode, :string, default: "k-means||", doc: "initialization algorithm: random or k-means||"
        param :init_steps, :int, default: 2, doc: "number of steps for k-means|| initialization (> 0)"
        param :max_iter, :int, default: 20, doc: "maximum number of iterations (>= 0)"
        param :tol, :double, default: 1e-4, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :seed, :long, default: SEED_DEFAULT, doc: "random seed"
        param :distance_measure, :string, default: "euclidean", doc: "distance measure: euclidean or cosine"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :solver, :string, default: "auto", doc: "the solver algorithm for optimization"
        param :max_block_size_in_mb, :double, default: 0.0, spark_name: "maxBlockSizeInMB",
                                              doc: "maximum memory in MB for stacking input data into blocks"
        param :intermediate_storage_level, :string, default: "MEMORY_AND_DISK",
                                                    doc: "StorageLevel for intermediate datasets"
        param :features_col, :string, default: "features", doc: "features column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
      end

      # A fitted {KMeans}: the cluster centers and the nearest-center prediction.
      class KMeansModel < Model
        operator "org.apache.spark.ml.clustering.KMeansModel"
        summary_class -> { KMeansSummary }

        param :k, :int, default: 2, doc: "the number of clusters to create (> 1)"
        param :init_mode, :string, default: "k-means||", doc: "initialization algorithm: random or k-means||"
        param :init_steps, :int, default: 2, doc: "number of steps for k-means|| initialization (> 0)"
        param :max_iter, :int, default: 20, doc: "maximum number of iterations (>= 0)"
        param :tol, :double, default: 1e-4, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :seed, :long, default: SEED_DEFAULT, doc: "random seed"
        param :distance_measure, :string, default: "euclidean", doc: "distance measure: euclidean or cosine"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :solver, :string, default: "auto", doc: "the solver algorithm for optimization"
        param :max_block_size_in_mb, :double, default: 0.0, spark_name: "maxBlockSizeInMB",
                                              doc: "maximum memory in MB for stacking input data into blocks"
        param :intermediate_storage_level, :string, default: "MEMORY_AND_DISK",
                                                    doc: "StorageLevel for intermediate datasets"
        param :features_col, :string, default: "features", doc: "features column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"

        attribute :cluster_center_matrix, :matrix, spark_name: "clusterCenterMatrix"
        attribute :num_features, :int, spark_name: "numFeatures"
        method_call :predict, arg_count: 1, returns: :int
      end

      # Bisecting (divisive hierarchical) k-means clustering.
      class BisectingKMeans < Estimator
        operator "org.apache.spark.ml.clustering.BisectingKMeans"
        model -> { BisectingKMeansModel }

        param :k, :int, default: 4, doc: "the desired number of leaf clusters (> 1)"
        param :min_divisible_cluster_size, :double, default: 1.0,
                                                    doc: "minimum divisible cluster size (> 0)"
        param :max_iter, :int, default: 20, doc: "maximum number of iterations (>= 0)"
        param :seed, :long, default: SEED_DEFAULT, doc: "random seed"
        param :distance_measure, :string, default: "euclidean", doc: "distance measure: euclidean or cosine"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :features_col, :string, default: "features", doc: "features column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
      end

      # A fitted {BisectingKMeans}.
      class BisectingKMeansModel < Model
        operator "org.apache.spark.ml.clustering.BisectingKMeansModel"
        summary_class -> { BisectingKMeansSummary }

        param :k, :int, default: 4, doc: "the desired number of leaf clusters (> 1)"
        param :min_divisible_cluster_size, :double, default: 1.0,
                                                    doc: "minimum divisible cluster size (> 0)"
        param :max_iter, :int, default: 20, doc: "maximum number of iterations (>= 0)"
        param :seed, :long, default: SEED_DEFAULT, doc: "random seed"
        param :distance_measure, :string, default: "euclidean", doc: "distance measure: euclidean or cosine"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :features_col, :string, default: "features", doc: "features column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"

        attribute :cluster_center_matrix, :matrix, spark_name: "clusterCenterMatrix"
        attribute :num_features, :int, spark_name: "numFeatures"
        method_call :predict, arg_count: 1, returns: :int
        method_call :compute_cost, arg_count: 1, returns: :double, spark_name: "computeCost"
      end

      # Gaussian mixture model fitted by expectation-maximization.
      class GaussianMixture < Estimator
        operator "org.apache.spark.ml.clustering.GaussianMixture"
        model -> { GaussianMixtureModel }

        param :k, :int, default: 2, doc: "number of independent Gaussians in the mixture model"
        param :max_iter, :int, default: 100, doc: "maximum number of iterations (>= 0)"
        param :tol, :double, default: 0.01, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :seed, :long, default: SEED_DEFAULT, doc: "random seed"
        param :aggregation_depth, :int, default: 2, doc: "suggested depth for treeAggregate (>= 2)"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :features_col, :string, default: "features", doc: "features column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
        param :probability_col, :string, default: "probability", doc: "predicted class conditional probabilities column"
      end

      # A fitted {GaussianMixture}.
      class GaussianMixtureModel < Model
        operator "org.apache.spark.ml.clustering.GaussianMixtureModel"
        summary_class -> { GaussianMixtureSummary }

        param :k, :int, default: 2, doc: "number of independent Gaussians in the mixture model"
        param :max_iter, :int, default: 100, doc: "maximum number of iterations (>= 0)"
        param :tol, :double, default: 0.01, doc: "convergence tolerance for iterative algorithms (>= 0)"
        param :seed, :long, default: SEED_DEFAULT, doc: "random seed"
        param :aggregation_depth, :int, default: 2, doc: "suggested depth for treeAggregate (>= 2)"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :features_col, :string, default: "features", doc: "features column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
        param :probability_col, :string, default: "probability", doc: "predicted class conditional probabilities column"

        attribute :weights, :double_array
        attribute :num_features, :int, spark_name: "numFeatures"
        attribute :gaussians_df, :dataframe, spark_name: "gaussiansDF"
        method_call :predict, arg_count: 1, returns: :int
        method_call :predict_probability, arg_count: 1, returns: :vector, spark_name: "predictProbability"
      end

      # Latent Dirichlet allocation. The fitted class depends on `optimizer`: see {LDA#model_class}.
      class LDA < Estimator
        operator "org.apache.spark.ml.clustering.LDA"
        model -> { LocalLDAModel }

        param :k, :int, default: 10, doc: "the number of topics (clusters) to infer (> 1)"
        param :max_iter, :int, default: 20, doc: "maximum number of iterations (>= 0)"
        param :seed, :long, default: SEED_DEFAULT, doc: "random seed"
        param :checkpoint_interval, :int, default: 10,
                                          doc: "checkpoint interval (>= 1), or -1 to disable checkpointing"
        param :doc_concentration, :double_array, doc: "concentration parameter for the prior over documents"
        param :topic_concentration, :double, doc: "concentration parameter for the prior over topics"
        param :optimizer, :string, default: "online", doc: "optimizer: online or em"
        param :learning_offset, :double, default: 1024.0, doc: "online optimizer: learning offset (> 0)"
        param :learning_decay, :double, default: 0.51, doc: "online optimizer: learning decay in (0.5, 1.0]"
        param :subsampling_rate, :double, default: 0.05, doc: "online optimizer: mini-batch fraction in (0, 1]"
        param :optimize_doc_concentration, :boolean, default: true,
                                                     doc: "online optimizer: whether to optimize doc concentration"
        param :keep_last_checkpoint, :boolean, default: true, doc: "whether to keep the last checkpoint"
        param :topic_distribution_col, :string, default: "topicDistribution",
                                                doc: "output column with the topic distribution of each document"
        param :features_col, :string, default: "features", doc: "features column name"

        # The fitted model: {DistributedLDAModel} for the `em` optimizer, {LocalLDAModel} otherwise.
        # @return [Class<LDAModel>]
        def model_class
          optimizer == "em" ? DistributedLDAModel : LocalLDAModel
        end
      end

      # Ruby base of the LDA models (Spark's abstract `LDAModel`). Its members apply to both subclasses.
      class LDAModel < Model
        param :k, :int, default: 10, doc: "the number of topics (clusters) to infer (> 1)"
        param :max_iter, :int, default: 20, doc: "maximum number of iterations (>= 0)"
        param :seed, :long, default: SEED_DEFAULT, doc: "random seed (the default is set by each concrete model class)"
        param :checkpoint_interval, :int, default: 10,
                                          doc: "checkpoint interval (>= 1), or -1 to disable checkpointing"
        param :doc_concentration, :double_array, doc: "concentration parameter for the prior over documents"
        param :topic_concentration, :double, doc: "concentration parameter for the prior over topics"
        param :optimizer, :string, default: "online", doc: "optimizer: online or em"
        param :learning_offset, :double, default: 1024.0, doc: "online optimizer: learning offset (> 0)"
        param :learning_decay, :double, default: 0.51, doc: "online optimizer: learning decay in (0.5, 1.0]"
        param :subsampling_rate, :double, default: 0.05, doc: "online optimizer: mini-batch fraction in (0, 1]"
        param :optimize_doc_concentration, :boolean, default: true,
                                                     doc: "online optimizer: whether to optimize doc concentration"
        param :keep_last_checkpoint, :boolean, default: true, doc: "whether to keep the last checkpoint"
        param :topic_distribution_col, :string, default: "topicDistribution",
                                                doc: "output column with the topic distribution of each document"
        param :features_col, :string, default: "features", doc: "features column name"

        attribute :estimated_doc_concentration, :vector, spark_name: "estimatedDocConcentration"
        attribute :topics_matrix, :matrix, spark_name: "topicsMatrix"
        attribute :is_distributed, :boolean, spark_name: "isDistributed"
        attribute :vocab_size, :int, spark_name: "vocabSize"
        method_call :log_likelihood, arg_count: 1, returns: :double, spark_name: "logLikelihood"
        method_call :log_perplexity, arg_count: 1, returns: :double, spark_name: "logPerplexity"

        # The `topics` of the model as a DataFrame: `describeTopics(maxTermsPerTopic)`
        # for a limit, or Spark's no-argument overload (ten terms per topic) without one.
        #
        # @param max_terms_per_topic [Integer, nil]
        # @return [DataFrame]
        def describe_topics(max_terms_per_topic = nil)
          args = max_terms_per_topic.nil? ? [] : [max_terms_per_topic]
          AttributeSupport.invoke(self, [AttributeSupport.method_proto("describeTopics", args)], :dataframe)
        end
      end

      # An LDA model fitted with the `em` optimizer (the server keeps the distributed model).
      class DistributedLDAModel < LDAModel
        operator "org.apache.spark.ml.clustering.DistributedLDAModel"

        param :seed, :long, default: SEED_DEFAULT, doc: "random seed"

        attribute :training_log_likelihood, :double, spark_name: "trainingLogLikelihood"
        attribute :log_prior, :double, spark_name: "logPrior"
        attribute :checkpoint_files, :string_array, spark_name: "getCheckpointFiles"

        # The same topics as a local model (`toLocal`), with this model's params.
        #
        # @return [LocalLDAModel]
        def to_local
          info = Rpc.fetch(session, ref_id, [AttributeSupport.method_proto("toLocal", [])])
          local = LocalLDAModel.new(uid: uid).copy_values_from(self)
          local.attach(session, info[:obj_ref])
          local
        end
      end

      # An LDA model fitted with the `online` optimizer (the default).
      class LocalLDAModel < LDAModel
        operator "org.apache.spark.ml.clustering.LocalLDAModel"

        param :seed, :long, default: SEED_DEFAULT, doc: "random seed"
      end

      # Power iteration clustering over an affinity graph. Spark's `PowerIterationClusteringWrapper`
      # has no transform: use {#assign_clusters}.
      class PowerIterationClustering < Transformer
        operator "org.apache.spark.ml.clustering.PowerIterationClusteringWrapper"

        param :k, :int, default: 2, doc: "the number of clusters to create (> 1)"
        param :max_iter, :int, default: 20, doc: "maximum number of iterations (>= 0)"
        param :init_mode, :string, default: "random", doc: "initialization algorithm: random or degree"
        param :src_col, :string, default: "src", doc: "name of the input column for source vertex ids"
        param :dst_col, :string, default: "dst", doc: "name of the input column for destination vertex ids"
        param :weight_col, :string, doc: "weight column name; unset means every edge has weight 1.0"

        # Cluster the vertices of an affinity graph given as `src`, `dst` and weight columns.
        #
        # @param dataset [DataFrame]
        # @return [DataFrame] one row per vertex with columns `id` (long) and `cluster` (int)
        def assign_clusters(dataset)
          weight = is_defined?(:weight_col) ? weight_col : ""
          args = [dataset, k, max_iter, init_mode, src_col, dst_col, weight]
          helper_frame(dataset, "powerIterationClusteringAssignClusters", args)
        end

        # @raise [NotImplementedError] always; use {#assign_clusters}.
        def transform(*)
          raise SparkConnect::NotImplementedError, "PowerIterationClustering has no transform; use assign_clusters"
        end

        private

        # A DataFrame produced by a method of the ML connect helper (no model reference).
        def helper_frame(dataset, method_name, args)
          session = dataset.session
          methods = [AttributeSupport.method_proto(method_name, args)]
          fetch = Proto::Fetch.new(obj_ref: Proto::ObjectRef.new(id: Rpc::HELPER_ID), methods: methods)
          DataFrame.new(session, PlanBuilder.relation(session, ml_relation: Proto::MlRelation.new(fetch: fetch)))
        end
      end
    end
  end
end
