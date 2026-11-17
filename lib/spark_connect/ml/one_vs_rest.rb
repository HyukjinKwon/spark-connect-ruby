# frozen_string_literal: true

require "securerandom"

module SparkConnect
  module ML
    # Multiclass classification by one binary classifier per class, like
    # `pyspark.ml.classification.OneVsRest`. Spark's server does not allowlist
    # this meta-algorithm, so, as in PySpark, it runs on the client: one binary
    # fit per class (on a relabelled copy of the data), and a transform that
    # scores every binary model and picks the class with the highest score.
    module Classification
      # One-vs-rest reduction around a binary `classifier`.
      class OneVsRest < Estimator
        operator "org.apache.spark.ml.classification.OneVsRest"
        model -> { OneVsRestModel }

        param :classifier, :any, doc: "base binary classifier"
        param :label_col, :string, default: "label", doc: "label column name"
        param :features_col, :string, default: "features", doc: "features column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
        param :weight_col, :string, doc: "weight column name; unset means every instance has weight 1.0"
        param :parallelism, :int, default: 1, doc: "number of threads to use when running parallel algorithms (>= 1)"

        # Fit one binary classifier per class, `max(label) + 1` of them.
        #
        # @param dataset [DataFrame]
        # @param params [Hash, nil]
        # @return [OneVsRestModel]
        def fit(dataset, params = nil)
          return copy(params).fit(dataset) if params && !params.empty?

          label = get(:label_col)
          features = get(:features_col)
          prediction = get(:prediction_col)
          base = get(:classifier)
          weight = is_set?(:weight_col) && base.class.params.key?(:weight_col) ? get(:weight_col) : nil
          num_classes = dataset.select(F.max(F.col(label)).alias("max_label")).collect.first["max_label"].to_i + 1

          columns = [label, features, weight].compact
          multiclass = dataset.select(*columns.map { |c| F.col(c) })
          multiclass.cache
          begin
            models = Tuning::ValidatorSupport.run_parallel(Array(0...num_classes), get(:parallelism)) do |index|
              binary_label = "mc2b$#{index}"
              training = multiclass.with_column(binary_label,
                                                F.when(F.col(label) == F.lit(index.to_f), 1.0).otherwise(0.0))
              pmap = { base.param(:label_col) => binary_label, base.param(:features_col) => features,
                       base.param(:prediction_col) => prediction, }
              pmap[base.param(:weight_col)] = weight if weight
              base.fit(training, pmap)
            end
          ensure
            multiclass.unpersist
          end
          OneVsRestModel.new(uid: uid, models: models).copy_values_from(self)
        end

        # @api private
        def save_meta(path, overwrite:, session:)
          Persistence.handle_overwrite(session, path, overwrite)
          Persistence.write_metadata(session, path, self,
                                     param_map: Persistence.extract_json_params(self, skip: ["classifier"]))
          get(:classifier).save(File.join(path, "classifier"), overwrite: false, session: session)
          nil
        end

        # @api private
        def self.load_meta(path, session:)
          meta = Persistence.read_metadata(session, path)
          classifier = Persistence.load_any(session, File.join(path, "classifier"))
          ovr = new(uid: meta.fetch("uid"), classifier: classifier)
          Persistence.apply_params(ovr, meta.fetch("paramMap").except("classifier"))
          ovr
        end
      end

      # The fitted {OneVsRest}: one binary model per class. A row is labelled with
      # the class whose model scores it highest.
      class OneVsRestModel < Model
        operator "org.apache.spark.ml.classification.OneVsRestModel"

        param :classifier, :any, doc: "base binary classifier"
        param :label_col, :string, default: "label", doc: "label column name"
        param :features_col, :string, default: "features", doc: "features column name"
        param :prediction_col, :string, default: "prediction", doc: "prediction column name"
        param :raw_prediction_col, :string, default: "rawPrediction", doc: "raw prediction (confidence) column name"
        param :weight_col, :string, doc: "weight column name"

        # @return [Array<Model>] the binary models, one per class.
        attr_reader :models

        # @api private
        def initialize(uid: nil, models: [], **values)
          super(uid: uid, **values)
          @models = models
        end

        # Score every class and label each row with the best one: the
        # `rawPrediction` vector holds the per-class scores, and `prediction` is
        # the index of the highest score. As PySpark does, the scores accumulate
        # in an array column that is turned into the vector at the end.
        #
        # @param dataset [DataFrame]
        # @param params [Hash, nil]
        # @return [DataFrame]
        def transform(dataset, params = nil)
          return copy(params).transform(dataset) if params && !params.empty?

          original = dataset.columns
          acc = "mbc$acc#{SecureRandom.hex(8)}"
          raw = get(:raw_prediction_col)
          aggregated = dataset.with_column(acc, F.array.cast("array<double>"))
          models.each do |model|
            tmp = "mbc$tmp#{SecureRandom.hex(8)}"
            scored = model.transform(aggregated).select(*(original + [raw, acc]))
            appended = scored.with_column(tmp, F.call_function("array_append", F.col(acc),
                                                               Functions.vector_to_array(F.col(raw)).get_item(1)))
            aggregated = appended.select(*(original + [tmp])).with_column_renamed(tmp, acc)
          end
          aggregated = aggregated.with_column(raw, Functions.array_to_vector(F.col(acc)))
          # The index of the highest score: the first position of the maximum (0-based).
          best = F.call_function("array_position", F.col(acc), F.call_function("array_max", F.col(acc))) - 1
          aggregated = aggregated.with_column(get(:prediction_col), best.cast("double"))
          aggregated.drop(acc)
        end

        # @api private
        def save_meta(path, overwrite:, session:)
          Persistence.handle_overwrite(session, path, overwrite)
          Persistence.write_metadata(session, path, self,
                                     param_map: Persistence.extract_json_params(self, skip: ["classifier"]),
                                     extra: { "numClasses" => models.size })
          get(:classifier).save(File.join(path, "classifier"), overwrite: false, session: session)
          models.each_with_index do |model, i|
            model.save(File.join(path, "model_#{i}"), overwrite: false, session: session)
          end
          nil
        end

        # @api private
        def self.load_meta(path, session:)
          meta = Persistence.read_metadata(session, path)
          classifier = Persistence.load_any(session, File.join(path, "classifier"))
          models = Array.new(meta.fetch("numClasses")) { |i| Persistence.load_any(session, File.join(path, "model_#{i}")) }
          ovr = new(uid: meta.fetch("uid"), classifier: classifier, models: models)
          Persistence.apply_params(ovr, meta.fetch("paramMap").except("classifier"))
          ovr
        end
      end
    end

    # The class is also available at the ML level, as in PySpark's `pyspark.ml`.
    OneVsRest = Classification::OneVsRest
    OneVsRestModel = Classification::OneVsRestModel
  end
end
