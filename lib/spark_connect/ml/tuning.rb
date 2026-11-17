# frozen_string_literal: true

module SparkConnect
  module ML
    # Model selection, like `pyspark.ml.tuning`.
    #
    # As in PySpark's Connect client, the search is orchestrated on the client:
    # for every fold (or the single split) and every param map, the estimator is
    # fit on the training rows and the evaluator scores the validation rows. Each
    # fit and evaluation is a server operation. The best param map is then fit on
    # the whole dataset. Splits are cached while their fold runs, and
    # `parallelism` fits run on that many threads.
    #
    # The split is random but seeded: the `seed` default is Spark's (the Java hash
    # of the validator's class name), so a fit is repeatable.
    module Tuning
      # Builds the grid of param maps for a validator, like `ParamGridBuilder`.
      #
      # @example
      #   grid = SparkConnect::ML::Tuning::ParamGridBuilder.new
      #                                                     .add_grid(lr.param(:reg_param), [0.1, 0.01])
      #                                                     .add_grid(lr.param(:max_iter), [5, 10])
      #                                                     .build
      class ParamGridBuilder
        def initialize
          @grid = {}
        end

        # @param param [Param] a param of an operator, e.g. `lr.param(:reg_param)`.
        # @param values [Array<Object>]
        # @return [self]
        def add_grid(param, values)
          raise IllegalArgumentError, "param must be a SparkConnect::ML::Param" unless param.is_a?(Param)

          @grid[param] = values.to_a
          self
        end

        # Fix params to one value in every map.
        # @param pairs [Hash{Param => Object}, Array<Array(Param, Object)>]
        # @return [self]
        def base_on(*pairs)
          pairs = pairs.first.to_a if pairs.first.is_a?(Hash)
          pairs.each { |param, value| add_grid(param, [value]) }
          self
        end

        # @return [Array<Hash{Param => Object}>] every combination of the grid.
        def build
          keys = @grid.keys
          combos = keys.empty? ? [[]] : keys.map { |k| @grid[k] }.reduce([[]]) { |acc, vals| acc.product(vals).map(&:flatten) }
          combos.map { |values| keys.zip(values).to_h }
        end
      end

      # The shared machinery of the two validators and their models.
      module ValidatorSupport
        module_function

        # Declare the params of a cross-validator (and of its model).
        # @api private
        def declare_cv_params(klass)
          klass.param :num_folds, :int, default: 3, doc: "number of folds for cross validation (>= 2)"
          klass.param :fold_col, :string, default: "",
                                          doc: "column with user-specified fold numbers in [0, numFolds); empty for random folds"
          klass.param :seed, :long, default: ML::SEED_DEFAULT, doc: "random seed for the random fold split"
          klass.param :parallelism, :int, default: 1, doc: "number of threads to use when running parallel algorithms (>= 1)"
          klass.param :collect_sub_models, :boolean, default: false, spark_name: "collectSubModels",
                                                     doc: "whether to keep the models of every fold and param map"
          klass.param :estimator, :any, doc: "estimator to be cross-validated"
          klass.param :estimator_param_maps, :any, spark_name: "estimatorParamMaps", doc: "param maps to try"
          klass.param :evaluator, :any, doc: "evaluator used to choose the best model"
        end

        # Declare the params of a train-validation split (and of its model).
        # @api private
        def declare_tvs_params(klass)
          klass.param :train_ratio, :double, default: 0.75, doc: "ratio between training set and validation set (in (0, 1))"
          klass.param :seed, :long, default: ML::SEED_DEFAULT, doc: "random seed for the split"
          klass.param :parallelism, :int, default: 1, doc: "number of threads to use when running parallel algorithms (>= 1)"
          klass.param :collect_sub_models, :boolean, default: false, spark_name: "collectSubModels",
                                                     doc: "whether to keep the model of every param map"
          klass.param :estimator, :any, doc: "estimator to be validated"
          klass.param :estimator_param_maps, :any, spark_name: "estimatorParamMaps", doc: "param maps to try"
          klass.param :evaluator, :any, doc: "evaluator used to choose the best model"
        end

        # Rows of `dataset` split into (train, validation) per fold, by a `rand`
        # column seeded by `seed`, or by the user's fold column.
        # @return [Array<Array(DataFrame, DataFrame)>]
        def folds(dataset, n_folds, owner_uid:, fold_col:, seed:)
          if fold_col.nil? || fold_col.empty?
            rand_col = "#{owner_uid}_rand"
            df = dataset.with_column(rand_col, F.rand(seed))
            h = 1.0 / n_folds
            (0...n_folds).map do |i|
              cond = (F.col(rand_col) >= i * h) & (F.col(rand_col) < (i + 1) * h)
              [df.filter(!cond), df.filter(cond)]
            end
          else
            (0...n_folds).map do |i|
              [dataset.filter(F.col(fold_col) != i), dataset.filter(F.col(fold_col) == i)]
            end
          end
        end

        # The single (train, validation) split of a train-validation split.
        # @return [Array(DataFrame, DataFrame)]
        def single_split(dataset, train_ratio, owner_uid:, seed:)
          rand_col = "#{owner_uid}_rand"
          df = dataset.with_column(rand_col, F.rand(seed))
          cond = F.col(rand_col) >= train_ratio
          [df.filter(!cond), df.filter(cond)]
        end

        # Mean and population standard deviation per param map (`np.mean`, `np.std`).
        # @return [Array(Array<Float>, Array<Float>)]
        def avg_and_std(metrics_by_fold)
          n = metrics_by_fold.size
          columns = metrics_by_fold.transpose
          avg = columns.map { |col| col.sum / n.to_f }
          std = columns.each_with_index.map { |col, j| Math.sqrt(col.sum { |m| (m - avg[j])**2 } / n.to_f) }
          [avg, std]
        end

        # Index of the best metric, by the evaluator's direction.
        # @return [Integer]
        def best_index(metrics, evaluator)
          evaluator.larger_better? ? metrics.each_with_index.max_by(&:first).last : metrics.each_with_index.min_by(&:first).last
        end

        # Map `items` through `block` on up to `parallelism` threads, keeping order.
        # @return [Array]
        def run_parallel(items, parallelism, &block)
          return items.map(&block) if parallelism <= 1 || items.size <= 1

          queue = Queue.new
          items.each_index { |i| queue << i }
          results = Array.new(items.size)
          workers = Array.new([parallelism, items.size].min) do
            Thread.new do
              loop do
                index = begin
                  queue.pop(true)
                rescue ThreadError
                  break
                end
                results[index] = block.call(items[index])
              end
            end
          end
          workers.each(&:join)
          results
        end

        # The operator stages (estimator and nested pipeline stages, and the
        # evaluator) whose params a param map may set.
        # @return [Array<Params>]
        def nested_stages(estimator, evaluator)
          nested = estimator.is_a?(Pipeline) ? estimator.stages_list.flat_map { |s| nested_stages(s, nil) } : []
          [estimator, *nested, evaluator].compact
        end

        # Save the estimator param maps: one entry per param, with the value as JSON
        # or, for an operator value, as a subdirectory (`epm_<name><n>`).
        # @return [Array<Array<Hash>>]
        def save_param_maps(path, maps, stages, session)
          numbered = 0
          maps.map do |map|
            map.map do |declared, value|
              owner = stages.find { |s| s.class.params[declared.name].equal?(declared) }
              raise IllegalArgumentError, "param #{declared.name} does not belong to the validator" unless owner

              entry = { "parent" => owner.uid, "name" => declared.spark_name }
              if value.is_a?(Estimator) || value.is_a?(Transformer) || value.is_a?(Evaluator)
                relative = "epm_#{declared.spark_name}#{numbered}"
                numbered += 1
                value.save(File.join(path, relative), overwrite: false, session: session)
                # Spark writes every field as a JSON string, the path included.
                entry.merge!("value" => JSON.generate(relative), "isJson" => "false")
              elsif value.is_a?(Params)
                raise IllegalArgumentError, "a meta-estimator cannot be a validator param value"
              else
                entry.merge!("value" => JSON.generate(Persistence.json_value(value)), "isJson" => "true")
              end
            end
          end
        end

        # @api private
        def json_flag?(flag)
          [true, "true"].include?(flag)
        end

        # Rebuild the param maps written by {save_param_maps}.
        # @return [Array<Hash{Param => Object}>]
        def load_param_maps(path, session, json, stages)
          json.map do |entries|
            entries.to_h do |entry|
              owner = stages.find { |s| s.uid == entry.fetch("parent") } or
                raise IllegalArgumentError, "unknown parent #{entry.fetch('parent')} in estimatorParamMaps"
              declared = owner.class.params.values.find { |p| p.spark_name == entry.fetch("name") }
              # `isJson` is "true"/"false" (Spark) or a boolean (PySpark); the value is a JSON
              # document (Spark, and PySpark for params) or a raw path (PySpark, for children).
              value = if json_flag?(entry.fetch("isJson"))
                        JSON.parse("[#{entry.fetch('value')}]").first
                      else
                        raw = entry.fetch("value")
                        path_part = raw.start_with?('"') ? JSON.parse("[#{raw}]").first : raw
                        Persistence.load_any(session, File.join(path, path_part))
                      end
              [declared, value]
            end
          end
        end

        # Write a validator or validator model: the metadata (with the param maps),
        # then the evaluator and the estimator.
        # @api private
        def save_validator(instance, path, overwrite:, session:, extra: {})
          Persistence.handle_overwrite(session, path, overwrite)
          estimator = instance.get(:estimator)
          evaluator = instance.get(:evaluator)
          maps = save_param_maps(path, instance.get(:estimator_param_maps), nested_stages(estimator, evaluator), session)
          param_map = Persistence.extract_json_params(instance, skip: %w[estimator evaluator estimatorParamMaps])
          param_map["estimatorParamMaps"] = maps
          # PySpark's validator models declare no parallelism or collectSubModels.
          exclude = instance.is_a?(Model) || instance.is_a?(Transformer) ? %w[parallelism collectSubModels] : []
          Persistence.write_metadata(session, path, instance, param_map: param_map, extra: extra, exclude: exclude)
          evaluator.save(File.join(path, "evaluator"), overwrite: false, session: session)
          estimator.save(File.join(path, "estimator"), overwrite: false, session: session)
          nil
        end

        # Read the estimator, evaluator and param maps written by {save_validator}.
        # @return [Array(Params, Params, Array<Hash>)]
        def load_validator(path, session, meta)
          estimator = Persistence.load_any(session, File.join(path, "estimator"))
          evaluator = Persistence.load_any(session, File.join(path, "evaluator"))
          maps = load_param_maps(path, session, meta.fetch("paramMap").fetch("estimatorParamMaps"),
                                 nested_stages(estimator, evaluator))
          [estimator, evaluator, maps]
        end

        # Run the search over `folds`: one fit and one evaluation per fold and
        # param map. Each fold's splits are cached while it runs.
        # @return [Array(Array<Array<Float>>, Array<Array<Transformer>>)] the metrics per fold, and models per fold (when collected).
        def search(folds, estimator, maps, evaluator, parallelism:, collect:)
          metrics_by_fold = []
          models_by_fold = []
          folds.each do |train, validation|
            train.cache
            validation.cache
            begin
              results = run_parallel(maps, parallelism) do |map|
                model = estimator.fit(train, map)
                [evaluator.evaluate(model.transform(validation, map)), (collect ? model : nil)]
              end
              metrics_by_fold << results.map(&:first)
              models_by_fold << results.map(&:last) if collect
            ensure
              train.unpersist
              validation.unpersist
            end
          end
          [metrics_by_fold, models_by_fold]
        end

        # Save the sub-models of a validator model under `subModels/fold<i>/<j>`.
        # @api private
        def save_sub_models(path, sub_models, session)
          sub_models.each_with_index do |fold_models, fold|
            fold_models.each_with_index do |model, j|
              model.save(File.join(path, "subModels", "fold#{fold}", j.to_s), overwrite: false, session: session)
            end
          end
          nil
        end

        # @api private
        def load_sub_models(path, session, folds, maps)
          Array.new(folds) do |fold|
            Array.new(maps) { |j| Persistence.load_any(session, File.join(path, "subModels", "fold#{fold}", j.to_s)) }
          end
        end
      end

      # K-fold cross validation over an estimator, a param grid and an evaluator.
      class CrossValidator < Estimator
        operator "org.apache.spark.ml.tuning.CrossValidator"
        model -> { CrossValidatorModel }

        ValidatorSupport.declare_cv_params(self)

        # @param uid [String, nil]
        # @param values [Hash] the params, e.g. `estimator:`, `estimator_param_maps:`, `evaluator:`.
        def initialize(uid: nil, **values)
          super
          raise IllegalArgumentError, "numFolds must be at least 2" if get(:num_folds) < 2
        end

        # Run the search and refit the best param map on `dataset`.
        #
        # @param dataset [DataFrame]
        # @param params [Hash, nil]
        # @return [CrossValidatorModel]
        def fit(dataset, params = nil)
          return copy(params).fit(dataset) if params && !params.empty?

          estimator = get(:estimator)
          maps = get(:estimator_param_maps)
          evaluator = get(:evaluator)
          folds = ValidatorSupport.folds(dataset, get(:num_folds), owner_uid: uid, fold_col: get(:fold_col), seed: get(:seed))
          metrics_by_fold, models_by_fold = ValidatorSupport.search(folds, estimator, maps, evaluator,
                                                                    parallelism: get(:parallelism), collect: get(:collect_sub_models))
          avg, std = ValidatorSupport.avg_and_std(metrics_by_fold)
          best = estimator.fit(dataset, maps[ValidatorSupport.best_index(avg, evaluator)])
          model = CrossValidatorModel.new(uid: uid, best_model: best, avg_metrics: avg, std_metrics: std,
                                          sub_models: get(:collect_sub_models) ? models_by_fold : nil)
          model.copy_values_from(self)
        end

        # @api private
        def save_meta(path, overwrite:, session:)
          ValidatorSupport.save_validator(self, path, overwrite: overwrite, session: session)
        end

        # @api private
        def self.load_meta(path, session:)
          meta = Persistence.read_metadata(session, path)
          estimator, evaluator, maps = ValidatorSupport.load_validator(path, session, meta)
          validator = new(uid: meta.fetch("uid"), estimator: estimator, evaluator: evaluator, estimator_param_maps: maps)
          Persistence.apply_params(validator, meta.fetch("paramMap").except("estimator", "evaluator", "estimatorParamMaps"))
          validator
        end
      end

      # The fitted {CrossValidator}: the best model, plus the metrics of every param map.
      class CrossValidatorModel < Transformer
        operator "org.apache.spark.ml.tuning.CrossValidatorModel"

        ValidatorSupport.declare_cv_params(self)

        # @return [Transformer] the best model (refit on the whole dataset).
        attr_reader :best_model
        # @return [Array<Float>] the average metric of each param map over the folds.
        attr_reader :avg_metrics
        # @return [Array<Float>] the (population) standard deviation of each param map's metric.
        attr_reader :std_metrics
        # @return [Array<Array<Transformer>>, nil] the models of every fold and map, when collected.
        attr_reader :sub_models

        # @api private
        def initialize(uid: nil, best_model: nil, avg_metrics: [], std_metrics: [], sub_models: nil, **values)
          super(uid: uid, **values)
          @best_model = best_model
          @avg_metrics = avg_metrics
          @std_metrics = std_metrics
          @sub_models = sub_models
        end

        # Apply the best model.
        # @return [DataFrame]
        def transform(dataset, params = nil)
          best_model.transform(dataset, params)
        end

        # @api private
        def save_meta(path, overwrite:, session:)
          persist = !sub_models.nil?
          extra = { "avgMetrics" => avg_metrics, "persistSubModels" => persist }
          extra["stdMetrics"] = std_metrics unless std_metrics.empty?
          ValidatorSupport.save_validator(self, path, overwrite: overwrite, session: session, extra: extra)
          best_model.save(File.join(path, "bestModel"), overwrite: false, session: session)
          ValidatorSupport.save_sub_models(path, sub_models, session) if persist
          nil
        end

        # @api private
        def self.load_meta(path, session:)
          meta = Persistence.read_metadata(session, path)
          estimator, evaluator, maps = ValidatorSupport.load_validator(path, session, meta)
          sub_models = if meta["persistSubModels"]
                         ValidatorSupport.load_sub_models(path, session, meta.fetch("avgMetrics").size, maps.size)
                       end
          model = new(uid: meta.fetch("uid"), best_model: Persistence.load_any(session, File.join(path, "bestModel")),
                      avg_metrics: meta.fetch("avgMetrics"), std_metrics: meta.fetch("stdMetrics", []), sub_models: sub_models)
          model.set(:estimator, estimator)
          model.set(:evaluator, evaluator)
          model.set(:estimator_param_maps, maps)
          Persistence.apply_params(model, meta.fetch("paramMap").except("estimator", "evaluator", "estimatorParamMaps"))
          model
        end
      end

      # Train-validation split: one random split of the rows, with `train_ratio` for training.
      class TrainValidationSplit < Estimator
        operator "org.apache.spark.ml.tuning.TrainValidationSplit"
        model -> { TrainValidationSplitModel }

        ValidatorSupport.declare_tvs_params(self)

        # Run the search and refit the best param map on `dataset`.
        #
        # @param dataset [DataFrame]
        # @param params [Hash, nil]
        # @return [TrainValidationSplitModel]
        def fit(dataset, params = nil)
          return copy(params).fit(dataset) if params && !params.empty?

          estimator = get(:estimator)
          maps = get(:estimator_param_maps)
          evaluator = get(:evaluator)
          split = ValidatorSupport.single_split(dataset, get(:train_ratio), owner_uid: uid, seed: get(:seed))
          metrics_by_fold, models_by_fold = ValidatorSupport.search([split], estimator, maps, evaluator,
                                                                    parallelism: get(:parallelism), collect: get(:collect_sub_models))
          metrics = metrics_by_fold.first
          best = estimator.fit(dataset, maps[ValidatorSupport.best_index(metrics, evaluator)])
          model = TrainValidationSplitModel.new(uid: uid, best_model: best, validation_metrics: metrics,
                                                sub_models: get(:collect_sub_models) ? models_by_fold.first : nil)
          model.copy_values_from(self)
        end

        # @api private
        def save_meta(path, overwrite:, session:)
          ValidatorSupport.save_validator(self, path, overwrite: overwrite, session: session)
        end

        # @api private
        def self.load_meta(path, session:)
          meta = Persistence.read_metadata(session, path)
          estimator, evaluator, maps = ValidatorSupport.load_validator(path, session, meta)
          validator = new(uid: meta.fetch("uid"), estimator: estimator, evaluator: evaluator, estimator_param_maps: maps)
          Persistence.apply_params(validator, meta.fetch("paramMap").except("estimator", "evaluator", "estimatorParamMaps"))
          validator
        end
      end

      # The fitted {TrainValidationSplit}.
      class TrainValidationSplitModel < Transformer
        operator "org.apache.spark.ml.tuning.TrainValidationSplitModel"

        ValidatorSupport.declare_tvs_params(self)

        # @return [Transformer] the best model (refit on the whole dataset).
        attr_reader :best_model
        # @return [Array<Float>] the validation metric of each param map.
        attr_reader :validation_metrics
        # @return [Array<Transformer>, nil] the model of every param map, when collected.
        attr_reader :sub_models

        # @api private
        def initialize(uid: nil, best_model: nil, validation_metrics: [], sub_models: nil, **values)
          super(uid: uid, **values)
          @best_model = best_model
          @validation_metrics = validation_metrics
          @sub_models = sub_models
        end

        # Apply the best model.
        # @return [DataFrame]
        def transform(dataset, params = nil)
          best_model.transform(dataset, params)
        end

        # @api private
        def save_meta(path, overwrite:, session:)
          persist = !sub_models.nil?
          extra = { "validationMetrics" => validation_metrics, "persistSubModels" => persist }
          ValidatorSupport.save_validator(self, path, overwrite: overwrite, session: session, extra: extra)
          best_model.save(File.join(path, "bestModel"), overwrite: false, session: session)
          sub_models.each_with_index { |m, j| m.save(File.join(path, "subModels", j.to_s), overwrite: false, session: session) } if persist
          nil
        end

        # @api private
        def self.load_meta(path, session:)
          meta = Persistence.read_metadata(session, path)
          estimator, evaluator, maps = ValidatorSupport.load_validator(path, session, meta)
          sub_models = if meta["persistSubModels"]
                         Array.new(maps.size) { |j| Persistence.load_any(session, File.join(path, "subModels", j.to_s)) }
                       end
          model = new(uid: meta.fetch("uid"), best_model: Persistence.load_any(session, File.join(path, "bestModel")),
                      validation_metrics: meta.fetch("validationMetrics"), sub_models: sub_models)
          model.set(:estimator, estimator)
          model.set(:evaluator, evaluator)
          model.set(:estimator_param_maps, maps)
          Persistence.apply_params(model, meta.fetch("paramMap").except("estimator", "evaluator", "estimatorParamMaps"))
          model
        end
      end
    end

    # Re-export the public names next to the other operators, as PySpark does.
    CrossValidator = Tuning::CrossValidator
    CrossValidatorModel = Tuning::CrossValidatorModel
    TrainValidationSplit = Tuning::TrainValidationSplit
    TrainValidationSplitModel = Tuning::TrainValidationSplitModel
    ParamGridBuilder = Tuning::ParamGridBuilder
  end
end
