# frozen_string_literal: true

module SparkConnect
  module ML
    # Model selection, like `pyspark.ml.tuning`.
    #
    # As in PySpark's Connect client, the search is orchestrated on the client:
    # for every fold (or the single split) and every param map, the estimator is
    # fit on the training rows and the evaluator scores the validation rows. Each
    # fit and evaluation is a server operation. The best param map is then fit on
    # the whole dataset.
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

      # Shared logic of the two validators: fold assignment and the search.
      module ValidatorSupport
        module_function

        # Rows of `dataset` split by a random column (or a user fold column).
        # @return [Array(DataFrame, DataFrame)] (train, validation) per fold.
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

        # Mean and population standard deviation per map.
        # @return [Array(Array<Float>, Array<Float>)]
        def avg_and_std(metrics_by_fold)
          n = metrics_by_fold.size
          columns = metrics_by_fold.transpose
          avg = columns.map { |col| col.sum / n.to_f }
          std = columns.each_with_index.map { |col, j| Math.sqrt(col.sum { |m| (m - avg[j])**2 } / n.to_f) }
          [avg, std]
        end

        # Best index by the evaluator's direction.
        # @return [Integer]
        def best_index(metrics, evaluator)
          evaluator.larger_better? ? metrics.each_with_index.max_by(&:first).last : metrics.each_with_index.min_by(&:first).last
        end

        # Serialise the estimator param maps for metadata.
        # @return [Array<Array<Hash>>]
        def json_maps(maps, estimator, evaluator)
          maps.map do |map|
            map.map do |param, value|
              parent = estimator.class.params.key?(param.name) ? "estimator" : "evaluator"
              { "parent" => parent, "name" => param.name.to_s, "value" => Persistence.json_value(value) }
            end
          end
        end

        # Rebuild param maps saved by {json_maps}.
        # @return [Array<Hash{Param => Object}>]
        def restore_maps(json, estimator, evaluator)
          json.map do |entries|
            entries.to_h do |entry|
              owner = entry.fetch("parent") == "estimator" ? estimator : evaluator
              [owner.param(entry.fetch("name")), entry.fetch("value")]
            end
          end
        end
      end

      # K-fold cross validation over an estimator, a param grid and an evaluator.
      class CrossValidator < Estimator
        param :num_folds, :int, default: 3, doc: "number of folds for cross validation (>= 2)"
        param :fold_col, :string, default: "", doc: "column with user-specified fold numbers in [0, numFolds); empty for random folds"
        param :seed, :long, doc: "random seed for the random fold split; unset means a random seed per fit"
        param :parallelism, :int, default: 1, doc: "number of threads to use when running parallel algorithms (>= 1)"
        param :collect_sub_models, :boolean, default: false, spark_name: "collectSubModels",
                                             doc: "whether to keep the models of every fold and param map"
        param :estimator, :any, doc: "estimator to be cross-validated"
        param :estimator_param_maps, :any, spark_name: "estimatorParamMaps", doc: "param maps to try"
        param :evaluator, :any, doc: "evaluator used to choose the best model"

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

          est = get(:estimator)
          maps = get(:estimator_param_maps)
          eva = get(:evaluator)
          n_folds = get(:num_folds)
          seed = is_defined?(:seed) ? get(:seed) : Random.new_seed % (2**31)
          fold_col = get(:fold_col)
          metrics_by_fold = []
          sub_models = []
          ValidatorSupport.folds(dataset, n_folds, owner_uid: uid, fold_col: fold_col, seed: seed).each do |train, validation|
            fold_metrics = []
            fold_models = []
            maps.each do |map|
              model = est.fit(train, map)
              fold_metrics << eva.evaluate(model.transform(validation, map))
              fold_models << model if get(:collect_sub_models)
            end
            metrics_by_fold << fold_metrics
            sub_models << fold_models if get(:collect_sub_models)
          end
          avg, std = ValidatorSupport.avg_and_std(metrics_by_fold)
          best = est.fit(dataset, maps[ValidatorSupport.best_index(avg, eva)])
          CrossValidatorModel.new(uid: uid, best_model: best, avg_metrics: avg, std_metrics: std,
                                  sub_models: get(:collect_sub_models) ? sub_models : nil)
        end

        # @api private
        def save_meta(path, overwrite:, session:)
          maps = ValidatorSupport.json_maps(get(:estimator_param_maps), get(:estimator), get(:evaluator))
          Persistence.save_composite(self, path, overwrite: overwrite, session: session,
                                                 children: { "estimator" => get(:estimator), "evaluator" => get(:evaluator) },
                                                 extra: { "estimatorParamMaps" => maps })
        end

        # @api private
        def self.load_meta(path, session:)
          meta, children = Persistence.load_composite(path, session: session)
          est = children.fetch("estimator")
          eva = children.fetch("evaluator")
          cv = new(uid: meta.fetch("uid"), estimator: est, evaluator: eva,
                   estimator_param_maps: ValidatorSupport.restore_maps(meta.fetch("estimatorParamMaps"), est, eva))
          Persistence.apply_params(cv, meta.fetch("paramMap", {}))
          cv
        end
      end

      # The fitted {CrossValidator}: the best model, plus the metrics of every param map.
      class CrossValidatorModel < Transformer
        # @return [Transformer] the best model (refit on the whole dataset).
        attr_reader :best_model
        # @return [Array<Float>] the average metric of each param map over the folds.
        attr_reader :avg_metrics
        # @return [Array<Float>] the standard deviation of each param map's metric.
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
          Persistence.save_composite(self, path, overwrite: overwrite, session: session,
                                                 children: { "bestModel" => best_model },
                                                 extra: { "avgMetrics" => avg_metrics, "stdMetrics" => std_metrics })
        end

        # @api private
        def self.load_meta(path, session:)
          meta, children = Persistence.load_composite(path, session: session)
          new(uid: meta.fetch("uid"), best_model: children.fetch("bestModel"),
              avg_metrics: meta.fetch("avgMetrics"), std_metrics: meta.fetch("stdMetrics"))
        end
      end

      # Train-validation split: one random split of the rows, with `train_ratio` for training.
      class TrainValidationSplit < Estimator
        param :train_ratio, :double, default: 0.75, doc: "ratio between training set and validation set (in (0, 1))"
        param :seed, :long, doc: "random seed for the split; unset means a random seed per fit"
        param :parallelism, :int, default: 1, doc: "number of threads to use when running parallel algorithms (>= 1)"
        param :collect_sub_models, :boolean, default: false, spark_name: "collectSubModels",
                                             doc: "whether to keep the model of every param map"
        param :estimator, :any, doc: "estimator to be validated"
        param :estimator_param_maps, :any, spark_name: "estimatorParamMaps", doc: "param maps to try"
        param :evaluator, :any, doc: "evaluator used to choose the best model"

        # Run the search and refit the best param map on `dataset`.
        #
        # @param dataset [DataFrame]
        # @param params [Hash, nil]
        # @return [TrainValidationSplitModel]
        def fit(dataset, params = nil)
          return copy(params).fit(dataset) if params && !params.empty?

          est = get(:estimator)
          maps = get(:estimator_param_maps)
          eva = get(:evaluator)
          seed = is_defined?(:seed) ? get(:seed) : Random.new_seed % (2**31)
          rand_col = "#{uid}_rand"
          df = dataset.with_column(rand_col, F.rand(seed))
          cond = F.col(rand_col) >= get(:train_ratio)
          train = df.filter(!cond)
          validation = df.filter(cond)
          models = []
          metrics = maps.map do |map|
            model = est.fit(train, map)
            models << model
            eva.evaluate(model.transform(validation, map))
          end
          best = est.fit(dataset, maps[ValidatorSupport.best_index(metrics, eva)])
          TrainValidationSplitModel.new(uid: uid, best_model: best, validation_metrics: metrics,
                                        sub_models: get(:collect_sub_models) ? models : nil)
        end

        # @api private
        def save_meta(path, overwrite:, session:)
          maps = ValidatorSupport.json_maps(get(:estimator_param_maps), get(:estimator), get(:evaluator))
          Persistence.save_composite(self, path, overwrite: overwrite, session: session,
                                                 children: { "estimator" => get(:estimator), "evaluator" => get(:evaluator) },
                                                 extra: { "estimatorParamMaps" => maps })
        end

        # @api private
        def self.load_meta(path, session:)
          meta, children = Persistence.load_composite(path, session: session)
          est = children.fetch("estimator")
          eva = children.fetch("evaluator")
          tvs = new(uid: meta.fetch("uid"), estimator: est, evaluator: eva,
                    estimator_param_maps: ValidatorSupport.restore_maps(meta.fetch("estimatorParamMaps"), est, eva))
          Persistence.apply_params(tvs, meta.fetch("paramMap", {}))
          tvs
        end
      end

      # The fitted {TrainValidationSplit}.
      class TrainValidationSplitModel < Transformer
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
          Persistence.save_composite(self, path, overwrite: overwrite, session: session,
                                                 children: { "bestModel" => best_model },
                                                 extra: { "validationMetrics" => validation_metrics })
        end

        # @api private
        def self.load_meta(path, session:)
          meta, children = Persistence.load_composite(path, session: session)
          new(uid: meta.fetch("uid"), best_model: children.fetch("bestModel"),
              validation_metrics: meta.fetch("validationMetrics"))
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
