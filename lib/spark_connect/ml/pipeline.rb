# frozen_string_literal: true

module SparkConnect
  module ML
    # A pipeline of stages (estimators and transformers), like `pyspark.ml.Pipeline`.
    #
    # `fit` runs client-side, the way PySpark's Connect client does: each
    # estimator stage is fit on the output of the stages before it, and the
    # result is a {PipelineModel}. Every stage is a real server operator, so the
    # whole graph runs on the server when the final DataFrame is executed.
    #
    # @example
    #   scaler = SparkConnect::ML::Feature::StandardScaler.new(input_col: "features", output_col: "scaled")
    #   lr = SparkConnect::ML::Classification::LogisticRegression.new(features_col: "scaled")
    #   model = SparkConnect::ML::Pipeline.new(stages: [scaler, lr]).fit(train)
    class Pipeline < Estimator
      param :stages, :any, doc: "a list of pipeline stages"

      # @return [Array<Params>] the stages.
      def stages_list
        get(:stages) || []
      end

      # @param value [Array<Params>]
      # @return [self]
      def set_stages(value)
        set(:stages, value.to_a)
      end

      # Fit the pipeline on `dataset`.
      #
      # @param dataset [DataFrame]
      # @param params [Hash, nil]
      # @return [PipelineModel]
      def fit(dataset, params = nil)
        return copy(params).fit(dataset) if params && !params.empty?

        stages = stages_list
        stages.each do |stage|
          unless stage.is_a?(Estimator) || stage.is_a?(Transformer)
            raise IllegalArgumentError,
                  "Cannot recognize a pipeline stage of type #{stage.class}."
          end
        end
        last_estimator = stages.rindex { |s| s.is_a?(Estimator) } || -1
        fitted = []
        stages.each_with_index do |stage, i|
          if i <= last_estimator
            if stage.is_a?(Transformer)
              fitted << stage
              dataset = stage.transform(dataset)
            else
              model = stage.fit(dataset)
              fitted << model
              dataset = model.transform(dataset) if i < last_estimator
            end
          else
            fitted << stage
          end
        end
        PipelineModel.new(uid: uid, stages: fitted)
      end

      # @api private
      def save_meta(path, overwrite:, session:)
        Persistence.save_composite(self, path, overwrite: overwrite, session: session,
                                               children: Persistence.stage_children(stages_list))
      end

      # @api private
      def self.load_meta(path, session:)
        meta, children = Persistence.load_composite(path, session: session)
        new(uid: meta.fetch("uid"), stages: children.values)
      end
    end

    # The fitted {Pipeline}: a list of fitted stages applied in order.
    class PipelineModel < Transformer
      param :stages, :any, doc: "a list of fitted stages"

      # @return [Array<Transformer>] the fitted stages.
      def stages_list
        get(:stages) || []
      end

      # Apply every stage in order.
      # @param dataset [DataFrame]
      # @param params [Hash, nil]
      # @return [DataFrame]
      def transform(dataset, params = nil)
        return copy(params).transform(dataset) if params && !params.empty?

        stages_list.inject(dataset) { |df, stage| stage.transform(df) }
      end

      # @api private
      def save_meta(path, overwrite:, session:)
        Persistence.save_composite(self, path, overwrite: overwrite, session: session,
                                               children: Persistence.stage_children(stages_list))
      end

      # @api private
      def self.load_meta(path, session:)
        meta, children = Persistence.load_composite(path, session: session)
        new(uid: meta.fetch("uid"), stages: children.values)
      end
    end
  end
end
