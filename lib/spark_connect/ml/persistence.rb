# frozen_string_literal: true

require "json"

module SparkConnect
  module ML
    # Save and load for estimators, transformers, evaluators, models and the
    # meta-algorithms (pipelines, cross-validators, train-validation-splits).
    #
    # Operators and models go through `MlCommand.Write` / `MlCommand.Read`, so the
    # server writes the files in Spark's own ML format. Meta-algorithms are
    # orchestrated here, like PySpark's connect client: their own metadata is a
    # one-line JSON document written with the DataFrame text writer, and each
    # child (stage, estimator, evaluator, best model) is saved into a
    # subdirectory with the same functions.
    module Persistence
      module_function

      # Write an estimator, transformer or evaluator (or a meta-algorithm).
      # @return [void]
      def save_operator(operator, path, overwrite:, session:)
        if operator.respond_to?(:save_meta, true)
          operator.send(:save_meta, path, overwrite: overwrite, session: session)
          return nil
        end

        Rpc.run(session, Proto::MlCommand.new(write: Proto::MlCommand::Write.new(
          operator: Rpc.operator_proto(operator),
          params: Serialize.ml_params(operator.explicit_param_map),
          path: path,
          should_overwrite: overwrite,
          options: {}
        )))
        nil
      end

      # Write a fitted model.
      # @return [void]
      def save_model(model, path, overwrite:, session:)
        Rpc.run(session, Proto::MlCommand.new(write: Proto::MlCommand::Write.new(
          obj_ref: Proto::ObjectRef.new(id: model.ref_id),
          params: Serialize.ml_params(model.explicit_param_map),
          path: path,
          should_overwrite: overwrite,
          options: {}
        )))
        nil
      end

      # Read an estimator, transformer or evaluator class `klass` from `path`.
      # @return [Params]
      def load_operator(klass, path, session:)
        return klass.load_meta(path, session: session) if klass.respond_to?(:load_meta)

        type = if klass <= Estimator then :estimator
               elsif klass <= Evaluator then :evaluator
               else :transformer
               end
        info = read_info(session, klass.operator, type, path)
        instance = klass.new(uid: info[:uid])
        apply_params(instance, info[:params])
        instance
      end

      # Read a model class `klass` (a {Model} subclass) from `path`.
      # @return [Model]
      def load_model(klass, path, session:)
        info = read_info(session, klass.operator, :model, path)
        model = klass.new(uid: info[:uid])
        model.attach(session, info[:obj_ref])
        apply_params(model, info[:params])
        model
      end

      # Load any saved ML instance whose class name is recorded in its metadata.
      # @api private
      # @return [Params]
      def load_any(session, path)
        meta = read_metadata(session, path)
        klass = Object.const_get(meta.fetch("class"))
        klass.load(path, session: session)
      end

      # @api private
      def read_info(session, operator_name, type, path)
        result = Rpc.run(session, Proto::MlCommand.new(read: Proto::MlCommand::Read.new(
          operator: Proto::MlOperator.new(name: operator_name, type: Rpc::OPERATOR_TYPES.fetch(type)),
          path: path
        )))
        Rpc.operator_info(result)
      end

      # Set loaded param values (keyed by Spark name) on `instance`.
      # @api private
      def apply_params(instance, spark_values)
        spark_values.each do |spark_name, value|
          declared = instance.class.params.values.find { |p| p.spark_name == spark_name }
          instance.set(declared, value) if declared && !value.nil?
        end
      end

      # ---- meta-algorithms (pipelines, validators) ----------------------------

      # Save a meta-algorithm: its own params as JSON metadata, then each child
      # (a stage, an estimator, an evaluator, a best model) into a subdirectory.
      #
      # @param instance [Params]
      # @param children [Hash{String => Params}] relative directory => child, in order.
      # @param extra [Hash] extra metadata (for example the estimator param maps).
      # @return [void]
      def save_composite(instance, path, overwrite:, session:, children:, extra: {})
        Rpc.handle_overwrite(session, path, overwrite)
        metadata = base_metadata(instance).merge(
          "paramMap" => json_params(instance.explicit_param_map.reject { |_, value| object_value?(value) }),
          "children" => children.map { |rel, child| { "path" => rel, "class" => child.class.name } }
        ).merge(extra)
        write_metadata(session, path, metadata, overwrite: false)
        children.each do |rel, child|
          child.save(File.join(path, rel), overwrite: false, session: session)
        end
        nil
      end

      # Load the children saved by {save_composite}, in order.
      # @return [Array(Hash, Hash{String => Params})] the metadata and the children.
      def load_composite(path, session:)
        meta = read_metadata(session, path)
        children = meta.fetch("children").to_h do |child|
          [child.fetch("path"), Object.const_get(child.fetch("class")).load(File.join(path, child.fetch("path")), session: session)]
        end
        [meta, children]
      end

      # Child directories for pipeline stages: `stages/<index>_<uid>`.
      # @return [Hash{String => Params}]
      def stage_children(stages)
        stages.each_with_index.to_h { |stage, i| [File.join("stages", format("%<i>d_%<u>s", i: i, u: stage.uid)), stage] }
      end

      # @api private
      def base_metadata(instance)
        { "class" => instance.class.name, "uid" => instance.uid, "timestamp" => (Time.now.to_f * 1000).to_i }
      end

      # Params holding operators (estimators, stages) are saved as children, not JSON.
      # @api private
      def object_value?(value)
        value.is_a?(Params) || (value.is_a?(Array) && value.any? { |v| v.is_a?(Params) || v.is_a?(Hash) })
      end

      # ---- one-line JSON metadata for meta-algorithms -------------------------

      # Write `metadata` (a Hash) to `path/metadata`.
      # @api private
      def write_metadata(session, path, metadata, overwrite:)
        json = JSON.generate(metadata)
        df = session.create_data_frame([{ "value" => json }], Types.struct(Types.field("value", Types.string)))
        writer = df.coalesce(1).write
        writer = writer.mode(:overwrite) if overwrite
        writer.text(File.join(path, "metadata"))
        nil
      end

      # Read the JSON metadata written by {write_metadata}.
      # @api private
      # @return [Hash]
      def read_metadata(session, path)
        row = session.read.text(File.join(path, "metadata")).collect.first
        JSON.parse(row["value"] || row.values.first)
      end

      # The JSON-friendly form of an explicit param map, keyed by Spark name.
      # @api private
      def json_params(params)
        params.to_h do |declared, value|
          [declared.spark_name, json_value(value)]
        end
      end

      # @api private
      def json_value(value)
        case value
        when Linalg::Vector, Linalg::Matrix
          raise IllegalArgumentError, "#{value.class.name.split('::').last} params cannot be saved in tuning metadata"
        when Array then value.map { |v| json_value(v) }
        else value
        end
      end

      # Stage directory name for child `index` with `uid`.
      # @api private
      def child_path(path, index, uid)
        File.join(path, "stages", format("%<i>d_%<uid>s", i: index, uid: uid))
      end
    end
  end
end
