# frozen_string_literal: true

require "json"

module SparkConnect
  module ML
    # Save and load in Spark's standard ML persistence format (the one PySpark's
    # Connect client and Spark Classic both read and write).
    #
    # - Operators and fitted models are written by the server (`MlCommand.Write`)
    #   and read back with `MlCommand.Read`, so their directories are exactly what
    #   Spark writes.
    # - Pipelines, cross-validators, train-validation splits and one-vs-rest
    #   models are orchestrated here, the way PySpark's connect client does it:
    #   `metadata` is a one-line JSON document (class, timestamp, sparkVersion,
    #   uid, paramMap, defaultParamMap, plus extra keys), written with the
    #   DataFrame text writer, and children go into the subdirectories PySpark
    #   uses (`stages/<index>_<uid>`, `estimator`, `evaluator`, `bestModel`,
    #   `subModels/fold<i>/<j>`, `classifier`, `model_<i>`, `epm_<name><n>`).
    module Persistence
      module_function

      # ---- operators and models: the server writes and reads them ------------

      # Write an estimator or transformer (or a composite that writes itself).
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

      # Write a fitted model through the server's object reference.
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

      # Load any saved ML instance, choosing the class from the `class` in its metadata.
      # @return [Params]
      def load_any(session, path)
        klass = class_for(read_metadata(session, path).fetch("class"))
        if klass <= Model && !klass.respond_to?(:load_meta)
          load_model(klass, path, session: session)
        else
          load_operator(klass, path, session: session)
        end
      end

      # @api private
      def read_info(session, operator_name, type, path)
        result = Rpc.run(session, Proto::MlCommand.new(read: Proto::MlCommand::Read.new(
          operator: Proto::MlOperator.new(name: operator_name, type: Rpc::OPERATOR_TYPES.fetch(type)),
          path: path
        )))
        Rpc.operator_info(result)
      end

      # Set loaded param values (keyed by Spark name) on `instance`. Names the
      # class does not declare (such as `stageUids`) are ignored.
      # @api private
      def apply_params(instance, spark_values)
        spark_values.each do |spark_name, value|
          declared = instance.class.params.values.find { |p| p.spark_name == spark_name }
          instance.set(declared, value) if declared && !value.nil?
        end
      end

      # ---- the metadata document ----------------------------------------------

      # Write the `metadata` document of `instance` under `path`, like
      # `DefaultParamsWriter.saveMetadata`.
      #
      # @param session [SparkSession]
      # @param path [String]
      # @param instance [Params]
      # @param param_map [Hash{String => Object}] the `paramMap` section (Spark names).
      # @param extra [Hash] keys added to the document (for example `avgMetrics`).
      # @return [void]
      def write_metadata(session, path, instance, param_map:, extra: {}, exclude: [])
        document = {
          "class" => scala_class_name(instance.class),
          "timestamp" => (Time.now.to_f * 1000).round,
          "sparkVersion" => Rpc.server_version(session).to_s,
          "uid" => instance.uid,
          "paramMap" => param_map.except(*exclude),
          "defaultParamMap" => default_json(instance, exclude: exclude),
        }.merge(extra)
        write_text(session, File.join(path, "metadata"), JSON.generate(document))
      end

      # @return [Hash] the parsed `metadata` document under `path`.
      def read_metadata(session, path)
        JSON.parse(read_text(session, File.join(path, "metadata")))
      end

      # The class name written to metadata: the Spark (Scala) class name.
      # @api private
      def scala_class_name(klass)
        klass.operator || klass.name
      end

      # The `paramMap` section of an instance: every param value (defaults
      # included, as `extractParamMap` does), except the names in `skip` and
      # operator-valued params (those are written as children).
      # @return [Hash{String => Object}]
      def extract_json_params(instance, skip: [])
        instance.extract_param_map.each_with_object({}) do |(declared, value), acc|
          next if skip.include?(declared.spark_name) || object_value?(value)

          acc[declared.spark_name] = json_value(value)
        end
      end

      # The `defaultParamMap` section: the defaults of the instance's class.
      # @api private
      def default_json(instance, exclude: [])
        instance.class.params.values.select(&:default?).each_with_object({}) do |declared, acc|
          next if exclude.include?(declared.spark_name)

          value = declared.default_for(instance.class)
          acc[declared.spark_name] = json_value(value) unless object_value?(value)
        end
      end

      # Params holding operators (estimators, stages) are children, not JSON.
      # @api private
      def object_value?(value)
        value.is_a?(Params) || (value.is_a?(Array) && value.any? { |v| v.is_a?(Params) || v.is_a?(Hash) })
      end

      # @api private
      def json_value(value)
        case value
        when Linalg::Vector, Linalg::Matrix
          raise IllegalArgumentError, "#{value.class.name.split('::').last} values cannot be written to metadata"
        when Array then value.map { |v| json_value(v) }
        else value
        end
      end

      # The directory name of stage `index` of `count` with `uid`: the index is
      # zero-padded to the width of `count`, as `PipelineSharedReadWrite.getStagePath`.
      # @api private
      def stage_dir_name(index, uid, count)
        "#{index.to_s.rjust(count.to_s.length, '0')}_#{uid}"
      end

      # Overwrite handling for a write target (deletes `path` first when asked).
      # @return [void]
      def handle_overwrite(session, path, overwrite)
        Rpc.handle_overwrite(session, path, overwrite)
      end

      # @api private
      def write_text(session, path, text)
        df = session.create_data_frame([{ "value" => text }], Types.struct(Types.field("value", Types.string)))
        df.coalesce(1).write.text(path)
        nil
      end

      # @api private
      def read_text(session, path)
        row = session.read.text(path).collect.first
        row["value"] || row.values.first
      end

      # Save pipeline stages to `path/stages/<index>_<uid>`.
      # @return [void]
      def save_stages(path, stages, session:)
        dir = File.join(path, "stages")
        stages.each_with_index do |stage, i|
          stage.save(File.join(dir, stage_dir_name(i, stage.uid, stages.size)), overwrite: false, session: session)
        end
        nil
      end

      # Load the stages listed in `meta["paramMap"]["stageUids"]`, in order.
      # @return [Array<Params>]
      def load_stages(path, meta, session:)
        uids = meta.fetch("paramMap").fetch("stageUids")
        uids.each_with_index.map do |uid, i|
          load_any(session, File.join(path, "stages", stage_dir_name(i, uid, uids.size)))
        end
      end

      # Find the Ruby class for a metadata `class` value: the Spark class name
      # (`org.apache.spark.ml.classification.LogisticRegression`), or a PySpark
      # one (`pyspark.ml.pipeline.PipelineModel`), matched by simple name.
      #
      # @param name [String]
      # @return [Class]
      # @raise [IllegalArgumentError] when no operator matches
      def class_for(name)
        classes = ML.operator_classes
        found = classes.find { |klass| klass.operator == name }
        if found.nil? && name.start_with?("pyspark.")
          simple = name.split(".").last
          found = classes.find { |klass| klass.name.split("::").last == simple }
        end
        found or raise IllegalArgumentError, "unsupported ML class #{name}"
      end
    end
  end
end
