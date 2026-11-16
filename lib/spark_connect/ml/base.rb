# frozen_string_literal: true

module SparkConnect
  module ML
    # Shared behaviour of the operator base classes: the server-side operator
    # name (`operator`) and the session an operator instance talks through.
    # Estimators and transformers are client-side objects; only fitted models
    # hold a server-side reference (see {Model}).
    class Estimator < Params
      # Fit on `dataset`. Returns a fitted {Model} of the class declared with
      # {Estimator.model}; the model is backed by an object in the server's ML cache.
      #
      # @param dataset [DataFrame]
      # @param params [Hash{Param, Symbol => Object}, Array<Hash>, nil] overrides; an
      #   array of param maps fits once per map and returns an Array of models.
      # @return [Model, Array<Model>]
      def fit(dataset, params = nil)
        return params.map { |map| fit(dataset, map) } if params.is_a?(Array)
        return copy(params).fit(dataset) if params && !params.empty?

        session = dataset.session
        result = Rpc.run(session, Proto::MlCommand.new(fit: Proto::MlCommand::Fit.new(
          estimator: Rpc.operator_proto(self, :estimator),
          params: Serialize.ml_params(explicit_param_map),
          dataset: dataset.relation
        )))
        info = Rpc.operator_info(result)
        warn("spark-connect: #{info[:warning_message]}") if info[:warning_message]
        build_model(session, info[:obj_ref], dataset.relation)
      end

      # @return [Class<Model>, nil] the model class this estimator produces.
      def model_class
        self.class.model_class
      end

      # Declare the model class produced by `fit`. Pass a class, or a lambda
      # returning one so the model can be declared later in the file.
      # @return [Class, nil]
      def self.model(klass = nil)
        return @model = klass if klass

        resolved = @model
        resolved = resolved.call if resolved.respond_to?(:call)
        resolved || (superclass.respond_to?(:model) ? superclass.model : nil)
      end

      # @api private
      def self.model_class = model

      # Write this estimator to `path`.
      # @param path [String]
      # @param overwrite [Boolean]
      # @param session [SparkSession]
      # @return [void]
      def save(path, overwrite: false, session: nil)
        session = ML.session_or_active(session)
        Persistence.save_operator(self, path, overwrite: overwrite, session: session)
      end

      # Read an estimator written by {#save}.
      # @param path [String]
      # @param session [SparkSession]
      # @return [Estimator]
      def self.load(path, session: nil)
        session = ML.session_or_active(session)
        Persistence.load_operator(self, path, session: session)
      end

      private

      def build_model(session, obj_ref, dataset_relation)
        model = model_class.new(uid: uid)
        model.attach(session, obj_ref, training_relation: dataset_relation)
        model.copy_values_from(self)
        model.send(:build_training_summary, dataset_relation) if model.class.summary_class
        model
      end
    end

    # Base class of the non-model transformers (VectorAssembler, Tokenizer, ...),
    # and of {Model}.
    class Transformer < Params
      # Apply the transformer to `dataset`, returning a lazy {DataFrame} (the
      # server runs it when the DataFrame is executed).
      #
      # @param dataset [DataFrame]
      # @param params [Hash, nil] overrides for this call only.
      # @return [DataFrame]
      def transform(dataset, params = nil)
        return copy(params).transform(dataset) if params && !params.empty?

        session = dataset.session
        relation = PlanBuilder.relation(session, ml_relation: transform_relation(dataset.relation))
        DataFrame.new(session, relation)
      end

      # @api private
      def transform_relation(input)
        Proto::MlRelation.new(transform: Proto::MlRelation::Transform.new(
          input: input,
          transformer: Rpc.operator_proto(self, :transformer),
          params: Serialize.ml_params(explicit_param_map)
        ))
      end

      # Write this transformer to `path`.
      # @return [void]
      def save(path, overwrite: false, session: nil)
        session = ML.session_or_active(session)
        Persistence.save_operator(self, path, overwrite: overwrite, session: session)
      end

      # Read a transformer written by {#save}.
      # @return [Transformer]
      def self.load(path, session: nil)
        session = ML.session_or_active(session)
        Persistence.load_operator(self, path, session: session)
      end
    end

    # Base class of evaluators (metrics over a predictions DataFrame).
    class Evaluator < Params
      # Compute the metric over `dataset`.
      #
      # @param dataset [DataFrame]
      # @param params [Hash, nil]
      # @return [Float]
      def evaluate(dataset, params = nil)
        return copy(params).evaluate(dataset) if params && !params.empty?

        result = Rpc.run(dataset.session, Proto::MlCommand.new(evaluate: Proto::MlCommand::Evaluate.new(
          evaluator: Rpc.operator_proto(self, :evaluator),
          params: Serialize.ml_params(explicit_param_map),
          dataset: dataset.relation
        )))
        Rpc.decode_result(result)
      end

      # Whether a larger metric value is better (PySpark's `isLargerBetter`).
      # @return [Boolean]
      def larger_better? = true

      # @return [Boolean] alias of {#larger_better?}.
      def is_larger_better? = larger_better?

      # Write this evaluator to `path`.
      def save(path, overwrite: false, session: nil)
        session = ML.session_or_active(session)
        Persistence.save_operator(self, path, overwrite: overwrite, session: session)
      end

      # @return [Evaluator]
      def self.load(path, session: nil)
        session = ML.session_or_active(session)
        Persistence.load_operator(self, path, session: session)
      end
    end

    # Class-level DSL for the accessors of models and summaries: {AttributeDSL.attribute},
    # {AttributeDSL.method_call} and (models only) {AttributeDSL.summary_class}.
    module AttributeDSL
      # Declare an attribute read from the server: `model.coefficients`.
      #
      # @param name [Symbol] Ruby name.
      # @param type [Symbol] `:vector`, `:matrix`, `:double`, `:int`, `:long`, `:string`,
      #   `:boolean`, `:double_array`, `:int_array`, `:string_array`, `:dataframe`, or `:any`.
      # @param spark_name [String, nil] the Scala method name when it differs (`getSplits`).
      # @return [void]
      def attribute(name, type = :any, spark_name: nil)
        method_name = spark_name || ML.camelize(name)
        define_method(name) do
          AttributeSupport.invoke(self, [AttributeSupport.method_proto(method_name, [])], type)
        end
        nil
      end

      # Declare a server method with arguments: `model.predict(vector)`.
      #
      # @param name [Symbol]
      # @param arg_count [Integer] the number of positional arguments.
      # @param returns [Symbol, Class] a result type (see {attribute}), or a
      #   {Summary} subclass when the method returns a summary.
      # @param spark_name [String, nil]
      # @return [void]
      def method_call(name, arg_count: 0, returns: :any, spark_name: nil)
        method_name = spark_name || ML.camelize(name)
        define_method(name) do |*args|
          raise IllegalArgumentError, "#{name} takes #{arg_count} argument(s)" unless args.size == arg_count

          if returns.is_a?(Class) && returns <= Summary
            obj_ref, base = AttributeSupport.owner_chain(self)
            summary_id = Rpc.fetch(session, obj_ref, base + [AttributeSupport.method_proto(method_name, args)])
            return returns.new(session, summary_id, [])
          end
          AttributeSupport.invoke(self, [AttributeSupport.method_proto(method_name, args)], returns)
        end
        nil
      end

      # Declare the training summary class. The summary is available as
      # `model.summary` after `fit`; it is absent on models loaded from disk
      # (unless the server still caches it).
      # @param klass [Class<Summary>, Proc]
      # @return [Class, nil]
      def summary_class(klass = nil)
        return @summary_class = klass if klass

        resolved = @summary_class
        resolved = resolved.call if resolved.respond_to?(:call)
        resolved || (superclass.respond_to?(:summary_class) ? superclass.summary_class : nil)
      end
    end

    # A training summary (or evaluation summary) cached by the server. A
    # training summary is addressed as a method chain on its model: the summary
    # of model `m` is the owner `(m.ref_id, [summary])`, and `m.summary.roc` is
    # the fetch `[summary, roc]` on the model id. The result of
    # `model.evaluate(df)` is a summary with its own cache id and no chain.
    class Summary
      extend AttributeDSL

      # @return [SparkSession]
      attr_reader :session

      # @api private
      # @return [String] the server object the chain starts from.
      attr_reader :obj_ref

      # @api private
      # @return [Array<Proto::Fetch::Method>] the chain of methods from `obj_ref`.
      attr_reader :chain

      # @api private
      # @return [Proto::Relation, nil] the dataset the summary was built on.
      attr_reader :dataset_relation

      # @api private
      def initialize(session, obj_ref, chain, dataset_relation: nil)
        @session = session
        @obj_ref = obj_ref
        @chain = chain
        @dataset_relation = dataset_relation
      end
    end

    # Plumbing for the accessors declared on models and summaries. Each accessor
    # becomes one `Fetch` (or, for DataFrame results, an `MlRelation.fetch`).
    module AttributeSupport
      module_function

      # @return [Proto::Fetch::Method]
      def method_proto(name, args)
        Proto::Fetch::Method.new(
          method: name,
          args: args.map do |arg|
            case arg
            when DataFrame then Proto::Fetch::Method::Args.new(input: arg.relation)
            else Proto::Fetch::Method::Args.new(param: Serialize.literal(arg))
            end
          end
        )
      end

      # @api private
      # @return [Array(String, Array<Proto::Fetch::Method>)] the root object id and its chain.
      def owner_chain(owner)
        owner.is_a?(Model) ? [owner.ref_id, []] : [owner.obj_ref, owner.chain]
      end

      # @api private
      def owner_dataset(owner)
        owner.is_a?(Model) ? owner.training_relation : owner.dataset_relation
      end

      # Call `chain` (one method, usually) on `owner` and convert the result.
      #
      # @param owner [Model, Summary]
      # @param chain [Array<Proto::Fetch::Method>] the methods to call after the owner's chain.
      # @param type [Symbol] the result type (see {Model::ClassMethods#attribute}).
      # @return [Object]
      def invoke(owner, chain, type)
        obj_ref, base = owner_chain(owner)
        methods = base + chain
        return relation_result(owner, methods) if type == :dataframe

        fetch_restoring_summary(owner, obj_ref, methods)
      end

      # @api private
      def relation_result(owner, methods)
        session = owner.session
        obj_ref = owner_chain(owner).first
        ml_rel = Proto::MlRelation.new(fetch: Proto::Fetch.new(obj_ref: Proto::ObjectRef.new(id: obj_ref), methods: methods))
        dataset = owner_dataset(owner)
        ml_rel.model_summary_dataset = dataset if summary_chain?(methods) && dataset && Rpc.supports?(session, :model_summary_dataset)
        DataFrame.new(session, PlanBuilder.relation(session, ml_relation: ml_rel))
      end

      # @api private
      def summary_chain?(methods)
        methods.first && methods.first["method"] == "summary"
      end

      # Fetch, rebuilding an offloaded training summary once when the server
      # lost it (Spark 4.1+ keeps the training dataset around for that).
      # @api private
      def fetch_restoring_summary(owner, obj_ref, methods)
        Rpc.fetch(owner.session, obj_ref, methods)
      rescue SparkConnectError => e
        raise unless e.error_class == Rpc::SUMMARY_LOST && summary_chain?(methods) && owner_dataset(owner)

        Rpc.create_summary(owner.session, obj_ref, owner_dataset(owner))
        Rpc.fetch(owner.session, obj_ref, methods)
      end
    end

    # A fitted model: a server-side object referenced by id, plus the
    # client-side decorations PySpark adds (uid, summary, copied params).
    #
    # Attributes are declared with {ClassMethods#attribute}, methods with
    # {ClassMethods#method_call}, and the training summary with
    # {ClassMethods#summary_class}.
    class Model < Transformer
      extend AttributeDSL

      # @return [SparkSession, nil]
      attr_reader :session

      # @return [ModelRef, nil] the server-side handle.
      attr_reader :model_ref

      # @return [Summary, nil] the training summary, when `fit` produced one.
      attr_reader :summary_object

      # @api private
      # @return [Proto::Relation, nil] the training dataset (summary restore).
      attr_reader :training_relation

      # Attach this model to a server object.
      #
      # @api private
      def attach(session, obj_ref, training_relation: nil)
        @session = session
        @model_ref = ModelRef.new(obj_ref, session)
        @training_relation = training_relation
        ObjectSpace.define_finalizer(self, Model.finalizer_for(@model_ref))
        self
      end

      # The server-side object id of this model.
      # @return [String]
      def ref_id = model_ref.id

      # @api private
      def self.finalizer_for(ref)
        proc { ref.release_ref }
      end

      # @return [Boolean] whether a training summary exists.
      def has_summary?
        !@summary_object.nil?
      end

      # The training summary.
      # @return [Summary]
      # @raise [IllegalArgumentError] when there is no summary (models loaded from disk)
      def summary
        return @summary_object if @summary_object

        raise IllegalArgumentError, "No training summary available for this #{self.class.name.split('::').last}"
      end

      # @return [self]
      def copy(extra = {})
        that = super
        ref = that.model_ref
        if ref
          ref.add_ref
          ObjectSpace.define_finalizer(that, Model.finalizer_for(ref))
        end
        that
      end

      # Release this model's server-side object (when no other copy holds it).
      # @return [void]
      def release
        @model_ref&.release_ref
        nil
      end

      # @api private
      def transform_relation(input)
        Proto::MlRelation.new(transform: Proto::MlRelation::Transform.new(
          input: input,
          obj_ref: Proto::ObjectRef.new(id: ref_id),
          params: Serialize.ml_params(explicit_param_map)
        ))
      end

      # Write this model to `path` (overwrites when `overwrite:`).
      # @return [void]
      def save(path, overwrite: false, session: nil)
        Persistence.save_model(self, path, overwrite: overwrite, session: ML.session_or_active(session, @session))
      end

      # Read a model written by {#save}.
      # @return [Model]
      def self.load(path, session: nil)
        session = ML.session_or_active(session)
        Persistence.load_model(self, path, session: session)
      end

      private

      def build_training_summary(dataset_relation)
        klass = self.class.summary_class
        return unless klass

        @training_relation = dataset_relation
        @summary_object = klass.new(session, ref_id, [AttributeSupport.method_proto("summary", [])],
                                    dataset_relation: dataset_relation)
      end
    end
  end
end
