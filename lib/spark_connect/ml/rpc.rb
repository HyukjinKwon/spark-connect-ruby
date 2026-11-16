# frozen_string_literal: true

module SparkConnect
  module ML
    # Raised when the connected server is too old for an ML feature (for example
    # `CleanCache` on Spark 4.0, which predates the ML cache admin commands).
    class UnsupportedServerError < SparkConnect::Error; end

    # Server-side model cache handle. Every fitted model (and every summary
    # cached by the server) is referenced by an id in the server's ML cache. The
    # handle is reference counted: copies of a model share it, and when the
    # last reference goes away the id is queued for deletion. The queue is
    # drained on the next ML call, or by {Rpc.flush_pending}, so no RPC ever runs
    # from a finalizer.
    class ModelRef
      # @return [String] the server-side object id.
      attr_reader :id

      # @param id [String]
      # @param session [SparkSession]
      def initialize(id, session)
        @id = id
        @session = session
        @count = 1
      end

      # @return [void]
      def add_ref
        @count += 1
        nil
      end

      # Drop one reference; the id is queued for deletion when none remain.
      # Takes no lock, so it is safe to call from a finalizer (a Mutex cannot
      # be taken there); the GVL keeps the integer update consistent.
      # @return [void]
      def release_ref
        return nil if @count <= 0

        @count -= 1
        Rpc.enqueue_delete(@session, @id) if @count.zero?
        nil
      end

      # @return [Boolean]
      def released? = @count <= 0
    end

    # Plumbing for ML commands: builds the protos, sends them through the
    # session's client, and decodes the `MlCommandResult`.
    module Rpc
      HELPER_ID = "______ML_CONNECT_HELPER______"
      SUMMARY_LOST = "CONNECT_ML.MODEL_SUMMARY_LOST"

      # Spark version (as "major.minor.patch") at which each ML feature appeared.
      FEATURES = {
        basic: "4.0.0",
        cache_admin: "4.1.0", # CleanCache, GetCacheInfo, GetModelSize, CreateSummary, Delete.evict_only
        model_summary_dataset: "4.1.0", # MlRelation.model_summary_dataset (restoring offloaded summaries)
        warning_message: "4.1.0",
      }.freeze

      PENDING = Queue.new

      @versions = ObjectSpace::WeakMap.new
      @versions_lock = Mutex.new

      module_function

      # @return [String] the server's Spark version, cached per session.
      def server_version(session)
        @versions_lock.synchronize { @versions[session] } || begin
          version = session.version
          @versions_lock.synchronize { @versions[session] = version }
          version
        end
      end

      # @return [Boolean] whether the server supports the named ML feature.
      def supports?(session, feature)
        wanted = FEATURES.fetch(feature).split(".").map(&:to_i)
        have = server_version(session).to_s.split(/[.-]/).first(3).map(&:to_i)
        (have.fill(0, have.size...3) <=> wanted) >= 0
      end

      # Raise {UnsupportedServerError} unless the server supports `feature`.
      # @return [void]
      def require_feature!(session, feature)
        return if supports?(session, feature)

        raise UnsupportedServerError,
              "#{feature} needs a Spark Connect server of at least #{FEATURES.fetch(feature)}, " \
              "but the server is #{server_version(session)}"
      end

      # The `MlOperator` proto for an operator instance.
      # @param operator [Params]
      # @param type [Symbol] `:estimator`, `:transformer`, `:model` or `:evaluator`.
      # @return [Proto::MlOperator]
      def operator_proto(operator, type = operator_type(operator))
        Proto::MlOperator.new(name: operator.class.operator, uid: operator.uid, type: OPERATOR_TYPES.fetch(type))
      end

      # @return [Symbol] the operator type of an instance.
      def operator_type(operator)
        case operator
        when Estimator then :estimator
        when Evaluator then :evaluator
        when Model then :model
        else :transformer
        end
      end

      OPERATOR_TYPES = {
        estimator: :OPERATOR_TYPE_ESTIMATOR,
        transformer: :OPERATOR_TYPE_TRANSFORMER,
        evaluator: :OPERATOR_TYPE_EVALUATOR,
        model: :OPERATOR_TYPE_MODEL,
      }.freeze

      # Send one `MlCommand` and return its `MlCommandResult`.
      #
      # @param session [SparkSession]
      # @param ml [Proto::MlCommand]
      # @return [Proto::MlCommandResult, nil]
      def run(session, ml)
        flush_pending(session)
        command = Proto::Command.new(ml_command: ml)
        session.client.execute_command(command).ml_command_result
      end

      # Ask the server to drop `ids` from its ML cache.
      # @return [void]
      def delete(session, ids)
        return if ids.empty?

        run(session, Proto::MlCommand.new(delete: Proto::MlCommand::Delete.new(obj_refs: ids.map { |i| Proto::ObjectRef.new(id: i) })))
        nil
      end

      # Queue an id for deletion (safe to call from a finalizer).
      # @api private
      def enqueue_delete(session, id)
        PENDING << [session, id]
        nil
      end

      # Delete every model id queued by garbage-collected handles that belong
      # to `session`; the others stay queued.
      # @return [void]
      def flush_pending(session)
        pending = []
        pending << PENDING.pop until PENDING.empty?
        mine, others = pending.partition { |owner, _| owner.equal?(session) }
        others.each { |entry| PENDING << entry }
        mine.map(&:last).each do |id|
          delete(session, [id])
        rescue StandardError
          nil
        end
        nil
      end

      # @return [Hash] the decoded `operator_info` of a result: `:obj_ref`,
      #   `:name`, `:uid`, `:params` (a `{spark_name => value}` hash) and `:warning_message`.
      def operator_info(result)
        info = result.operator_info
        {
          obj_ref: info.obj_ref && !info.obj_ref.id.empty? ? info.obj_ref.id : nil,
          name: info.name,
          uid: info.uid,
          params: info.params ? info.params.params.each_with_object({}) { |(k, v), acc| acc[k] = Serialize.decode(v) } : {},
          warning_message: info.warning_message.empty? ? nil : info.warning_message,
        }
      end

      # Decode a `MlCommandResult` into a Ruby value: the operator info, the
      # summary id, or the decoded param.
      #
      # @return [Object]
      def decode_result(result)
        case result.result_type
        when :operator_info then operator_info(result)
        when :summary then result.summary
        when :param then Serialize.decode(result.param)
        end
      end

      # Run a `Fetch` against an object (a model, a summary, or the helper).
      #
      # @param session [SparkSession]
      # @param obj_ref [String] the root object id.
      # @param methods [Array<Proto::Fetch::Method>] the method chain.
      # @return [Object] the decoded result (see {decode_result})
      def fetch(session, obj_ref, methods)
        ml = Proto::MlCommand.new(fetch: Proto::Fetch.new(obj_ref: Proto::ObjectRef.new(id: obj_ref), methods: methods))
        decode_result(run(session, ml))
      end

      # Ask the server to rebuild an offloaded summary from `dataset`.
      # @return [void]
      def create_summary(session, model_ref, dataset_relation)
        require_feature!(session, :cache_admin)
        ml = Proto::MlCommand.new(create_summary: Proto::MlCommand::CreateSummary.new(
          model_ref: Proto::ObjectRef.new(id: model_ref), dataset: dataset_relation
        ))
        run(session, ml)
        nil
      end

      # Evict every ML object of the session from the server cache.
      # @return [Integer] the number of objects removed (Spark 4.1+).
      def clean_cache(session)
        require_feature!(session, :cache_admin)
        decode_result(run(session, Proto::MlCommand.new(clean_cache: Proto::MlCommand::CleanCache.new)))
      end

      # @return [Hash] the server's ML cache information (Spark 4.1+).
      def cache_info(session)
        require_feature!(session, :cache_admin)
        decode_result(run(session, Proto::MlCommand.new(get_cache_info: Proto::MlCommand::GetCacheInfo.new)))
      end

      # Overwrite handling for a write target, the way PySpark's connect writer
      # does it: the server's ML helper deletes `path` first.
      # @return [void]
      def handle_overwrite(session, path, overwrite)
        return unless overwrite

        fetch(session, HELPER_ID, [Proto::Fetch::Method.new(method: "handleOverwrite", args: [
                                                              Proto::Fetch::Method::Args.new(param: Serialize.literal(path)),
                                                              Proto::Fetch::Method::Args.new(param: Serialize.literal(true)),
                                                            ])])
        nil
      end
    end
  end
end
