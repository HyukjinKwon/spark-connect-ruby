# frozen_string_literal: true

require "securerandom"

module SparkConnect
  # Progress of one Spark stage, reported to the handlers registered with
  # {SparkSession#register_progress_handler} while a query runs.
  #
  # @!attribute [r] stage_id
  #   @return [Integer]
  # @!attribute [r] num_tasks
  #   @return [Integer]
  # @!attribute [r] num_completed_tasks
  #   @return [Integer]
  # @!attribute [r] num_bytes_read
  #   @return [Integer]
  # @!attribute [r] done
  #   @return [Boolean]
  StageInfo = Struct.new(:stage_id, :num_tasks, :num_completed_tasks, :num_bytes_read, :done, keyword_init: true)

  # Per-RPC timeouts (in seconds; `nil` disables the timeout for that RPC),
  # mirroring the Spark 4.3 clients' defaults. `ExecutePlan` (the query stream
  # itself) never has a deadline, since a timeout there would kill the query.
  #
  # @example
  #   SparkConnect::SparkConnectClient.new(cb, rpc_deadlines: SparkConnect::RpcDeadlines.new(analyze_plan: 120))
  RpcDeadlines = Struct.new(
    :analyze_plan, :config, :interrupt, :release_session, :clone_session, :get_status,
    keyword_init: true
  ) do
    # @return [RpcDeadlines] the Spark 4.3 defaults (1 hour for AnalyzePlan, 10 minutes otherwise).
    def self.defaults
      new(analyze_plan: 3600, config: 600, interrupt: 600, release_session: 600, clone_session: 600, get_status: 600)
    end

    # @return [RpcDeadlines] all per-RPC timeouts disabled.
    def self.disabled
      new
    end
  end

  # The low-level Spark Connect client. Wraps the gRPC stub and exposes the four
  # core RPC families used by the high-level API: {#execute_plan},
  # {#execute_command}, {#analyze}, and {#config}. Higher layers
  # ({SparkSession}, {DataFrame}) never touch the stub directly.
  #
  # Transient transport failures (e.g. `GRPC::Unavailable`) are retried with
  # exponential backoff and jitter before any response data has been observed.
  class SparkConnectClient
    Proto = SparkConnect::Proto

    # Accumulated result of an `ExecutePlan` stream.
    #
    # @!attribute [r] arrow_batches
    #   @return [Array<String>] each element is one Arrow IPC stream chunk.
    # @!attribute [r] schema
    #   @return [Spark::Connect::DataType, nil] result schema, if returned.
    # @!attribute [r] metrics
    #   @return [Spark::Connect::ExecutePlanResponse::Metrics, nil]
    # @!attribute [r] observed_metrics
    #   @return [Array] observed (named) metrics.
    # @!attribute [r] sql_command_result
    #   @return [Spark::Connect::Relation, nil] relation produced by a SQL command.
    ExecuteResult = Struct.new(
      :arrow_batches, :schema, :metrics, :observed_metrics, :sql_command_result, :row_count,
      :write_stream_result, :streaming_query_result, :streaming_manager_result, :checkpoint_relation,
      :pipeline_command_result, :pipeline_events, :operation_id
    )

    # @return [String] the client-side session id (UUID v4).
    attr_reader :session_id
    # @return [String] the user agent / client type.
    attr_reader :client_type
    # @return [ChannelBuilder]
    attr_reader :channel_builder
    # @return [RpcDeadlines]
    attr_reader :rpc_deadlines

    # @param channel_builder [ChannelBuilder]
    # @param session_id [String, nil] reuse a session id, otherwise generated.
    # @param max_retries [Integer]
    # @param retry_base_delay [Float] base backoff in seconds.
    # @param rpc_deadlines [RpcDeadlines] per-RPC timeouts.
    def initialize(channel_builder, session_id: nil, max_retries: 10, retry_base_delay: 0.05, max_retry_delay: 10.0,
                   rpc_deadlines: RpcDeadlines.defaults)
      @channel_builder = channel_builder
      @stub = channel_builder.build_stub
      @metadata = channel_builder.metadata
      @session_id = session_id || channel_builder.session_id || SecureRandom.uuid
      @client_type = channel_builder.user_agent
      @user_context = Proto::UserContext.new(user_id: channel_builder.user_id || "")
      @max_retries = max_retries
      @retry_base_delay = retry_base_delay
      @max_retry_delay = max_retry_delay
      @rpc_deadlines = rpc_deadlines
      @server_side_session_id = nil
      @tags = []
      @progress_handlers = []
      @closed = false
    end

    # Operation tags attached to every subsequent execution (used with
    # {#interrupt} `type: :tag`).
    # @return [Array<String>]
    attr_reader :tags

    # Add an operation tag. Tags must be non-empty and contain no commas.
    # @return [void]
    def add_tag(tag)
      tag = tag.to_s
      raise IllegalArgumentError, "Tag must not be empty" if tag.empty?
      raise IllegalArgumentError, "Tag must not contain ','" if tag.include?(",")

      @tags << tag unless @tags.include?(tag)
      nil
    end

    # Remove an operation tag. @return [void]
    def remove_tag(tag)
      @tags.delete(tag.to_s)
      nil
    end

    # Remove all operation tags. @return [void]
    def clear_tags
      @tags.clear
      nil
    end

    # ---- Progress handlers ---------------------------------------------------

    # Register a callable notified of execution progress (see {StageInfo}).
    # @param handler [#call]
    # @return [void]
    def register_progress_handler(handler)
      raise IllegalArgumentError, "progress handler must respond to #call" unless handler.respond_to?(:call)

      @progress_handlers << handler unless @progress_handlers.include?(handler)
      nil
    end

    # @return [void]
    def remove_progress_handler(handler)
      @progress_handlers.delete(handler)
      nil
    end

    # @return [void]
    def clear_progress_handlers
      @progress_handlers.clear
      nil
    end

    # ---- Core RPCs -----------------------------------------------------------

    # Execute a relation plan and accumulate the streamed response.
    #
    # @param relation [Spark::Connect::Relation]
    # @return [ExecuteResult]
    def execute_plan(relation)
      execute(PlanBuilder.root_plan(relation))
    end

    # Execute a command plan (side-effecting, e.g. write/SQL DML).
    #
    # @param command [Spark::Connect::Command]
    # @return [ExecuteResult]
    def execute_command(command)
      execute(PlanBuilder.command_plan(command))
    end

    # Execute a command and yield each raw `ExecutePlanResponse` as it arrives,
    # without accumulating (and without retrying, since responses may already
    # have been consumed). Used for long-running streams such as the streaming
    # query listener bus.
    #
    # @param command [Spark::Connect::Command]
    # @yieldparam response [Spark::Connect::ExecutePlanResponse]
    # @return [void]
    def stream_command(command, &)
      req = execute_request(PlanBuilder.command_plan(command), SecureRandom.uuid)
      @stub.execute_plan(req, metadata: @metadata).each(&)
      nil
    rescue GRPC::BadStatus => e
      raise translate_error(e)
    end

    # Run an `AnalyzePlan` request.
    #
    # @param analyze_kw [Hash] exactly one `analyze` oneof keyword, e.g.
    #   `schema:`, `explain:`, `tree_string:`, `is_local:`, `spark_version:`.
    # @return [Spark::Connect::AnalyzePlanResponse]
    def analyze(**analyze_kw)
      req = Proto::AnalyzePlanRequest.new(
        session_id: @session_id,
        user_context: @user_context,
        client_type: @client_type,
        **analyze_kw
      )
      with_retries { @stub.analyze_plan(req, **call_options(:analyze_plan)) }
    end

    # Run a `Config` request.
    #
    # @param operation [Spark::Connect::ConfigRequest::Operation]
    # @return [Spark::Connect::ConfigResponse]
    def config(operation)
      req = Proto::ConfigRequest.new(
        session_id: @session_id,
        user_context: @user_context,
        client_type: @client_type,
        operation: operation
      )
      with_retries { @stub.config(req, **call_options(:config)) }
    end

    # Interrupt running operations.
    #
    # @param type [Symbol] `:all`, `:tag`, or `:operation_id`.
    # @param value [String, nil] the tag or operation id when applicable.
    # @return [Spark::Connect::InterruptResponse]
    def interrupt(type: :all, value: nil)
      kw = { interrupt_type: :"INTERRUPT_TYPE_#{type.to_s.upcase}" }
      kw[:operation_tag] = value if type == :tag
      kw[:operation_id] = value if type == :operation_id
      req = Proto::InterruptRequest.new(
        session_id: @session_id, user_context: @user_context, client_type: @client_type, **kw
      )
      with_retries { @stub.interrupt(req, **call_options(:interrupt)) }
    end

    # The status of operations in this session (the `GetStatus` RPC, Spark 4.2+).
    #
    # @param operation_ids [Array<String>] operations to report; all operations
    #   of the session when empty.
    # @param operation_extensions [Array<Google::Protobuf::Any>] per-operation
    #   extension requests.
    # @param request_extensions [Array<Google::Protobuf::Any>] request-level extensions.
    # @return [Spark::Connect::GetStatusResponse] each `operation_statuses`
    #   entry has an `operation_id` and a `state` (e.g. `:OPERATION_STATE_RUNNING`).
    def get_operation_statuses(operation_ids = [], operation_extensions: [], request_extensions: [])
      req = Proto::GetStatusRequest.new(
        session_id: @session_id, user_context: @user_context, client_type: @client_type,
        operation_status: Proto::GetStatusRequest::OperationStatusRequest.new(
          operation_ids: Array(operation_ids).map(&:to_s), extensions: operation_extensions
        ),
        extensions: request_extensions
      )
      req.client_observed_server_side_session_id = @server_side_session_id if @server_side_session_id
      with_retries { @stub.get_status(req, **call_options(:get_status)) }
    end
    alias getOperationStatuses get_operation_statuses

    # Clone this client's server-side session (the `CloneSession` RPC, Spark
    # 4.1+) and return a client bound to the clone.
    #
    # @param new_session_id [String, nil] a UUID for the clone (generated when omitted).
    # @return [SparkConnectClient]
    def clone_session(new_session_id = nil)
      req = Proto::CloneSessionRequest.new(session_id: @session_id, user_context: @user_context, client_type: @client_type)
      req.new_session_id = new_session_id if new_session_id
      req.client_observed_server_side_session_id = @server_side_session_id if @server_side_session_id
      resp = with_retries { @stub.clone_session(req, **call_options(:clone_session)) }
      if new_session_id && resp.new_session_id != new_session_id
        raise SparkConnectError, "Server returned session id #{resp.new_session_id}, expected #{new_session_id}"
      end

      derive(resp.new_session_id)
    end

    # A client against the same endpoint bound to a fresh, empty server-side
    # session (no state is copied; no server round trip is made).
    # @return [SparkConnectClient]
    def new_session_client
      derive(nil)
    end

    # Release this client's server-side session.
    # @return [void]
    def release_session
      req = Proto::ReleaseSessionRequest.new(
        session_id: @session_id, user_context: @user_context, client_type: @client_type
      )
      # Best-effort and non-retrying: this runs on teardown, so a dead server
      # must not block the caller with the retry/backoff loop.
      @stub.release_session(req, **call_options(:release_session))
      nil
    rescue StandardError
      nil
    end

    # Close the gRPC channel. Idempotent.
    # @return [void]
    def close
      return nil if @closed

      @closed = true
      channel = @stub.instance_variable_get(:@ch)
      channel.close if channel.respond_to?(:close)
      nil
    rescue StandardError
      nil
    end

    # @return [Boolean] whether {#close} has been called.
    def closed?
      @closed
    end

    private

    def derive(session_id)
      SparkConnectClient.new(
        @channel_builder.with_session_id(session_id),
        max_retries: @max_retries, retry_base_delay: @retry_base_delay, max_retry_delay: @max_retry_delay,
        rpc_deadlines: @rpc_deadlines
      )
    end

    def call_options(rpc)
      opts = { metadata: @metadata }
      timeout = @rpc_deadlines&.public_send(rpc)
      opts[:deadline] = Time.now + timeout if timeout
      opts
    end

    def execute_request(plan, operation_id)
      Proto::ExecutePlanRequest.new(
        session_id: @session_id,
        user_context: @user_context,
        operation_id: operation_id,
        plan: plan,
        client_type: @client_type,
        tags: @tags
      )
    end

    def execute(plan)
      operation_id = SecureRandom.uuid
      req = execute_request(plan, operation_id)

      # Build the accumulator *inside* the retry block so that a mid-stream
      # failure (which restarts the gRPC stream from the beginning) starts from
      # a clean slate. Accumulating into a result created outside the block
      # would re-append already-seen batches and duplicate rows on retry.
      progress = ProgressTracker.new(@progress_handlers, operation_id)
      begin
        with_retries do
          result = ExecuteResult.new([], nil, nil, [], nil, 0)
          result.pipeline_events = []
          result.operation_id = operation_id
          responses = @stub.execute_plan(req, metadata: @metadata)
          responses.each do |resp|
            @server_side_session_id = resp.server_side_session_id unless resp.server_side_session_id.empty?
            progress.update(resp.execution_progress) if resp.execution_progress
            accumulate(result, resp)
          end
          result
        end
      ensure
        progress.finish
      end
    end

    def accumulate(result, resp)
      result.schema = resp.schema if resp.schema
      result.metrics = resp.metrics if resp.metrics
      result.observed_metrics += resp.observed_metrics.to_a unless resp.observed_metrics.empty?

      case resp.response_type
      when :arrow_batch
        batch = resp.arrow_batch
        result.arrow_batches << batch.data unless batch.data.empty?
        result.row_count += batch.row_count
      when :sql_command_result
        result.sql_command_result = resp.sql_command_result.relation
      when :write_stream_operation_start_result
        result.write_stream_result = resp.write_stream_operation_start_result
      when :streaming_query_command_result
        result.streaming_query_result = resp.streaming_query_command_result
      when :streaming_query_manager_command_result
        result.streaming_manager_result = resp.streaming_query_manager_command_result
      when :checkpoint_command_result
        result.checkpoint_relation = resp.checkpoint_command_result.relation
      when :pipeline_command_result
        result.pipeline_command_result = resp.pipeline_command_result
      when :pipeline_event_result
        result.pipeline_events << resp.pipeline_event_result.event
      end
    end

    def with_retries
      attempt = 0
      begin
        yield
      rescue GRPC::BadStatus => e
        if retryable?(e) && attempt < @max_retries
          delay = backoff(attempt)
          attempt += 1
          sleep(delay)
          retry
        end
        raise translate_error(e)
      end
    end

    RETRYABLE_CODES = [
      GRPC::Core::StatusCodes::UNAVAILABLE,
      GRPC::Core::StatusCodes::DEADLINE_EXCEEDED,
      GRPC::Core::StatusCodes::ABORTED,
      GRPC::Core::StatusCodes::RESOURCE_EXHAUSTED,
    ].freeze

    def retryable?(error)
      RETRYABLE_CODES.include?(error.code)
    end

    def backoff(attempt)
      delay = @retry_base_delay * (2**attempt)
      delay = [delay, @max_retry_delay].min
      delay + (rand * delay * 0.5)
    end

    def translate_error(error)
      message = error.respond_to?(:details) ? error.details : error.message
      code = grpc_code_name(error)
      klass =
        if /\[(PARSE_SYNTAX_ERROR|PARSE_)/.match?(message.to_s) || /ParseException/.match?(message.to_s)
          ParseError
        elsif /AnalysisException|UNRESOLVED_|TABLE_OR_VIEW_NOT_FOUND|\[.*\] /.match?(message.to_s)
          AnalysisError
        else
          SparkConnectError
        end
      error_class = message.to_s[/\[([A-Z0-9_.]+)\]/, 1]
      klass.new(message, error_class: error_class, grpc_code: code)
    end

    def grpc_code_name(error)
      GRPC::Core::StatusCodes.constants.find { |c| GRPC::Core::StatusCodes.const_get(c) == error.code }&.to_s
    end

    # Tracks `ExecutionProgress` updates of one execution and notifies the
    # registered progress handlers, like PySpark's `Progress` helper: on every
    # update that reports tasks, and once more with `done: true` at the end.
    # @api private
    class ProgressTracker
      def initialize(handlers, operation_id)
        @handlers = handlers.dup
        @operation_id = operation_id
        @stages = []
        @inflight = 0
      end

      def update(progress)
        stages = progress.stages.map do |s|
          StageInfo.new(stage_id: s.stage_id, num_tasks: s.num_tasks, num_completed_tasks: s.num_completed_tasks,
                        num_bytes_read: s.input_bytes_read, done: s.done)
        end
        return if stages.sum(&:num_tasks).zero?

        @stages = stages
        @inflight = progress.num_inflight_tasks
        notify(false)
      end

      def finish
        notify(true)
      end

      private

      def notify(done)
        @handlers.each do |h|
          h.call(stages: @stages, inflight_tasks: @inflight, operation_id: @operation_id, done: done)
        rescue StandardError => e
          warn("spark-connect: progress handler raised #{e.class}: #{e.message}")
        end
      end
    end
  end
end
