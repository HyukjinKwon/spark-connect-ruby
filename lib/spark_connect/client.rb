# frozen_string_literal: true

require "securerandom"

module SparkConnect
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
      :write_stream_result, :streaming_query_result, :streaming_manager_result, :checkpoint_relation
    )

    # @return [String] the client-side session id (UUID v4).
    attr_reader :session_id
    # @return [String] the user agent / client type.
    attr_reader :client_type
    # @return [ChannelBuilder]
    attr_reader :channel_builder

    # @param channel_builder [ChannelBuilder]
    # @param session_id [String, nil] reuse a session id, otherwise generated.
    # @param max_retries [Integer]
    # @param retry_base_delay [Float] base backoff in seconds.
    def initialize(channel_builder, session_id: nil, max_retries: 10, retry_base_delay: 0.05, max_retry_delay: 10.0)
      @channel_builder = channel_builder
      @stub = channel_builder.build_stub
      @metadata = channel_builder.metadata
      @session_id = session_id || channel_builder.session_id || SecureRandom.uuid
      @client_type = channel_builder.user_agent
      @user_context = Proto::UserContext.new(user_id: channel_builder.user_id || "")
      @max_retries = max_retries
      @retry_base_delay = retry_base_delay
      @max_retry_delay = max_retry_delay
      @server_side_session_id = nil
    end

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
      with_retries { @stub.analyze_plan(req, metadata: @metadata) }
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
      with_retries { @stub.config(req, metadata: @metadata) }
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
      with_retries { @stub.interrupt(req, metadata: @metadata) }
    end

    # Release this client's server-side session.
    # @return [void]
    def release_session
      req = Proto::ReleaseSessionRequest.new(
        session_id: @session_id, user_context: @user_context, client_type: @client_type
      )
      # Best-effort and non-retrying: this runs on teardown, so a dead server
      # must not block the caller with the retry/backoff loop.
      @stub.release_session(req, metadata: @metadata)
      nil
    rescue StandardError
      nil
    end

    private

    def execute(plan)
      operation_id = SecureRandom.uuid
      req = Proto::ExecutePlanRequest.new(
        session_id: @session_id,
        user_context: @user_context,
        operation_id: operation_id,
        plan: plan,
        client_type: @client_type
      )

      result = ExecuteResult.new([], nil, nil, [], nil, 0)
      with_retries do
        responses = @stub.execute_plan(req, metadata: @metadata)
        responses.each do |resp|
          @server_side_session_id = resp.server_side_session_id unless resp.server_side_session_id.empty?
          accumulate(result, resp)
        end
      end
      result
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
  end
end
