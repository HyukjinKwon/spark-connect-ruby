# frozen_string_literal: true

# Shared helpers for the ML unit specs. Nothing here touches a server: the
# session talks to a {MlSpecSupport::FakeMlClient}, which records every command
# and relation and answers with canned `MlCommandResult`s.
module MlSpecSupport
  Proto = SparkConnect::Proto

  # A stand-in for {SparkConnect::SparkConnectClient} for ML commands.
  #
  # Responses come from {#responder}, a callable receiving the
  # `MlCommand` and returning the `MlCommandResult` (or nil). The default
  # responder hands out `model-N` ids for fits, `0.5` for evaluations, and an
  # empty param for everything else.
  class FakeMlClient
    attr_reader :commands, :relations, :session_id
    attr_accessor :spark_version, :responder

    def initialize(spark_version: "4.3.0")
      @spark_version = spark_version
      @commands = []
      @relations = []
      @session_id = "ml-test-session"
      @fit_count = 0
      @responder = method(:default_response)
      @closed = false
    end

    # @return [Array<Proto::MlCommand>] the ML commands executed, in order.
    def ml_commands = @commands.map(&:ml_command)

    # @return [Proto::MlCommand] the last ML command executed.
    def last_ml_command = ml_commands.last

    def execute_command(command)
      @commands << command
      result = SparkConnect::SparkConnectClient::ExecuteResult.new([], nil, nil, [], nil, 0)
      result.ml_command_result = command.command_type == :ml_command ? @responder.call(command.ml_command) : nil
      result
    end

    def execute_plan(relation)
      @relations << relation
      SparkConnect::SparkConnectClient::ExecuteResult.new([], nil, nil, [], nil, 0)
    end

    def analyze(**kw)
      raise NotImplementedError, "FakeMlClient does not answer #{kw.keys.first}" unless kw.key?(:spark_version)

      Proto::AnalyzePlanResponse.new(session_id: session_id,
                                     spark_version: Proto::AnalyzePlanResponse::SparkVersion.new(version: @spark_version))
    end

    def config(_operation) = nil
    def release_session = nil
    def close = (@closed = true)
    def closed? = @closed
    def new_session_client = self
    def register_progress_handler(_handler) = nil
    def remove_progress_handler(_handler) = nil
    def clear_progress_handlers = nil
    def add_tag(_tag) = nil

    private

    def default_response(ml)
      case ml.command
      when :fit
        @fit_count += 1
        Proto::MlCommandResult.new(operator_info: Proto::MlCommandResult::MlOperatorInfo.new(
          obj_ref: Proto::ObjectRef.new(id: "model-#{@fit_count}"), uid: ml.fit.estimator.uid
        ))
      when :evaluate
        Proto::MlCommandResult.new(param: SparkConnect::ML::Serialize.literal(0.5))
      else
        Proto::MlCommandResult.new(param: SparkConnect::ML::Serialize.literal(nil))
      end
    end
  end

  # A session backed by a {FakeMlClient}.
  # @return [SparkConnect::SparkSession]
  def ml_session(client = FakeMlClient.new)
    SparkConnect::SparkSession.new(client)
  end

  # A DataFrame with `n` rows of an `id` column (no server needed).
  def ml_frame(session, n = 4)
    session.range(n)
  end

  # The `MlCommand` built for the first command sent by `client`, optionally
  # filtered by its oneof case.
  def first_ml(client, kind = nil)
    client.ml_commands.find { |m| kind.nil? || m.command == kind }
  end
end
