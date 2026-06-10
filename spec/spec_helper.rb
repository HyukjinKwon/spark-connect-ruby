# frozen_string_literal: true

require "spark-connect"

# Load shared support files (e.g. the integration-suite live-session helper).
# These are safe to load for the unit suite: they only register behaviour for
# examples tagged `:integration`, of which there are none in the unit run.
Dir[File.join(__dir__, "integration", "*_helper.rb")].each { |file| require file }

# Shared helpers for the unit suite. Unit specs never touch the network: they
# either assert on the protobuf plan a DataFrame builds, or run actions against
# a {SpecHelpers::FakeClient} that returns canned Arrow data.
module SpecHelpers
  Proto = SparkConnect::Proto

  # A drop-in replacement for {SparkConnect::SparkConnectClient} that records the
  # plans/commands/requests it receives and returns configurable canned results.
  class FakeClient
    attr_accessor :rows, :schema, :spark_version
    attr_reader :executed_relations, :executed_commands, :analyze_requests, :config_operations

    def initialize(rows: nil, schema: nil)
      @rows = rows
      @schema = schema
      @spark_version = "4.0.0"
      @executed_relations = []
      @executed_commands = []
      @analyze_requests = []
      @config_operations = []
      @tags = []
      @interrupts = []
    end

    def session_id = "test-session-id"

    def last_relation = @executed_relations.last
    def last_command = @executed_commands.last

    def execute_plan(relation)
      @executed_relations << relation
      build_result
    end

    def execute_command(command)
      @executed_commands << command
      result = build_result
      case command.command_type
      when :write_stream_operation_start
        result.write_stream_result = Proto::WriteStreamOperationStartResult.new(
          query_id: Proto::StreamingQueryInstanceId.new(id: "test-query-id", run_id: "test-run-id"),
          name: command.write_stream_operation_start.query_name
        )
      when :checkpoint_command
        result.checkpoint_relation = Proto::CachedRemoteRelation.new(relation_id: "test-relation-id")
      end
      result
    end

    def analyze(**kw)
      @analyze_requests << kw
      key = kw.keys.first
      result =
        case key
        when :schema then Proto::AnalyzePlanResponse::Schema.new(schema: (@schema || default_schema).to_proto)
        when :spark_version then Proto::AnalyzePlanResponse::SparkVersion.new(version: @spark_version)
        when :explain then Proto::AnalyzePlanResponse::Explain.new(explain_string: "== Physical Plan ==")
        when :tree_string then Proto::AnalyzePlanResponse::TreeString.new(tree_string: "root")
        when :is_local then Proto::AnalyzePlanResponse::IsLocal.new(is_local: true)
        when :is_streaming then Proto::AnalyzePlanResponse::IsStreaming.new(is_streaming: false)
        when :input_files then Proto::AnalyzePlanResponse::InputFiles.new(files: [])
        when :semantic_hash then Proto::AnalyzePlanResponse::SemanticHash.new(result: 42)
        when :same_semantics then Proto::AnalyzePlanResponse::SameSemantics.new(result: true)
        else Proto::AnalyzePlanResponse::IsLocal.new(is_local: true) # rubocop:disable Lint/DuplicateBranch
        end
      Proto::AnalyzePlanResponse.new(session_id: session_id, key => result)
    end

    def config(operation)
      @config_operations << operation
      Proto::ConfigResponse.new(session_id: session_id, pairs: [Proto::KeyValue.new(key: "k", value: "v")])
    end

    def interrupt(type: :all, value: nil)
      @interrupts << { type: type, value: value }
      Proto::InterruptResponse.new(session_id: session_id, interrupted_ids: ["op-1"])
    end

    def release_session = nil

    # --- operation tags (mirrors SparkConnectClient) ---
    attr_reader :tags, :interrupts

    def add_tag(tag)
      @tags << tag.to_s unless @tags.include?(tag.to_s)
    end

    def remove_tag(tag) = @tags.delete(tag.to_s)
    def clear_tags = @tags.clear

    # A real (non-connecting) channel builder so new_session can build a client.
    def channel_builder
      @channel_builder ||= SparkConnect::ChannelBuilder.new("sc://localhost:15002")
    end

    private

    def build_result
      batches = @rows && @schema ? [SparkConnect::ArrowConverter.from_rows(@rows, @schema)] : []
      SparkConnect::SparkConnectClient::ExecuteResult.new(
        batches, @schema&.to_proto, nil, [], nil, @rows ? @rows.size : 0
      )
    end

    def default_schema
      SparkConnect::Types.struct(SparkConnect::Types.field("id", SparkConnect::Types.long))
    end
  end

  # A session backed by a {FakeClient} (no network).
  # @return [SparkConnect::SparkSession]
  def fake_session(client = FakeClient.new)
    SparkConnect::SparkSession.new(client)
  end

  # The active fake client for the current spec.
  # @return [FakeClient]
  def fake_client
    @fake_client ||= FakeClient.new
  end

  # The session wrapping {#fake_client}.
  def spark
    @spark ||= fake_session(fake_client)
  end

  # The rel_type oneof symbol of a DataFrame's (or Relation's) top relation.
  def rel_type(df_or_rel)
    rel = df_or_rel.respond_to?(:relation) ? df_or_rel.relation : df_or_rel
    rel.rel_type
  end

  # The sub-message of a DataFrame's top relation (e.g. the Project message).
  def rel_body(df_or_rel)
    rel = df_or_rel.respond_to?(:relation) ? df_or_rel.relation : df_or_rel
    rel.public_send(rel.rel_type)
  end

  # Shorthand for the functions module.
  def f
    SparkConnect::F
  end
end

RSpec.configure do |config|
  config.include SpecHelpers
  config.expect_with(:rspec) { |c| c.syntax = :expect }
  config.mock_with(:rspec) { |c| c.verify_partial_doubles = true }
  config.disable_monkey_patching!
  config.order = :random
  Kernel.srand(config.seed)
end
