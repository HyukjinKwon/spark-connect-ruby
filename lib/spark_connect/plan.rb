# frozen_string_literal: true

module SparkConnect
  # Low-level helpers for assembling the protobuf logical plan that the client
  # sends to the server. {DataFrame} and {SparkSession} build relations through
  # these helpers so that every relation carries a unique `plan_id` (used by the
  # server to resolve columns to a specific subtree, e.g. for self-joins).
  module PlanBuilder
    Proto = SparkConnect::Proto

    module_function

    # Wrap a `rel_type` oneof keyword into a {Spark::Connect::Relation},
    # attaching a fresh `plan_id` from `id_source`.
    #
    # @param id_source [#next_plan_id] usually a {SparkSession}.
    # @param rel [Hash] exactly one `rel_type` keyword, e.g. `project:`.
    # @return [Spark::Connect::Relation]
    def relation(id_source, **rel)
      Proto::Relation.new(common: Proto::RelationCommon.new(plan_id: id_source.next_plan_id), **rel)
    end

    # Wrap a relation as the root of an executable {Spark::Connect::Plan}.
    #
    # @param relation [Spark::Connect::Relation]
    # @return [Spark::Connect::Plan]
    def root_plan(relation)
      Proto::Plan.new(root: relation)
    end

    # Wrap a command as an executable {Spark::Connect::Plan}.
    #
    # @param command [Spark::Connect::Command]
    # @return [Spark::Connect::Plan]
    def command_plan(command)
      Proto::Plan.new(command: command)
    end
  end
end
