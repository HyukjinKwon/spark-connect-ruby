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

    # When the expressions of `relation` (not of its child relations, which
    # were wrapped when they were built) reference subquery DataFrames
    # ({DataFrame#scalar}, {DataFrame#exists}, {Column#isin} with a DataFrame,
    # {DataFrame#as_table}), wrap it in a `WithRelations` that carries those
    # subquery plans, like PySpark:
    #
    #     with_relations [new plan id]
    #       root: relation
    #       references: [subquery plans...]
    #
    # @param session [SparkSession] the session that registered the subqueries.
    # @param relation [Spark::Connect::Relation]
    # @return [Spark::Connect::Relation]
    def with_subquery_references(session, relation)
      ids = []
      body = relation.rel_type && relation.public_send(relation.rel_type)
      collect_subquery_plan_ids(body, ids) if body
      return relation if ids.empty?

      references = ids.uniq.map do |id|
        session.subquery_relation(id) or
          raise IllegalArgumentError, "Subquery DataFrame (plan id #{id}) does not belong to this SparkSession"
      end
      Proto::Relation.new(
        common: Proto::RelationCommon.new(plan_id: session.next_plan_id),
        with_relations: Proto::WithRelations.new(root: relation, references: references)
      )
    end

    # @api private
    def collect_subquery_plan_ids(msg, acc)
      return acc if msg.is_a?(Proto::Relation)

      acc << msg.plan_id if msg.is_a?(Proto::SubqueryExpression)
      msg.class.descriptor.each do |field|
        next unless field.type == :message

        value = msg[field.name]
        case value
        when Google::Protobuf::RepeatedField then value.each { |v| collect_subquery_plan_ids(v, acc) }
        when Google::Protobuf::Map
          # Google::Protobuf::Map has no #each_value; iterate its message values.
          value.values.grep(Google::Protobuf::MessageExts).each { |v| collect_subquery_plan_ids(v, acc) }
        when nil then nil
        else collect_subquery_plan_ids(value, acc)
        end
      end
      acc
    end
  end
end
