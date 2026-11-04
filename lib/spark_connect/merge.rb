# frozen_string_literal: true

module SparkConnect
  # Builds and runs a `MERGE INTO` command from a source {DataFrame}. Returned by
  # {DataFrame#merge_into}. Mirrors PySpark's `MergeIntoWriter` (Spark 4.0+).
  #
  # Each `when_*` clause returns a small builder whose action method
  # (`update`, `update_all`, `delete`, `insert`, `insert_all`) records the
  # action and returns this writer, so clauses chain:
  #
  # @example
  #   source.merge_into("target", F.expr("target.id = source.id"))
  #         .when_matched(F.expr("source.deleted")).delete
  #         .when_matched.update("value" => F.col("source.value"))
  #         .when_not_matched.insert_all
  #         .when_not_matched_by_source.delete
  #         .merge
  class MergeIntoWriter
    Proto = SparkConnect::Proto
    MA = Proto::MergeAction

    # @param df [DataFrame] the source.
    # @param table [String] the target table.
    # @param condition [Column] the merge condition.
    def initialize(df, table, condition)
      raise IllegalArgumentError, "merge condition must be a Column, got #{condition.class}" unless condition.is_a?(Column)

      @df = df
      @table = table.to_s
      @condition = condition
      @schema_evolution = false
      @matched = []
      @not_matched = []
      @not_matched_by_source = []
    end

    # Actions for target rows that match a source row.
    # @param condition [Column, nil] an additional condition for this clause.
    # @return [WhenMatched]
    def when_matched(condition = nil) = WhenMatched.new(self, condition)
    alias whenMatched when_matched

    # Actions for source rows without a matching target row.
    # @param condition [Column, nil]
    # @return [WhenNotMatched]
    def when_not_matched(condition = nil) = WhenNotMatched.new(self, condition)
    alias whenNotMatched when_not_matched

    # Actions for target rows without a matching source row.
    # @param condition [Column, nil]
    # @return [WhenNotMatchedBySource]
    def when_not_matched_by_source(condition = nil) = WhenNotMatchedBySource.new(self, condition)
    alias whenNotMatchedBySource when_not_matched_by_source

    # Let the merge evolve the target schema (add source columns).
    # @return [self]
    def with_schema_evolution
      @schema_evolution = true
      self
    end
    alias withSchemaEvolution with_schema_evolution

    # Run the merge. @return [void]
    def merge
      expr = ->(action) { Proto::Expression.new(merge_action: action) }
      cmd = Proto::MergeIntoTableCommand.new(
        target_table_name: @table, source_table_plan: @df.relation, merge_condition: @condition.to_expr,
        match_actions: @matched.map(&expr), not_matched_actions: @not_matched.map(&expr),
        not_matched_by_source_actions: @not_matched_by_source.map(&expr),
        with_schema_evolution: @schema_evolution
      )
      @df.session.client.execute_command(Proto::Command.new(merge_into_table_command: cmd))
      nil
    end

    # @api private
    def add_action(kind, type, condition, assignments = nil)
      action = MA.new(action_type: type)
      action.condition = condition.to_expr if condition
      if assignments
        action.assignments += assignments.map do |key, value|
          MA::Assignment.new(key: Functions.expr(key.to_s).to_expr, value: Column.to_col(value).to_expr)
        end
      end
      instance_variable_get(:"@#{kind}") << action
      self
    end

    # Clause builder returned by {MergeIntoWriter#when_matched}.
    class WhenMatched
      def initialize(writer, condition)
        @writer = writer
        @condition = condition
      end

      # Update every column of the matched target row from the source row.
      # @return [MergeIntoWriter]
      def update_all = @writer.add_action(:matched, :ACTION_TYPE_UPDATE_STAR, @condition)
      alias updateAll update_all

      # @param assignments [Hash{String=>Column}] target column => new value.
      # @return [MergeIntoWriter]
      def update(assignments) = @writer.add_action(:matched, :ACTION_TYPE_UPDATE, @condition, assignments)

      # Delete the matched target row. @return [MergeIntoWriter]
      def delete = @writer.add_action(:matched, :ACTION_TYPE_DELETE, @condition)
    end

    # Clause builder returned by {MergeIntoWriter#when_not_matched}.
    class WhenNotMatched
      def initialize(writer, condition)
        @writer = writer
        @condition = condition
      end

      # Insert the source row with all its columns. @return [MergeIntoWriter]
      def insert_all = @writer.add_action(:not_matched, :ACTION_TYPE_INSERT_STAR, @condition)
      alias insertAll insert_all

      # @param assignments [Hash{String=>Column}] target column => value.
      # @return [MergeIntoWriter]
      def insert(assignments) = @writer.add_action(:not_matched, :ACTION_TYPE_INSERT, @condition, assignments)
    end

    # Clause builder returned by {MergeIntoWriter#when_not_matched_by_source}.
    class WhenNotMatchedBySource
      def initialize(writer, condition)
        @writer = writer
        @condition = condition
      end

      # @return [MergeIntoWriter]
      def update_all = @writer.add_action(:not_matched_by_source, :ACTION_TYPE_UPDATE_STAR, @condition)
      alias updateAll update_all

      # @param assignments [Hash{String=>Column}] target column => new value.
      # @return [MergeIntoWriter]
      def update(assignments) = @writer.add_action(:not_matched_by_source, :ACTION_TYPE_UPDATE, @condition, assignments)

      # Delete the unmatched target row. @return [MergeIntoWriter]
      def delete = @writer.add_action(:not_matched_by_source, :ACTION_TYPE_DELETE, @condition)
    end
  end
end
