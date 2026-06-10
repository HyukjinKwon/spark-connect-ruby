# frozen_string_literal: true

module SparkConnect
  # A distributed, lazily-evaluated collection of rows organised into named
  # columns - the central abstraction of the DataFrame API.
  #
  # A {DataFrame} is immutable: every transformation ({#select}, {#filter},
  # {#join}, ...) returns a new {DataFrame} wrapping a new logical plan; nothing
  # is sent to the server until an action ({#collect}, {#show}, {#count}, ...) is
  # invoked.
  #
  # Method names are snake_case (Ruby idiom); camelCase aliases are provided for
  # the highest-traffic PySpark names (`groupBy`, `withColumn`, `orderBy`, ...).
  #
  # @example
  #   F = SparkConnect::F
  #   df = spark.range(100)
  #   df.filter(F.col("id") % 2 == 0)
  #     .select((F.col("id") * 10).alias("ten_x"))
  #     .order_by(F.col("ten_x").desc)
  #     .show(5)
  class DataFrame
    Proto = SparkConnect::Proto

    JOIN_TYPES = {
      inner: :JOIN_TYPE_INNER,
      cross: :JOIN_TYPE_CROSS,
      outer: :JOIN_TYPE_FULL_OUTER,
      full: :JOIN_TYPE_FULL_OUTER,
      fullouter: :JOIN_TYPE_FULL_OUTER,
      full_outer: :JOIN_TYPE_FULL_OUTER,
      left: :JOIN_TYPE_LEFT_OUTER,
      leftouter: :JOIN_TYPE_LEFT_OUTER,
      left_outer: :JOIN_TYPE_LEFT_OUTER,
      right: :JOIN_TYPE_RIGHT_OUTER,
      rightouter: :JOIN_TYPE_RIGHT_OUTER,
      right_outer: :JOIN_TYPE_RIGHT_OUTER,
      semi: :JOIN_TYPE_LEFT_SEMI,
      leftsemi: :JOIN_TYPE_LEFT_SEMI,
      left_semi: :JOIN_TYPE_LEFT_SEMI,
      anti: :JOIN_TYPE_LEFT_ANTI,
      leftanti: :JOIN_TYPE_LEFT_ANTI,
      left_anti: :JOIN_TYPE_LEFT_ANTI,
    }.freeze

    # @return [SparkSession]
    attr_reader :session
    # @return [Spark::Connect::Relation] the logical plan this DataFrame builds.
    attr_reader :relation

    # @param session [SparkSession]
    # @param relation [Spark::Connect::Relation]
    def initialize(session, relation)
      @session = session
      @relation = relation
    end

    # ---- Projection --------------------------------------------------------

    # Select a set of columns or expressions.
    #
    # @param cols [Array<Column, String, Symbol>]
    # @return [DataFrame]
    def select(*cols)
      exprs = normalize_columns(cols).map(&:to_expr)
      build(project: Proto::Project.new(input: @relation, expressions: exprs))
    end

    # Select using SQL expression strings.
    #
    # @param exprs [Array<String>]
    # @return [DataFrame]
    def select_expr(*exprs)
      parsed = exprs.flatten.map do |e|
        Proto::Expression.new(expression_string: Proto::Expression::ExpressionString.new(expression: e))
      end
      build(project: Proto::Project.new(input: @relation, expressions: parsed))
    end
    alias selectExpr select_expr

    # ---- Filtering ---------------------------------------------------------

    # Filter rows by a condition.
    #
    # @param condition [Column, String] a boolean column or SQL expression string.
    # @return [DataFrame]
    def filter(condition)
      cond = condition.is_a?(String) ? Functions.expr(condition) : condition
      build(filter: Proto::Filter.new(input: @relation, condition: cond.to_expr))
    end
    alias where filter

    # ---- Column manipulation ----------------------------------------------

    # Add or replace a single column.
    #
    # @param name [String]
    # @param col [Column]
    # @return [DataFrame]
    def with_column(name, col)
      with_columns(name => col)
    end
    alias withColumn with_column

    # Add or replace multiple columns.
    #
    # @param assigns [Hash{String=>Column}]
    # @return [DataFrame]
    def with_columns(assigns)
      aliases = assigns.map do |name, col|
        Proto::Expression::Alias.new(expr: Column.to_col(col).to_expr, name: [name.to_s])
      end
      build(with_columns: Proto::WithColumns.new(input: @relation, aliases: aliases))
    end
    alias withColumns with_columns

    # Rename a single column.
    # @return [DataFrame]
    def with_column_renamed(existing, new_name)
      with_columns_renamed(existing => new_name)
    end
    alias withColumnRenamed with_column_renamed

    # Rename multiple columns.
    #
    # @param renames [Hash{String=>String}]
    # @return [DataFrame]
    def with_columns_renamed(renames)
      pairs = renames.map do |old, new_name|
        Proto::WithColumnsRenamed::Rename.new(col_name: old.to_s, new_col_name: new_name.to_s)
      end
      build(with_columns_renamed: Proto::WithColumnsRenamed.new(input: @relation, renames: pairs))
    end
    alias withColumnsRenamed with_columns_renamed

    # Drop one or more columns (by name or {Column}).
    # @return [DataFrame]
    def drop(*cols)
      names = []
      columns = []
      cols.flatten.each do |c|
        case c
        when Column then columns << c.to_expr
        else names << c.to_s
        end
      end
      build(drop: Proto::Drop.new(input: @relation, columns: columns, column_names: names))
    end

    # Rename all columns positionally.
    # @return [DataFrame]
    def to_df(*names)
      build(to_df: Proto::ToDF.new(input: @relation, column_names: names.flatten.map(&:to_s)))
    end
    alias toDF to_df

    # Apply a {Types::StructType} (reconciling/casting columns to it).
    # @return [DataFrame]
    def to(schema)
      build(to_schema: Proto::ToSchema.new(input: @relation, schema: schema.to_proto))
    end

    # ---- Deduplication -----------------------------------------------------

    # Distinct rows.
    # @return [DataFrame]
    def distinct
      build(deduplicate: Proto::Deduplicate.new(input: @relation, all_columns_as_keys: true))
    end

    # Drop duplicate rows, optionally restricted to a subset of columns.
    #
    # @param subset [Array<String>, nil]
    # @return [DataFrame]
    def drop_duplicates(subset = nil)
      dedup =
        if subset.nil? || subset.empty?
          Proto::Deduplicate.new(input: @relation, all_columns_as_keys: true)
        else
          Proto::Deduplicate.new(input: @relation, column_names: Array(subset).map(&:to_s))
        end
      build(deduplicate: dedup)
    end
    alias dropDuplicates drop_duplicates
    alias drop_duplicates_within_watermark drop_duplicates

    # ---- Ordering ----------------------------------------------------------

    # Sort by the given columns (globally).
    #
    # @param cols [Array<Column, String>]
    # @return [DataFrame]
    def order_by(*cols)
      orders = normalize_columns(cols).map { |c| to_sort_order(c) }
      build(sort: Proto::Sort.new(input: @relation, order: orders, is_global: true))
    end
    alias sort order_by
    alias orderBy order_by

    # Sort within each partition (no global shuffle).
    # @return [DataFrame]
    def sort_within_partitions(*cols)
      orders = normalize_columns(cols).map { |c| to_sort_order(c) }
      build(sort: Proto::Sort.new(input: @relation, order: orders, is_global: false))
    end
    alias sortWithinPartitions sort_within_partitions

    # ---- Limiting ----------------------------------------------------------

    # @return [DataFrame] the first `n` rows.
    def limit(n)
      build(limit: Proto::Limit.new(input: @relation, limit: n))
    end

    # @return [DataFrame] all rows except the first `n`.
    def offset(n)
      build(offset: Proto::Offset.new(input: @relation, offset: n))
    end

    # ---- Grouping & aggregation -------------------------------------------

    # Group by the given columns.
    #
    # @param cols [Array<Column, String>]
    # @return [GroupedData]
    def group_by(*cols)
      GroupedData.new(self, normalize_columns(cols), :GROUP_TYPE_GROUPBY)
    end
    alias groupBy group_by
    alias groupby group_by

    # Multi-dimensional rollup.
    # @return [GroupedData]
    def rollup(*cols)
      GroupedData.new(self, normalize_columns(cols), :GROUP_TYPE_ROLLUP)
    end

    # Multi-dimensional cube.
    # @return [GroupedData]
    def cube(*cols)
      GroupedData.new(self, normalize_columns(cols), :GROUP_TYPE_CUBE)
    end

    # Aggregate over the whole DataFrame (a group-by with no grouping columns).
    #
    # @param exprs [Array<Column>, Hash]
    # @return [DataFrame]
    def agg(*exprs)
      group_by.agg(*exprs)
    end

    # ---- Joins -------------------------------------------------------------

    # Join with another DataFrame.
    #
    # @param other [DataFrame]
    # @param on [String, Array<String>, Column, nil] join key column name(s) or a
    #   boolean join condition.
    # @param how [Symbol, String] join type (see {JOIN_TYPES}).
    # @return [DataFrame]
    def join(other, on: nil, how: :inner)
      jt = JOIN_TYPES[how.to_s.downcase.to_sym] or
        raise IllegalArgumentError, "Unsupported join type: #{how}"
      j = Proto::Join.new(left: @relation, right: other.relation, join_type: jt)
      case on
      when nil then nil
      when Column then j.join_condition = on.to_expr
      when Array then j.using_columns += on.map(&:to_s)
      else j.using_columns << on.to_s
      end
      build(join: j)
    end

    # Cartesian product with another DataFrame.
    # @return [DataFrame]
    def cross_join(other)
      build(join: Proto::Join.new(left: @relation, right: other.relation, join_type: :JOIN_TYPE_CROSS))
    end
    alias crossJoin cross_join

    # ---- Set operations ----------------------------------------------------

    # Union (by position; keeps duplicates - equivalent to Spark's `unionAll`).
    # @return [DataFrame]
    def union(other)
      set_op(other, :SET_OP_TYPE_UNION, is_all: true)
    end
    alias union_all union
    alias unionAll union

    # Union by column name.
    #
    # @param other [DataFrame]
    # @param allow_missing_columns [Boolean]
    # @return [DataFrame]
    def union_by_name(other, allow_missing_columns: false)
      set_op(other, :SET_OP_TYPE_UNION, is_all: true, by_name: true, allow_missing_columns: allow_missing_columns)
    end
    alias unionByName union_by_name

    # Set intersection (distinct).
    # @return [DataFrame]
    def intersect(other)
      set_op(other, :SET_OP_TYPE_INTERSECT, is_all: false)
    end

    # Set intersection keeping duplicates.
    # @return [DataFrame]
    def intersect_all(other)
      set_op(other, :SET_OP_TYPE_INTERSECT, is_all: true)
    end
    alias intersectAll intersect_all

    # Rows in this DataFrame not in `other` (distinct).
    # @return [DataFrame]
    def except_all(other)
      set_op(other, :SET_OP_TYPE_EXCEPT, is_all: true)
    end
    alias exceptAll except_all

    # Rows in this DataFrame not in `other` (distinct) - Spark's `EXCEPT`.
    # @return [DataFrame]
    def subtract(other)
      set_op(other, :SET_OP_TYPE_EXCEPT, is_all: false)
    end

    # ---- Partitioning ------------------------------------------------------

    # Repartition into `num_partitions`, optionally hash-partitioned by columns.
    #
    # @param num_partitions [Integer]
    # @param cols [Array<Column, String>]
    # @return [DataFrame]
    def repartition(num_partitions, *cols)
      if cols.empty?
        build(repartition: Proto::Repartition.new(input: @relation, num_partitions: num_partitions, shuffle: true))
      else
        rbe = Proto::RepartitionByExpression.new(
          input: @relation, partition_exprs: normalize_columns(cols).map(&:to_expr), num_partitions: num_partitions
        )
        build(repartition_by_expression: rbe)
      end
    end

    # Reduce to `num_partitions` without a full shuffle.
    # @return [DataFrame]
    def coalesce(num_partitions)
      build(repartition: Proto::Repartition.new(input: @relation, num_partitions: num_partitions, shuffle: false))
    end

    # Range-partition by the given columns (rows are range-partitioned on the
    # sort order of the columns).
    #
    # @overload repartition_by_range(*cols)
    # @overload repartition_by_range(num_partitions, *cols)
    # @return [DataFrame]
    def repartition_by_range(*args)
      num_partitions = args.first.is_a?(Integer) ? args.shift : nil
      orders = normalize_columns(args).map do |c|
        expr = c.to_expr
        if expr.expr_type == :sort_order
          expr
        else
          Proto::Expression.new(sort_order: Proto::Expression::SortOrder.new(
            child: expr, direction: :SORT_DIRECTION_ASCENDING, null_ordering: :SORT_NULLS_FIRST
          ))
        end
      end
      rbe = Proto::RepartitionByExpression.new(input: @relation, partition_exprs: orders)
      rbe.num_partitions = num_partitions if num_partitions
      build(repartition_by_expression: rbe)
    end
    alias repartitionByRange repartition_by_range

    # ---- Sampling ----------------------------------------------------------

    # Random sample of rows.
    #
    # @param fraction [Float] expected fraction (0.0..1.0).
    # @param with_replacement [Boolean]
    # @param seed [Integer, nil]
    # @return [DataFrame]
    def sample(fraction, with_replacement: false, seed: nil)
      s = Proto::Sample.new(
        input: @relation, lower_bound: 0.0, upper_bound: fraction, with_replacement: with_replacement
      )
      s.seed = seed if seed
      build(sample: s)
    end

    # ---- Misc transforms ---------------------------------------------------

    # Alias this DataFrame (a subquery alias usable in join conditions).
    # @return [DataFrame]
    def alias(name)
      build(subquery_alias: Proto::SubqueryAlias.new(input: @relation, alias: name.to_s))
    end
    alias as alias

    # Attach a planner hint (e.g. `"broadcast"`).
    #
    # @param name [String]
    # @param params [Array]
    # @return [DataFrame]
    def hint(name, *params)
      h = Proto::Hint.new(input: @relation, name: name.to_s,
                          parameters: params.map { |p| Column.to_col(p).to_expr })
      build(hint: h)
    end

    # Unpivot (melt) columns from wide to long format.
    #
    # @param ids [Array<Column, String>] identifier columns.
    # @param values [Array<Column, String>, nil] value columns (nil = all others).
    # @param variable_column_name [String]
    # @param value_column_name [String]
    # @return [DataFrame]
    def unpivot(ids, values, variable_column_name, value_column_name)
      u = Proto::Unpivot.new(
        input: @relation,
        ids: normalize_columns(Array(ids)).map(&:to_expr),
        variable_column_name: variable_column_name,
        value_column_name: value_column_name
      )
      u.values = Proto::Unpivot::Values.new(values: normalize_columns(Array(values)).map(&:to_expr)) unless values.nil?
      build(unpivot: u)
    end
    alias melt unpivot

    # ---- NA / stat / IO facades -------------------------------------------

    # @return [DataFrameNaFunctions] missing-data helpers (`drop`, `fill`, `replace`).
    def na
      DataFrameNaFunctions.new(self)
    end

    # @return [DataFrameStatFunctions] statistical helpers.
    def stat
      DataFrameStatFunctions.new(self)
    end

    # @return [DataFrameWriter] interface for saving this DataFrame.
    def write
      DataFrameWriter.new(self)
    end

    # @return [DataFrameWriterV2] the v2 (catalog) write interface.
    def write_to(table)
      DataFrameWriterV2.new(self, table)
    end
    alias writeTo write_to

    # @return [DataStreamWriter] interface for starting a streaming query from
    #   this (streaming) DataFrame.
    def write_stream
      DataStreamWriter.new(self)
    end
    alias writeStream write_stream

    # Define an event-time watermark for late-data handling on a streaming
    # DataFrame.
    #
    # @param event_time [String] the event-time column name.
    # @param delay_threshold [String] e.g. `"10 minutes"`.
    # @return [DataFrame]
    def with_watermark(event_time, delay_threshold)
      build(with_watermark: Proto::WithWatermark.new(
        input: @relation, event_time: event_time.to_s, delay_threshold: delay_threshold.to_s
      ))
    end
    alias withWatermark with_watermark

    # Apply a function to this DataFrame and return its result, enabling a
    # fluent chain of custom transformations.
    #
    # @yieldparam df [DataFrame] self
    # @return [DataFrame] whatever the block returns
    def transform
      yield(self)
    end

    # Eagerly checkpoint this DataFrame: materialise it server-side and return a
    # new DataFrame backed by the cached result (truncates the logical plan).
    #
    # @param eager [Boolean] materialise immediately.
    # @return [DataFrame]
    def checkpoint(eager: true)
      checkpoint_command(local: false, eager: eager)
    end

    # Like {#checkpoint} but uses the executors' local storage (no reliable
    # storage), which is faster but not fault-tolerant.
    #
    # @param eager [Boolean]
    # @return [DataFrame]
    def local_checkpoint(eager: true)
      checkpoint_command(local: true, eager: eager)
    end
    alias localCheckpoint local_checkpoint

    # Observe named metrics over this DataFrame.
    #
    # @param name [String, Observation]
    # @param exprs [Array<Column>]
    # @return [DataFrame]
    def observe(name, *exprs)
      obs_name = name.is_a?(Observation) ? name.name : name.to_s
      cm = Proto::CollectMetrics.new(
        input: @relation, name: obs_name, metrics: exprs.map { |e| Column.to_col(e).to_expr }
      )
      df = build(collect_metrics: cm)
      name.bind(df) if name.is_a?(Observation)
      df
    end

    # ---- Schema introspection ---------------------------------------------

    # @return [Types::StructType] the DataFrame's schema.
    def schema
      @schema ||= Types.from_proto(analyze(schema: Proto::AnalyzePlanRequest::Schema.new(plan: plan)).schema.schema)
    end

    # @return [Array<String>] column names.
    def columns
      schema.names
    end

    # @return [Array<Array(String, String)>] (name, simpleString-type) pairs.
    def dtypes
      schema.fields.map { |f| [f.name, f.data_type.simple_string] }
    end

    # @return [Array<Column>] one {Column} per output column.
    def column_objects
      columns.map { |c| Functions.col(c) }
    end

    # Print the schema as an indented tree to `io`.
    # @return [void]
    def print_schema(io = $stdout)
      io.puts(schema.tree_string)
    end
    alias printSchema print_schema

    # Index into a column by name (`df["id"]`) or position (`df[0]`).
    #
    # @param key [String, Symbol, Integer]
    # @return [Column]
    def [](key)
      case key
      when Integer then Functions.col(columns[key])
      else Functions.col(key.to_s)
      end
    end

    # Allows `df.column_name` for valid identifier column names.
    def method_missing(name, *args)
      if args.empty? && columns.include?(name.to_s)
        Functions.col(name.to_s)
      else
        super
      end
    end

    def respond_to_missing?(name, include_private = false)
      begin
        columns.include?(name.to_s)
      rescue StandardError
        false
      end || super
    end

    # ---- Actions -----------------------------------------------------------

    # Execute the plan and return all rows.
    # @return [Array<Row>]
    def collect
      result = @session.client.execute_plan(@relation)
      ArrowConverter.to_rows(result.arrow_batches)
    end
    alias to_a collect

    # @return [Array<Row>] the first `n` rows.
    def take(n)
      limit(n).collect
    end

    # @return [Array<Row>, Row] `n` rows (array) or the single first row when called with no arg.
    def head(n = nil)
      return first if n.nil?

      take(n)
    end

    # @return [Row, nil] the first row.
    def first
      take(1).first
    end

    # @return [Integer] the number of rows.
    def count
      df = build(aggregate: Proto::Aggregate.new(
        input: @relation,
        group_type: :GROUP_TYPE_GROUPBY,
        grouping_expressions: [],
        aggregate_expressions: [Column.invoke("count", Column.lit(1)).to_expr]
      ))
      row = df.collect.first
      row ? row[0] : 0
    end

    # @return [Boolean] whether the DataFrame has no rows.
    def empty?
      limit(1).collect.empty?
    end
    alias is_empty empty?

    # Render the first `n` rows as a formatted table.
    #
    # @param n [Integer]
    # @param truncate [Boolean, Integer] truncate long values to 20 chars (true)
    #   or to the given width (Integer).
    # @param vertical [Boolean]
    # @return [void]
    def show(n = 20, truncate: true, vertical: false)
      $stdout.puts(show_string(n, truncate: truncate, vertical: vertical))
    end

    # @return [String] the formatted table string (what {#show} prints).
    def show_string(n = 20, truncate: true, vertical: false)
      trunc = if truncate == true
                20
              else
                (truncate == false ? 0 : truncate.to_i)
              end
      ss = Proto::ShowString.new(input: @relation, num_rows: n, truncate: trunc, vertical: vertical)
      df = build(show_string: ss)
      df.collect.first&.[](0).to_s
    end

    # Materialise the result as an Arrow {Arrow::Table} (columnar).
    # @return [Arrow::Table, nil]
    def to_arrow
      result = @session.client.execute_plan(@relation)
      ArrowConverter.to_table(result.arrow_batches)
    end

    # @return [Array<Hash>] all rows as Hashes.
    def to_h_array
      collect.map(&:to_h)
    end

    # ---- Explain / metadata ------------------------------------------------

    # Return the query plan as a string.
    #
    # @param mode [Symbol] `:simple`, `:extended`, `:codegen`, `:cost`, `:formatted`.
    # @return [String]
    def explain_string(mode = :simple)
      em = :"EXPLAIN_MODE_#{mode.to_s.upcase}"
      analyze(explain: Proto::AnalyzePlanRequest::Explain.new(plan: plan, explain_mode: em)).explain.explain_string
    end

    # Print the query plan.
    # @return [void]
    def explain(mode = :simple)
      $stdout.puts(explain_string(mode))
    end

    # @return [Array<String>] the input files backing this DataFrame.
    def input_files
      analyze(input_files: Proto::AnalyzePlanRequest::InputFiles.new(plan: plan)).input_files.files.to_a
    end

    # @return [Boolean] whether the data is small enough to be local.
    def local?
      analyze(is_local: Proto::AnalyzePlanRequest::IsLocal.new(plan: plan)).is_local.is_local
    end

    # @return [Boolean] whether this is a streaming DataFrame.
    def streaming?
      analyze(is_streaming: Proto::AnalyzePlanRequest::IsStreaming.new(plan: plan)).is_streaming.is_streaming
    end

    # @return [Boolean] whether `other` has the same logical plan.
    def same_semantics?(other)
      analyze(same_semantics: Proto::AnalyzePlanRequest::SameSemantics.new(
        target_plan: plan, other_plan: other.plan
      )).same_semantics.result
    end

    # @return [Integer] a hash of the logical plan.
    def semantic_hash
      analyze(semantic_hash: Proto::AnalyzePlanRequest::SemanticHash.new(plan: plan)).semantic_hash.result
    end

    # @api private - the executable plan rooted at this relation.
    # @return [Spark::Connect::Plan]
    def plan
      PlanBuilder.root_plan(@relation)
    end

    def to_s
      "#<SparkConnect::DataFrame>"
    end
    alias inspect to_s

    # @api private - build a derived DataFrame from a relation built by W-owned
    #   facades (GroupedData, NaFunctions, ...).
    def build(**rel)
      DataFrame.new(@session, PlanBuilder.relation(@session, **rel))
    end

    private

    def analyze(**kw)
      @session.client.analyze(**kw)
    end

    def checkpoint_command(local:, eager:)
      cmd = Proto::CheckpointCommand.new(relation: @relation, local: local, eager: eager)
      result = @session.client.execute_command(Proto::Command.new(checkpoint_command: cmd))
      cached = result.checkpoint_relation
      raise SparkConnectError, "Server did not return a checkpointed relation" unless cached

      relation = Proto::Relation.new(
        common: Proto::RelationCommon.new(plan_id: @session.next_plan_id),
        cached_remote_relation: Proto::CachedRemoteRelation.new(relation_id: cached.relation_id)
      )
      DataFrame.new(@session, relation)
    end

    def normalize_columns(cols)
      cols.flatten.map { |c| c.is_a?(Column) ? c : Functions.col(c.to_s) }
    end

    def to_sort_order(col)
      expr = col.to_expr
      if expr.expr_type == :sort_order
        expr.sort_order
      else
        Proto::Expression::SortOrder.new(
          child: expr, direction: :SORT_DIRECTION_ASCENDING, null_ordering: :SORT_NULLS_FIRST
        )
      end
    end

    def set_op(other, type, is_all:, by_name: false, allow_missing_columns: false)
      op = Proto::SetOperation.new(
        left_input: @relation, right_input: other.relation, set_op_type: type,
        is_all: is_all, by_name: by_name, allow_missing_columns: allow_missing_columns
      )
      build(set_op: op)
    end
  end
end
