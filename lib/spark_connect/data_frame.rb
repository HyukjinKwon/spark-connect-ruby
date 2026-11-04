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

    # Join types accepted by {#lateral_join}.
    LATERAL_JOIN_TYPES = {
      inner: :JOIN_TYPE_INNER, left: :JOIN_TYPE_LEFT_OUTER, leftouter: :JOIN_TYPE_LEFT_OUTER, cross: :JOIN_TYPE_CROSS,
    }.freeze

    # {#nearest_by_join} argument domains (mirroring Spark's `NearestByJoinValidation`).
    NEAREST_BY_JOIN_MAX_NUM_RESULTS = 100_000
    NEAREST_BY_JOIN_JOIN_TYPES = %w[inner leftouter left].freeze
    NEAREST_BY_JOIN_MODES = %w[approx exact].freeze
    NEAREST_BY_JOIN_DIRECTIONS = %w[distance similarity].freeze

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

    # Drop duplicate rows within the event-time watermark, optionally restricted
    # to a subset of columns. Unlike {#drop_duplicates}, this is watermark-aware
    # and is intended for streaming DataFrames (mirrors PySpark's
    # `dropDuplicatesWithinWatermark`).
    #
    # @param subset [Array<String>, nil]
    # @return [DataFrame]
    def drop_duplicates_within_watermark(subset = nil)
      dedup =
        if subset.nil? || subset.empty?
          Proto::Deduplicate.new(input: @relation, all_columns_as_keys: true, within_watermark: true)
        else
          Proto::Deduplicate.new(input: @relation, column_names: Array(subset).map(&:to_s), within_watermark: true)
        end
      build(deduplicate: dedup)
    end
    alias dropDuplicatesWithinWatermark drop_duplicates_within_watermark

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
      GroupedData.new(self, grouping_columns(cols), :GROUP_TYPE_GROUPBY)
    end
    alias groupBy group_by
    alias groupby group_by

    # Multi-dimensional rollup.
    # @return [GroupedData]
    def rollup(*cols)
      GroupedData.new(self, grouping_columns(cols), :GROUP_TYPE_ROLLUP)
    end

    # Multi-dimensional cube.
    # @return [GroupedData]
    def cube(*cols)
      GroupedData.new(self, grouping_columns(cols), :GROUP_TYPE_CUBE)
    end

    # Aggregate over explicit grouping sets (`GROUP BY ... GROUPING SETS (...)`).
    #
    # @example
    #   df.grouping_sets([["dept", "year"], ["dept"], []], "dept", "year").agg(F.sum("x"))
    # @param grouping_sets [Array<Array<Column, String, Integer>>]
    # @param cols [Array<Column, String, Integer>] the grouping columns.
    # @return [GroupedData]
    def grouping_sets(grouping_sets, *cols)
      sets = grouping_sets.map { |set| grouping_columns(Array(set)) }
      GroupedData.new(self, grouping_columns(cols), :GROUP_TYPE_GROUPING_SETS, grouping_sets: sets)
    end
    alias groupingSets grouping_sets

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
    # @param on [String, Array<String>, Column, Array<Column>, nil] join key
    #   column name(s), a boolean join condition, or several conditions that
    #   are combined with AND.
    # @param how [Symbol, String, nil] join type (see {JOIN_TYPES}); `inner` by
    #   default. Case and underscores are ignored (`"LEFT_OUTER"` == `"leftouter"`).
    # @return [DataFrame]
    def join(other, on: nil, how: :inner)
      check_same_session(other)
      j = Proto::Join.new(left: @relation, right: other.relation, join_type: join_type(how))
      case on
      when nil then nil
      when Column then j.join_condition = on.to_expr
      when Array
        if on.all?(Column)
          j.join_condition = on.reduce(:&).to_expr unless on.empty?
        else
          j.using_columns += on.map(&:to_s)
        end
      else j.using_columns << on.to_s
      end
      build(join: j)
    end

    # Lateral join with a DataFrame that may reference this one's columns via
    # {Column#outer} (e.g. a table-valued function over each row).
    #
    # @param other [DataFrame]
    # @param on [Column, nil] the join condition.
    # @param how [Symbol, String, nil] `inner` (default), `left`/`leftouter`, or `cross`.
    # @return [DataFrame]
    def lateral_join(other, on: nil, how: nil)
      check_same_session(other)
      jt = how.nil? ? :JOIN_TYPE_INNER : LATERAL_JOIN_TYPES[normalize_how(how)]
      raise IllegalArgumentError, "Unsupported lateral join type: #{how} (supported: inner, left, leftouter, cross)" unless jt

      lj = Proto::LateralJoin.new(left: @relation, right: other.relation, join_type: jt)
      lj.join_condition = on.to_expr if on
      build(lateral_join: lj)
    end
    alias lateralJoin lateral_join

    # Combine the columns of this DataFrame with those of `other` side by side
    # (Spark 4.3+). Both must derive from the same source through
    # projection-only operations (`select`, `with_column`, ...) so that their
    # rows stay aligned; the server rejects anything else.
    #
    # @param other [DataFrame]
    # @return [DataFrame] the columns of `self` followed by those of `other`.
    def zip(other)
      check_same_session(other)
      build(zip: Proto::Zip.new(left: @relation, right: other.relation))
    end

    # For each row of this DataFrame (the query side), join up to
    # `num_results` rows of `other` (the base side) ranked by
    # `ranking_expression` (Spark 4.2+).
    #
    # @param other [DataFrame]
    # @param ranking_expression [Column] a scalar ranking expression.
    # @param num_results [Integer] matches per row, 1..100000.
    # @param mode [String] `"approx"` or `"exact"`.
    # @param direction [String] `"distance"` (smallest first) or `"similarity"` (largest first).
    # @param join_type [String] `"inner"` (default) or `"leftouter"`.
    # @raise [AnalysisError] for invalid arguments (`NEAREST_BY_JOIN.*` error classes).
    # @return [DataFrame]
    def nearest_by_join(other, ranking_expression, num_results, mode, direction, join_type: "inner")
      check_same_session(other)
      validate_nearest_by_join(num_results, join_type.to_s, mode.to_s, direction.to_s)
      nbj = Proto::NearestByJoin.new(
        left: @relation, right: other.relation, ranking_expression: Column.to_col(ranking_expression).to_expr,
        num_results: Integer(num_results), join_type: join_type.to_s, mode: mode.to_s, direction: direction.to_s
      )
      build(nearest_by_join: nbj)
    end
    alias nearestByJoin nearest_by_join

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

    # Rows in this DataFrame not in `other`, keeping duplicates - Spark's `EXCEPT ALL`.
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

    # Repartition into `num_partitions` sending each row directly to the
    # partition given by the integer column `partition_id_col` (Spark 4.1+).
    #
    # @param num_partitions [Integer]
    # @param partition_id_col [Column, String]
    # @return [DataFrame]
    def repartition_by_id(num_partitions, partition_id_col)
      unless num_partitions.is_a?(Integer) && num_partitions.positive?
        raise IllegalArgumentError, "num_partitions must be a positive Integer, got #{num_partitions.inspect}"
      end

      pid = Functions._col(partition_id_col).to_expr
      expr = Proto::Expression.new(direct_shuffle_partition_id: Proto::Expression::DirectShufflePartitionID.new(child: pid))
      build(repartition_by_expression: Proto::RepartitionByExpression.new(
        input: @relation, partition_exprs: [expr], num_partitions: num_partitions
      ))
    end
    alias repartitionById repartition_by_id

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
      # Like PySpark, pick the seed on the client so the sample is stable across actions.
      s = Proto::Sample.new(
        input: @relation, lower_bound: 0.0, upper_bound: fraction.to_f, with_replacement: with_replacement,
        seed: seed.nil? ? random_seed : Integer(seed)
      )
      build(sample: s)
    end

    # Randomly split this DataFrame by `weights` (normalized to sum to 1).
    #
    # @param weights [Array<Numeric>] non-negative weights.
    # @param seed [Integer, nil] random seed (generated when omitted).
    # @return [Array<DataFrame>]
    def random_split(weights, seed = nil)
      raise IllegalArgumentError, "weights must be non-negative, got #{weights.inspect}" if weights.any?(&:negative?)

      total = weights.sum.to_f
      raise IllegalArgumentError, "the sum of weights must be positive, got #{total}" unless total.positive?

      seed = seed.nil? ? random_seed : Integer(seed)
      bounds = weights.each_with_object([0.0]) { |w, acc| acc << (acc.last + (w / total)) }
      bounds.each_cons(2).map do |lower, upper|
        build(sample: Proto::Sample.new(input: @relation, lower_bound: lower, upper_bound: upper,
                                        with_replacement: false, seed: seed, deterministic_order: true))
      end
    end
    alias randomSplit random_split

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
      params = params.first if params.size == 1 && params.first.is_a?(Array)
      h = Proto::Hint.new(input: @relation, name: name.to_s,
                          parameters: params.map { |p| Column.lit(p).to_expr })
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

    # Shortcut for {DataFrameNaFunctions#drop} (PySpark's `dropna`).
    # @return [DataFrame]
    def dropna(how: :any, thresh: nil, subset: nil) = na.drop(how: how, thresh: thresh, subset: subset)

    # Shortcut for {DataFrameNaFunctions#fill} (PySpark's `fillna`).
    # @return [DataFrame]
    def fillna(value, subset: nil) = na.fill(value, subset: subset)

    # Shortcut for {DataFrameNaFunctions#replace}.
    # @return [DataFrame]
    def replace(to_replace, value = DataFrameNaFunctions::NO_VALUE, subset: nil) = na.replace(to_replace, value, subset: subset)

    # Shortcut for {DataFrameStatFunctions#approx_quantile}.
    # @return [Array<Float>, Array<Array<Float>>]
    def approx_quantile(cols, probabilities, relative_error) = stat.approx_quantile(cols, probabilities, relative_error)
    alias approxQuantile approx_quantile

    # Shortcut for {DataFrameStatFunctions#corr}.
    # @return [Float]
    def corr(col1, col2, method = nil) = stat.corr(col1, col2, method)

    # Shortcut for {DataFrameStatFunctions#cov}.
    # @return [Float]
    def cov(col1, col2) = stat.cov(col1, col2)

    # Shortcut for {DataFrameStatFunctions#crosstab}.
    # @return [DataFrame]
    def crosstab(col1, col2) = stat.crosstab(col1, col2)

    # Shortcut for {DataFrameStatFunctions#freq_items}.
    # @return [DataFrame]
    def freq_items(cols, support = nil) = stat.freq_items(cols, support)
    alias freqItems freq_items

    # Shortcut for {DataFrameStatFunctions#sample_by}.
    # @return [DataFrame]
    def sample_by(col, fractions, seed = nil) = stat.sample_by(col, fractions, seed)
    alias sampleBy sample_by

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

    # Start a `MERGE INTO table` from this (source) DataFrame.
    #
    # @example
    #   source.merge_into("target", F.col("t.id") == F.col("s.id"))
    #         .when_matched.update_all
    #         .when_not_matched.insert_all
    #         .merge
    # @param table [String] the target table.
    # @param condition [Column] the merge condition.
    # @return [MergeIntoWriter]
    def merge_into(table, condition)
      MergeIntoWriter.new(self, table, condition)
    end
    alias mergeInto merge_into

    # ---- Temporary views ---------------------------------------------------

    # Register this DataFrame as a session-scoped temporary view, failing if a
    # view of the same name already exists.
    # @return [void]
    def create_temp_view(name)
      register_view(name, global: false, replace: false)
    end
    alias createTempView create_temp_view

    # Register (or replace) this DataFrame as a session-scoped temporary view.
    # @return [void]
    def create_or_replace_temp_view(name)
      register_view(name, global: false, replace: true)
    end
    alias createOrReplaceTempView create_or_replace_temp_view

    # Register this DataFrame as a global (cross-session) temporary view.
    # @return [void]
    def create_global_temp_view(name)
      register_view(name, global: true, replace: false)
    end
    alias createGlobalTempView create_global_temp_view

    # Register (or replace) this DataFrame as a global temporary view.
    # @return [void]
    def create_or_replace_global_temp_view(name)
      register_view(name, global: true, replace: true)
    end
    alias createOrReplaceGlobalTempView create_or_replace_global_temp_view

    # Deprecated alias of {#create_or_replace_temp_view} (kept for PySpark parity).
    # @return [void]
    def register_temp_table(name)
      create_or_replace_temp_view(name)
    end
    alias registerTempTable register_temp_table

    # Select columns by a regular expression matched against their names.
    #
    # @param regex [String]
    # @return [Column]
    def col_regex(regex)
      raise IllegalArgumentError, "col_regex expects a String, got #{regex.class}" unless regex.is_a?(String)

      Column.new(Proto::Expression.new(
                   unresolved_regex: Proto::Expression::UnresolvedRegex.new(col_name: regex, plan_id: plan_id)
                 ))
    end
    alias colRegex col_regex

    # A column of this DataFrame bound to it by plan id, so it stays
    # unambiguous in self-joins (like `df.col` / `df["col"]` in PySpark).
    #
    # @param name [String]
    # @return [Column]
    def col(name)
      Column.from_name(name.to_s, plan_id: plan_id)
    end

    # A metadata column (e.g. `_metadata` of file sources) of this DataFrame.
    #
    # @param name [String]
    # @return [Column]
    def metadata_column(name)
      raise IllegalArgumentError, "metadata_column expects a String, got #{name.class}" unless name.is_a?(String)

      Column.from_name(name, plan_id: plan_id, is_metadata_column: true)
    end
    alias metadataColumn metadata_column

    # Attach JSON-serializable `metadata` to the column `column_name`.
    #
    # @param column_name [String]
    # @param metadata [Hash]
    # @return [DataFrame]
    def with_metadata(column_name, metadata)
      raise IllegalArgumentError, "metadata must be a Hash, got #{metadata.class}" unless metadata.is_a?(Hash)

      a = Proto::Expression::Alias.new(expr: col(column_name.to_s).to_expr, name: [column_name.to_s],
                                       metadata: JSON.generate(metadata))
      build(with_columns: Proto::WithColumns.new(input: @relation, aliases: [a]))
    end
    alias withMetadata with_metadata

    # Append a column of consecutive 0-based Long row indices (Spark 4.2+),
    # like `RDD#zipWithIndex`.
    #
    # @param index_col_name [String]
    # @return [DataFrame]
    def zip_with_index(index_col_name = "index")
      seq = Proto::Expression.new(
        unresolved_function: Proto::Expression::UnresolvedFunction.new(function_name: "distributed_sequence_id")
      )
      select(Functions.col("*"), Column.new(seq).alias(index_col_name.to_s))
    end
    alias zipWithIndex zip_with_index

    # Transpose rows and columns. The `index_column` (the first column when
    # omitted) supplies the new column names.
    #
    # @param index_column [Column, String, nil]
    # @return [DataFrame]
    def transpose(index_column = nil)
      cols = index_column.nil? ? [] : [Functions._col(index_column).to_expr]
      build(transpose: Proto::Transpose.new(input: @relation, index_columns: cols))
    end

    # @return [DataFrame] a single-column (`value`) DataFrame of each row encoded
    #   as a JSON string.
    def to_json(*_args)
      select(Functions.to_json(Functions.struct(Functions.col("*"))).alias("value"))
    end
    alias toJSON to_json

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
    # @param storage_level [StorageLevel, nil] where to keep the checkpointed data.
    # @return [DataFrame]
    def local_checkpoint(eager: true, storage_level: nil)
      checkpoint_command(local: true, eager: eager, storage_level: storage_level)
    end
    alias localCheckpoint local_checkpoint

    # Observe named metrics over this DataFrame.
    #
    # @param name [String, Observation]
    # @param exprs [Array<Column>]
    # @return [DataFrame]
    def observe(name, *exprs)
      raise IllegalArgumentError, "observe requires at least one expression" if exprs.empty?
      raise IllegalArgumentError, "observe expressions must be Columns" unless exprs.all?(Column)

      obs_name = name.is_a?(Observation) ? name.name : name.to_s
      cm = Proto::CollectMetrics.new(
        input: @relation, name: obs_name, metrics: exprs.map { |e| Column.to_col(e).to_expr }
      )
      df = build(collect_metrics: cm)
      name.bind(df) if name.is_a?(Observation)
      df
    end

    # ---- Subqueries ----------------------------------------------------------

    # This (single-row, single-column) DataFrame as a scalar subquery
    # {Column}, e.g. `df.filter(F.col("x") > other.select(F.max("x")).scalar)`.
    # Correlated references to the outer query use {Column#outer}.
    # @return [Column]
    def scalar
      subquery_expression(:SUBQUERY_TYPE_SCALAR)
    end

    # An `EXISTS (subquery)` {Column} over this DataFrame.
    # @return [Column]
    def exists
      subquery_expression(:SUBQUERY_TYPE_EXISTS)
    end

    # This DataFrame as a table argument for a table-valued function, with
    # optional partitioning/ordering (see {TableArg}).
    # @return [TableArg]
    def as_table
      TableArg.new(self)
    end
    alias asTable as_table

    # ---- Caching -------------------------------------------------------------

    # Persist with the default storage level (`MEMORY_AND_DISK_DESER`).
    # @return [self]
    def cache
      persist
    end

    # Persist this DataFrame with the given storage level.
    # @param storage_level [StorageLevel]
    # @return [self]
    def persist(storage_level = StorageLevel::MEMORY_AND_DISK_DESER)
      analyze(persist: Proto::AnalyzePlanRequest::Persist.new(relation: @relation, storage_level: storage_level.to_proto))
      self
    end

    # Remove this DataFrame from the cache.
    # @param blocking [Boolean] wait until all blocks are deleted.
    # @return [self]
    def unpersist(blocking = false)
      analyze(unpersist: Proto::AnalyzePlanRequest::Unpersist.new(relation: @relation, blocking: blocking))
      self
    end

    # @return [StorageLevel] the current storage level (`StorageLevel::NONE` when not cached).
    def storage_level
      resp = analyze(get_storage_level: Proto::AnalyzePlanRequest::GetStorageLevel.new(relation: @relation))
      StorageLevel.from_proto(resp.get_storage_level.storage_level)
    end
    alias storageLevel storage_level

    # @return [Boolean] whether this DataFrame is cached.
    def cached?
      storage_level != StorageLevel::NONE
    end
    alias is_cached cached?

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

    # Index into a column by name (`df["id"]`, bound to this DataFrame like
    # {#col}) or position (`df[0]`), filter by a boolean {Column}
    # (`df[df["x"] > 1]`), or select several columns (`df[["a", "b"]]`).
    #
    # @param key [String, Symbol, Integer, Column, Array]
    # @return [Column, DataFrame]
    def [](key)
      case key
      when Integer then Functions.col(columns[key])
      when Column then filter(key)
      when Array then select(*key)
      else col(key.to_s)
      end
    end

    # Allows `df.column_name` for valid identifier column names.
    def method_missing(name, *args)
      if args.empty? && columns.include?(name.to_s)
        col(name.to_s)
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

    # Iterate over all rows (materialises the result). Returns an Enumerator when
    # no block is given.
    #
    # @yieldparam row [Row]
    # @return [Enumerator, void]
    def each(&block)
      return collect.each unless block

      collect.each(&block)
    end

    # @return [Enumerator<Row>] an enumerator over all rows.
    def to_local_iterator
      collect.each
    end
    alias toLocalIterator to_local_iterator

    # @return [Array<Row>] the first `n` rows.
    def take(n)
      limit(n).collect
    end

    # @return [Array<Row>] the last `n` rows.
    def tail(n)
      build(tail: Proto::Tail.new(input: @relation, limit: n)).collect
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
    alias isEmpty empty?

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
    alias inputFiles input_files

    # @return [Boolean] whether the data is small enough to be local.
    def local?
      analyze(is_local: Proto::AnalyzePlanRequest::IsLocal.new(plan: plan)).is_local.is_local
    end
    alias is_local local?
    alias isLocal local?

    # @return [Boolean] whether this is a streaming DataFrame.
    def streaming?
      analyze(is_streaming: Proto::AnalyzePlanRequest::IsStreaming.new(plan: plan)).is_streaming.is_streaming
    end
    alias is_streaming streaming?
    alias isStreaming streaming?

    # @return [Boolean] whether `other` has the same logical plan.
    def same_semantics?(other)
      analyze(same_semantics: Proto::AnalyzePlanRequest::SameSemantics.new(
        target_plan: plan, other_plan: other.plan
      )).same_semantics.result
    end
    alias same_semantics same_semantics?
    alias sameSemantics same_semantics?

    # @return [Integer] a hash of the logical plan.
    def semantic_hash
      analyze(semantic_hash: Proto::AnalyzePlanRequest::SemanticHash.new(plan: plan)).semantic_hash.result
    end
    alias semanticHash semantic_hash

    # @return [SparkSession] the session this DataFrame belongs to.
    def spark_session = @session
    alias sparkSession spark_session

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
      relation = PlanBuilder.relation(@session, **rel)
      DataFrame.new(@session, PlanBuilder.with_subquery_references(@session, relation))
    end

    # @api private - the plan id of this DataFrame's relation.
    # @return [Integer]
    def plan_id
      @relation.common.plan_id
    end

    # @api private - a subquery expression referring to this DataFrame.
    # @return [Column]
    def subquery_expression(type, in_subquery_values: [], table_arg_options: nil)
      @session.register_subquery_relation(@relation)
      sq = Proto::SubqueryExpression.new(plan_id: plan_id, subquery_type: type, in_subquery_values: in_subquery_values)
      sq.table_arg_options = table_arg_options if table_arg_options
      Column.new(Proto::Expression.new(subquery_expression: sq))
    end

    private

    def analyze(**kw)
      @session.client.analyze(**kw)
    end

    def register_view(name, global:, replace:)
      cmd = Proto::CreateDataFrameViewCommand.new(
        input: @relation, name: name.to_s, is_global: global, replace: replace
      )
      @session.client.execute_command(Proto::Command.new(create_dataframe_view: cmd))
      nil
    end

    def checkpoint_command(local:, eager:, storage_level: nil)
      cmd = Proto::CheckpointCommand.new(relation: @relation, local: local, eager: eager)
      cmd.storage_level = storage_level.to_proto if storage_level
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

    # Like {#normalize_columns} but an Integer is a 1-based column ordinal
    # (`group_by(1)`), like PySpark.
    def grouping_columns(cols)
      cols.flatten.map do |c|
        case c
        when Column then c
        when Integer
          raise IllegalArgumentError, "column ordinal must be positive, got #{c}" if c < 1

          self[c - 1]
        else Functions.col(c.to_s)
        end
      end
    end

    def check_same_session(other)
      raise IllegalArgumentError, "expected a DataFrame, got #{other.class}" unless other.is_a?(DataFrame)
      return if other.session.session_id == @session.session_id

      raise IllegalArgumentError, "the DataFrames belong to different SparkSessions"
    end

    def normalize_how(how)
      how.to_s.downcase.delete("_").to_sym
    end

    def join_type(how)
      return :JOIN_TYPE_INNER if how.nil?

      JOIN_TYPES[normalize_how(how)] or raise IllegalArgumentError, "Unsupported join type: #{how}"
    end

    def random_seed = Random.rand(2**63)

    def validate_nearest_by_join(num_results, join_type, mode, direction)
      unless num_results.is_a?(Integer) && num_results.between?(1, NEAREST_BY_JOIN_MAX_NUM_RESULTS)
        raise AnalysisError.new(
          "numResults must be between 1 and #{NEAREST_BY_JOIN_MAX_NUM_RESULTS}, got #{num_results}",
          error_class: "NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE"
        )
      end
      unless NEAREST_BY_JOIN_JOIN_TYPES.include?(join_type.downcase.delete("_"))
        raise AnalysisError.new("Unsupported join type #{join_type}; supported: 'INNER', 'LEFT OUTER'",
                                error_class: "NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE")
      end
      unless NEAREST_BY_JOIN_MODES.include?(mode.downcase)
        raise AnalysisError.new("Unsupported mode #{mode}; supported: 'approx', 'exact'",
                                error_class: "NEAREST_BY_JOIN.UNSUPPORTED_MODE")
      end
      return if NEAREST_BY_JOIN_DIRECTIONS.include?(direction.downcase)

      raise AnalysisError.new("Unsupported direction #{direction}; supported: 'distance', 'similarity'",
                              error_class: "NEAREST_BY_JOIN.UNSUPPORTED_DIRECTION")
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
