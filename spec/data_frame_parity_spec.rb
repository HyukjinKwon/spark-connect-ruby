# frozen_string_literal: true

# DataFrame and Column APIs added for PySpark 4.3.0 Spark Connect parity.
RSpec.describe SparkConnect::DataFrame do
  let(:df) { spark.range(10) }
  let(:other) { spark.range(5) }

  describe "#zip / #zip_with_index / #nearest_by_join" do
    it "zip builds a Zip relation over both inputs" do
      out = df.zip(other)
      expect(rel_type(out)).to eq(:zip)
      expect(rel_body(out).left).to eq(df.relation)
      expect(rel_body(out).right).to eq(other.relation)
    end

    it "zip_with_index appends distributed_sequence_id under the given name" do
      exprs = rel_body(df.zip_with_index("idx")).expressions
      expect(exprs.first.expr_type).to eq(:unresolved_star)
      alias_ = exprs.last.alias
      expect(alias_.name.to_a).to eq(["idx"])
      expect(alias_.expr.unresolved_function.function_name).to eq("distributed_sequence_id")
      expect(rel_body(df.zipWithIndex).expressions.last.alias.name.to_a).to eq(["index"])
    end

    it "nearest_by_join builds a NearestByJoin relation" do
      out = df.nearest_by_join(other, f.col("a") - f.col("b"), 3, "exact", "distance", join_type: "leftouter")
      body = rel_body(out)
      expect(rel_type(out)).to eq(:nearest_by_join)
      expect([body.num_results, body.mode, body.direction, body.join_type]).to eq([3, "exact", "distance", "leftouter"])
      expect(body.ranking_expression.unresolved_function.function_name).to eq("-")
    end

    it "nearest_by_join validates its arguments with the NEAREST_BY_JOIN error classes" do
      rank = f.col("x")
      {
        -> { df.nearest_by_join(other, rank, 0, "exact", "distance") } => "NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE",
        -> { df.nearest_by_join(other, rank, 100_001, "exact", "distance") } => "NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE",
        -> { df.nearest_by_join(other, rank, 1, "exact", "distance", join_type: "right") } => "NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE",
        -> { df.nearest_by_join(other, rank, 1, "fuzzy", "distance") } => "NEAREST_BY_JOIN.UNSUPPORTED_MODE",
        -> { df.nearest_by_join(other, rank, 1, "exact", "up") } => "NEAREST_BY_JOIN.UNSUPPORTED_DIRECTION",
      }.each do |call, error_class|
        expect { call.call }.to raise_error(SparkConnect::AnalysisError) { |e| expect(e.error_class).to eq(error_class) }
      end
      expect { df.nearest_by_join(other, rank, 1, "APPROX", "Similarity", join_type: "LEFT_OUTER") }.not_to raise_error
    end
  end

  describe "subqueries" do
    def with_relations(out)
      expect(rel_type(out)).to eq(:with_relations)
      rel_body(out)
    end

    it "scalar wraps the consuming relation in WithRelations carrying the subquery plan" do
      sub = other.select(f.max("id"))
      wr = with_relations(df.filter(f.col("id") > sub.scalar))
      expect(wr.root.rel_type).to eq(:filter)
      cond = wr.root.filter.condition.unresolved_function.arguments[1].subquery_expression
      expect(cond.subquery_type).to eq(:SUBQUERY_TYPE_SCALAR)
      expect(cond.plan_id).to eq(sub.relation.common.plan_id)
      expect(wr.references.to_a).to eq([sub.relation])
    end

    it "exists and isin(DataFrame) build EXISTS / IN subqueries" do
      expect(with_relations(df.filter(other.exists)).root.filter.condition.subquery_expression.subquery_type)
        .to eq(:SUBQUERY_TYPE_EXISTS)
      in_sq = with_relations(df.filter(f.col("id").isin(other))).root.filter.condition.subquery_expression
      expect(in_sq.subquery_type).to eq(:SUBQUERY_TYPE_IN)
      expect(in_sq.in_subquery_values.size).to eq(1)
      multi = with_relations(df.filter(f.struct("a", "b").isin(other))).root.filter.condition.subquery_expression
      expect(multi.in_subquery_values.size).to eq(2)
    end

    it "does not wrap relations without subqueries, nor re-wrap children" do
      expect(rel_type(df.select("id"))).to eq(:project)
      inner = df.filter(other.exists)
      expect(rel_type(inner.select("id"))).to eq(:project)
    end

    it "as_table builds a table-argument subquery with partitioning and ordering" do
      ta = other.as_table.partition_by("id").order_by(f.col("id").desc)
      sq = ta.to_expr.subquery_expression
      expect(sq.subquery_type).to eq(:SUBQUERY_TYPE_TABLE_ARG)
      expect(sq.table_arg_options.partition_spec.size).to eq(1)
      expect(sq.table_arg_options.order_spec.first.direction).to eq(:SORT_DIRECTION_DESCENDING)
      expect(other.as_table.with_single_partition.to_expr.subquery_expression.table_arg_options.with_single_partition).to be(true)
      expect { other.as_table.order_by("id") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { other.as_table.with_single_partition.partition_by("id") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect(rel_type(spark.tvf.explode(ta))).to eq(:with_relations)
    end
  end

  describe "joins" do
    it "lateral_join builds a LateralJoin with a normalized join type" do
      body = rel_body(df.lateral_join(other, on: f.col("a") == 1, how: "LEFT_OUTER"))
      expect(body.join_type).to eq(:JOIN_TYPE_LEFT_OUTER)
      expect(body.join_condition).not_to be_nil
      expect { df.lateral_join(other, how: :right) }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "join combines an Array of Columns with AND and ignores case/underscores in how" do
      body = rel_body(df.join(other, on: [f.col("a") == f.col("b"), f.col("c") == f.col("d")], how: "LEFT_OUTER"))
      expect(body.join_type).to eq(:JOIN_TYPE_LEFT_OUTER)
      expect(body.join_condition.unresolved_function.function_name).to eq("and")
      expect(rel_body(df.join(other, how: nil)).join_type).to eq(:JOIN_TYPE_INNER)
    end
  end

  describe "column references" do
    it "binds df[name] / df.col / method access to this DataFrame's plan id" do
      e = df["id"].to_expr.unresolved_attribute
      expect(e.plan_id).to eq(df.relation.common.plan_id)
      expect(df.col("id").to_expr.unresolved_attribute.plan_id).to eq(df.relation.common.plan_id)
      expect(df["*"].to_expr.unresolved_star.plan_id).to eq(df.relation.common.plan_id)
    end

    it "filters with df[Column] and selects with df[Array]" do
      expect(rel_type(df[f.col("id") > 1])).to eq(:filter)
      expect(rel_body(df[%w[id id]]).expressions.size).to eq(2)
    end

    it "metadata_column and col_regex carry the plan id" do
      m = df.metadata_column("_metadata").to_expr.unresolved_attribute
      expect([m.is_metadata_column, m.plan_id]).to eq([true, df.relation.common.plan_id])
      expect(df.col_regex("`a.*`").to_expr.unresolved_regex.plan_id).to eq(df.relation.common.plan_id)
    end
  end

  describe "other transformations" do
    it "grouping_sets builds a GROUPING_SETS aggregate (and accepts ordinals)" do
      agg = rel_body(df.grouping_sets([["id"], []], "id").agg(f.count("*")))
      expect(agg.group_type).to eq(:GROUP_TYPE_GROUPING_SETS)
      expect(agg.grouping_sets.map { |g| g.grouping_set.size }).to eq([1, 0])
      fake_client.schema = SparkConnect::Types.struct(SparkConnect::Types.field("k", SparkConnect::Types.string))
      expect(rel_body(df.group_by(1).count).grouping_expressions.first.unresolved_attribute.unparsed_identifier).to eq("k")
    end

    it "transpose, tail, with_metadata, repartition_by_id build the matching relations" do
      expect(rel_body(df.transpose("id")).index_columns.size).to eq(1)
      expect(rel_body(df.transpose).index_columns).to be_empty
      df.tail(3)
      expect(fake_client.last_relation.tail.limit).to eq(3)
      wm = rel_body(df.with_metadata("id", { "k" => "v" })).aliases.first
      expect(JSON.parse(wm.metadata)).to eq({ "k" => "v" })
      rp = rel_body(df.repartition_by_id(4, "id"))
      expect(rp.num_partitions).to eq(4)
      expect(rp.partition_exprs.first.expr_type).to eq(:direct_shuffle_partition_id)
      expect { df.repartition_by_id(0, "id") }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "sample and random_split always send a client-side seed" do
      expect(rel_body(df.sample(0.5)).seed).to be_a(Integer)
      expect(rel_body(df.sample(0.5, seed: 3)).seed).to eq(3)
      splits = df.random_split([1, 3], 42)
      expect(splits.map { |s| [rel_body(s).lower_bound, rel_body(s).upper_bound] }).to eq([[0.0, 0.25], [0.25, 1.0]])
      expect(splits.map { |s| rel_body(s).seed }.uniq).to eq([42])
      expect(rel_body(splits.first).deterministic_order).to be(true)
      expect { df.random_split([-1, 1]) }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "hint accepts a single Array of parameters" do
      expect(rel_body(df.hint("rebalance", [1, 2])).parameters.size).to eq(2)
    end

    it "local_checkpoint passes a storage level" do
      df.local_checkpoint(storage_level: SparkConnect::StorageLevel::DISK_ONLY)
      expect(fake_client.last_command.checkpoint_command.storage_level.use_disk).to be(true)
    end
  end

  describe "caching" do
    it "persist/unpersist/storage_level use the AnalyzePlan RPC" do
      expect(df.cache).to equal(df)
      expect(fake_client.analyze_requests.last[:persist].storage_level.deserialized).to be(true)
      df.persist(SparkConnect::StorageLevel::MEMORY_ONLY_2)
      expect(fake_client.analyze_requests.last[:persist].storage_level.replication).to eq(2)
      df.unpersist(true)
      expect(fake_client.analyze_requests.last[:unpersist].blocking).to be(true)
      expect(df.storage_level).to eq(SparkConnect::StorageLevel::MEMORY_ONLY)
      expect(df.cached?).to be(true)
    end
  end

  describe "NA/stat shortcuts and merge" do
    it "delegates dropna/fillna/replace/crosstab/freq_items/sample_by" do
      expect(rel_type(df.dropna(how: :all))).to eq(:drop_na)
      expect(rel_type(df.fillna(0))).to eq(:fill_na)
      expect(rel_type(df.replace(1, 2))).to eq(:replace)
      expect(rel_type(df.crosstab("a", "b"))).to eq(:crosstab)
      expect(rel_body(df.freq_items(%w[a])).support).to eq(0.01)
      expect(rel_body(df.sample_by("a", { 1 => 0.5 })).seed).to be_a(Integer)
      expect { df.corr("a", "b", "spearman") }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "merge_into builds a MergeIntoTableCommand with every clause" do
      df.merge_into("t", f.expr("t.id = s.id"))
        .when_matched(f.expr("s.del")).delete
        .when_matched.update("v" => f.col("s.v"))
        .when_not_matched.insert_all
        .when_not_matched_by_source.update_all
        .with_schema_evolution
        .merge
      cmd = fake_client.last_command.merge_into_table_command
      expect(cmd.target_table_name).to eq("t")
      expect(cmd.match_actions.map { |a| a.merge_action.action_type }).to eq(%i[ACTION_TYPE_DELETE ACTION_TYPE_UPDATE])
      expect(cmd.match_actions.last.merge_action.assignments.first.key.expression_string.expression).to eq("v")
      expect(cmd.not_matched_actions.first.merge_action.action_type).to eq(:ACTION_TYPE_INSERT_STAR)
      expect(cmd.not_matched_by_source_actions.first.merge_action.action_type).to eq(:ACTION_TYPE_UPDATE_STAR)
      expect(cmd.with_schema_evolution).to be(true)
      expect { df.merge_into("t", "cond") }.to raise_error(SparkConnect::IllegalArgumentError)
    end
  end

  describe "aliases" do
    it "exposes the PySpark names" do
      %i[isLocal isStreaming sameSemantics semanticHash inputFiles isEmpty sparkSession registerTempTable
         lateralJoin groupingSets mergeInto metadataColumn randomSplit repartitionById withMetadata zipWithIndex
         nearestByJoin asTable approxQuantile freqItems sampleBy storageLevel is_cached].each do |m|
        expect(df).to respond_to(m)
      end
      expect(df.spark_session).to equal(spark)
    end
  end
end

RSpec.describe SparkConnect::Column do
  let(:c) { SparkConnect::F.col("s") }

  it "try_cast casts in TRY eval mode" do
    cast = c.try_cast("int").to_expr.cast
    expect([cast.type_str, cast.eval_mode]).to eq(["int", :EVAL_MODE_TRY])
  end

  it "with_field and drop_fields build UpdateFields" do
    wf = c.with_field("x", SparkConnect::F.lit(1)).to_expr.update_fields
    expect([wf.field_name, wf.value_expression.literal.integer]).to eq(["x", 1])
    df = c.drop_fields("a", "b").to_expr.update_fields
    expect(df.field_name).to eq("b")
    expect(df.struct_expression.update_fields.field_name).to eq("a")
    expect(df.value_expression).to be_nil
    expect { c.drop_fields }.to raise_error(SparkConnect::IllegalArgumentError)
    expect { c.with_field("x", 1) }.to raise_error(SparkConnect::IllegalArgumentError)
  end

  it "outer returns an equivalent column and transform applies a function" do
    expect(c.outer.to_expr).to eq(c.to_expr)
    expect(c.transform { |x| x + 1 }.to_expr.unresolved_function.function_name).to eq("+")
  end

  it "has the PySpark camelCase aliases" do
    expect(c.bitwiseAND(1).to_expr.unresolved_function.function_name).to eq("&")
    expect(c.eqNullSafe(1).to_expr.unresolved_function.function_name).to eq("<=>")
    expect(c.isNaN.to_expr.unresolved_function.function_name).to eq("isNaN")
    %i[getItem getField withField dropFields bitwiseOR bitwiseXOR].each { |m| expect(c).to respond_to(m) }
  end

  it "decodes literal protos back to Ruby values" do
    l = SparkConnect::Proto::Expression::Literal
    expect(described_class.literal_value(l.new(long: 5))).to eq(5)
    expect(described_class.literal_value(l.new(decimal: l::Decimal.new(value: "1.50")))).to eq(BigDecimal("1.5"))
    expect(described_class.literal_value(l.new(date: 1))).to eq(Date.new(1970, 1, 2))
    arr = l.new(array: l::Array.new(elements: [l.new(integer: 1), l.new(integer: 2)]))
    expect(described_class.literal_value(arr)).to eq([1, 2])
  end
end
