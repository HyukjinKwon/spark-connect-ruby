# frozen_string_literal: true

RSpec.describe SparkConnect::DataFrame do
  let(:df) { spark.range(10) }
  let(:other) { spark.range(5) }

  describe "#select / #select_expr" do
    it "builds a project with one expression per column" do
      out = df.select("id", f.col("id"))
      expect(rel_type(out)).to eq(:project)
      expect(rel_body(out).expressions.size).to eq(2)
      expect(rel_body(out).input).to eq(df.relation)
    end

    it "select_expr wraps each string in an expression_string" do
      out = df.select_expr("id + 1", "id * 2")
      expect(rel_type(out)).to eq(:project)
      exprs = rel_body(out).expressions
      expect(exprs.map { |e| e.expression_string.expression }).to eq(["id + 1", "id * 2"])
    end

    it "exposes the selectExpr camelCase alias" do
      out = df.selectExpr("id")
      expect(rel_body(out).expressions.first.expression_string.expression).to eq("id")
    end
  end

  describe "#filter / #where" do
    it "builds a filter from a Column condition" do
      out = df.filter(f.col("id") > 3)
      expect(rel_type(out)).to eq(:filter)
      expect(rel_body(out).input).to eq(df.relation)
      expect(rel_body(out).condition).not_to be_nil
    end

    it "converts a String condition into an expression_string via expr" do
      out = df.filter("id > 3")
      expect(rel_type(out)).to eq(:filter)
      expect(rel_body(out).condition.expression_string.expression).to eq("id > 3")
    end

    it "aliases where to filter" do
      out = df.where("id < 5")
      expect(rel_type(out)).to eq(:filter)
      expect(rel_body(out).condition.expression_string.expression).to eq("id < 5")
    end
  end

  describe "#with_column(s)" do
    it "with_column wraps a single alias" do
      out = df.with_column("two", f.lit(2))
      expect(rel_type(out)).to eq(:with_columns)
      aliases = rel_body(out).aliases
      expect(aliases.size).to eq(1)
      expect(aliases.first.name).to eq(["two"])
    end

    it "with_columns adds multiple aliases" do
      out = df.with_columns("a" => f.lit(1), "b" => f.lit(2))
      expect(rel_body(out).aliases.map { |a| a.name.first }).to eq(%w[a b])
    end

    it "supports the withColumn and withColumns camelCase aliases" do
      expect(rel_type(df.withColumn("x", f.lit(1)))).to eq(:with_columns)
      expect(rel_body(df.withColumns("x" => f.lit(1))).aliases.size).to eq(1)
    end
  end

  describe "#with_column_renamed(s)" do
    it "with_column_renamed builds a single rename" do
      out = df.with_column_renamed("id", "key")
      expect(rel_type(out)).to eq(:with_columns_renamed)
      r = rel_body(out).renames.first
      expect([r.col_name, r.new_col_name]).to eq(%w[id key])
    end

    it "with_columns_renamed builds multiple renames" do
      out = df.with_columns_renamed("id" => "k", "x" => "y")
      pairs = rel_body(out).renames.map { |r| [r.col_name, r.new_col_name] }
      expect(pairs).to eq([%w[id k], %w[x y]])
    end

    it "exposes the withColumnRenamed alias" do
      expect(rel_type(df.withColumnRenamed("id", "k"))).to eq(:with_columns_renamed)
    end
  end

  describe "#drop" do
    it "routes String args to column_names" do
      out = df.drop("id", "x")
      expect(rel_type(out)).to eq(:drop)
      expect(rel_body(out).column_names).to eq(%w[id x])
      expect(rel_body(out).columns).to be_empty
    end

    it "routes Column args to columns" do
      out = df.drop(f.col("id"))
      expect(rel_body(out).columns.size).to eq(1)
      expect(rel_body(out).column_names).to be_empty
    end
  end

  describe "#distinct / #drop_duplicates" do
    it "distinct sets all_columns_as_keys" do
      out = df.distinct
      expect(rel_type(out)).to eq(:deduplicate)
      expect(rel_body(out).all_columns_as_keys).to be(true)
    end

    it "drop_duplicates with no subset uses all columns as keys" do
      out = df.drop_duplicates
      expect(rel_body(out).all_columns_as_keys).to be(true)
    end

    it "drop_duplicates with a subset lists column_names" do
      out = df.drop_duplicates(%w[id])
      expect(rel_body(out).column_names).to eq(%w[id])
      expect(rel_body(out).all_columns_as_keys).to be(false)
    end

    it "exposes the dropDuplicates alias" do
      expect(rel_type(df.dropDuplicates)).to eq(:deduplicate)
    end
  end

  describe "#order_by / #sort / #sort_within_partitions" do
    it "order_by builds a global sort with SortOrder entries" do
      out = df.order_by("id", f.col("id").desc)
      expect(rel_type(out)).to eq(:sort)
      expect(rel_body(out).is_global).to be(true)
      orders = rel_body(out).order
      expect(orders.size).to eq(2)
      expect(orders.first.direction).to eq(:SORT_DIRECTION_ASCENDING)
      expect(orders.last.direction).to eq(:SORT_DIRECTION_DESCENDING)
    end

    it "sort is an alias of order_by" do
      expect(rel_body(df.sort("id")).is_global).to be(true)
    end

    it "sort_within_partitions sets is_global false" do
      out = df.sort_within_partitions("id")
      expect(rel_type(out)).to eq(:sort)
      expect(rel_body(out).is_global).to be(false)
    end

    it "exposes the orderBy alias" do
      expect(rel_type(df.orderBy("id"))).to eq(:sort)
    end
  end

  describe "#limit / #offset" do
    it "limit sets the row count" do
      out = df.limit(3)
      expect(rel_type(out)).to eq(:limit)
      expect(rel_body(out).limit).to eq(3)
    end

    it "offset sets the skip count" do
      out = df.offset(7)
      expect(rel_type(out)).to eq(:offset)
      expect(rel_body(out).offset).to eq(7)
    end
  end

  describe "#group_by / #rollup / #cube" do
    it "group_by returns GroupedData producing a GROUPBY aggregate" do
      gd = df.group_by("id")
      expect(gd).to be_a(SparkConnect::GroupedData)
      out = gd.count
      expect(rel_type(out)).to eq(:aggregate)
      expect(rel_body(out).group_type).to eq(:GROUP_TYPE_GROUPBY)
    end

    it "rollup yields a ROLLUP aggregate" do
      out = df.rollup("id").count
      expect(rel_body(out).group_type).to eq(:GROUP_TYPE_ROLLUP)
    end

    it "cube yields a CUBE aggregate" do
      out = df.cube("id").count
      expect(rel_body(out).group_type).to eq(:GROUP_TYPE_CUBE)
    end

    it "exposes the groupBy alias" do
      expect(df.groupBy("id")).to be_a(SparkConnect::GroupedData)
    end
  end

  describe "#join" do
    it "maps how symbols/strings to JoinType enums" do
      {
        inner: :JOIN_TYPE_INNER,
        left: :JOIN_TYPE_LEFT_OUTER,
        "right" => :JOIN_TYPE_RIGHT_OUTER,
        full: :JOIN_TYPE_FULL_OUTER,
        leftsemi: :JOIN_TYPE_LEFT_SEMI,
        anti: :JOIN_TYPE_LEFT_ANTI,
        cross: :JOIN_TYPE_CROSS,
      }.each do |how, expected|
        out = df.join(other, how: how)
        expect(rel_type(out)).to eq(:join)
        expect(rel_body(out).join_type).to eq(expected)
      end
    end

    it "treats a String on as using_columns" do
      out = df.join(other, on: "id")
      expect(rel_body(out).using_columns).to eq(%w[id])
      expect(rel_body(out).join_condition).to be_nil
    end

    it "treats an Array on as using_columns" do
      out = df.join(other, on: %w[id])
      expect(rel_body(out).using_columns).to eq(%w[id])
    end

    it "treats a Column on as join_condition" do
      out = df.join(other, on: f.col("id"))
      expect(rel_body(out).join_condition).not_to be_nil
      expect(rel_body(out).using_columns).to be_empty
    end

    it "raises for an unsupported join type" do
      expect { df.join(other, how: :bogus) }.to raise_error(SparkConnect::IllegalArgumentError)
    end
  end

  describe "#cross_join" do
    it "builds a CROSS join" do
      out = df.cross_join(other)
      expect(rel_type(out)).to eq(:join)
      expect(rel_body(out).join_type).to eq(:JOIN_TYPE_CROSS)
    end

    it "exposes the crossJoin alias" do
      expect(rel_body(df.crossJoin(other)).join_type).to eq(:JOIN_TYPE_CROSS)
    end
  end

  describe "set operations" do
    it "union is an all-union by position" do
      out = df.union(other)
      expect(rel_type(out)).to eq(:set_op)
      expect(rel_body(out).set_op_type).to eq(:SET_OP_TYPE_UNION)
      expect(rel_body(out).is_all).to be(true)
      expect(rel_body(out).by_name).to be(false)
    end

    it "union_by_name sets by_name and allow_missing_columns" do
      out = df.union_by_name(other, allow_missing_columns: true)
      expect(rel_body(out).set_op_type).to eq(:SET_OP_TYPE_UNION)
      expect(rel_body(out).by_name).to be(true)
      expect(rel_body(out).allow_missing_columns).to be(true)
    end

    it "intersect is a distinct intersection" do
      out = df.intersect(other)
      expect(rel_body(out).set_op_type).to eq(:SET_OP_TYPE_INTERSECT)
      expect(rel_body(out).is_all).to be(false)
    end

    it "intersect_all keeps duplicates" do
      out = df.intersect_all(other)
      expect(rel_body(out).set_op_type).to eq(:SET_OP_TYPE_INTERSECT)
      expect(rel_body(out).is_all).to be(true)
    end

    it "except_all keeps duplicates" do
      out = df.except_all(other)
      expect(rel_body(out).set_op_type).to eq(:SET_OP_TYPE_EXCEPT)
      expect(rel_body(out).is_all).to be(true)
    end

    it "subtract is a distinct except" do
      out = df.subtract(other)
      expect(rel_body(out).set_op_type).to eq(:SET_OP_TYPE_EXCEPT)
      expect(rel_body(out).is_all).to be(false)
    end

    it "exposes the unionByName alias" do
      expect(rel_body(df.unionByName(other)).by_name).to be(true)
    end
  end

  describe "#repartition / #coalesce" do
    it "repartition without cols sets shuffle true" do
      out = df.repartition(4)
      expect(rel_type(out)).to eq(:repartition)
      expect(rel_body(out).num_partitions).to eq(4)
      expect(rel_body(out).shuffle).to be(true)
    end

    it "repartition with cols uses repartition_by_expression" do
      out = df.repartition(4, "id")
      expect(rel_type(out)).to eq(:repartition_by_expression)
      expect(rel_body(out).num_partitions).to eq(4)
      expect(rel_body(out).partition_exprs.size).to eq(1)
    end

    it "coalesce sets shuffle false" do
      out = df.coalesce(2)
      expect(rel_type(out)).to eq(:repartition)
      expect(rel_body(out).shuffle).to be(false)
      expect(rel_body(out).num_partitions).to eq(2)
    end
  end

  describe "#sample" do
    it "builds a sample with bounds and seed" do
      out = df.sample(0.25, with_replacement: true, seed: 11)
      expect(rel_type(out)).to eq(:sample)
      expect(rel_body(out).lower_bound).to eq(0.0)
      expect(rel_body(out).upper_bound).to be_within(1e-9).of(0.25)
      expect(rel_body(out).with_replacement).to be(true)
      expect(rel_body(out).seed).to eq(11)
    end
  end

  describe "#alias / #hint / #unpivot" do
    it "alias builds a subquery_alias" do
      out = df.alias("t")
      expect(rel_type(out)).to eq(:subquery_alias)
      expect(rel_body(out).alias).to eq("t")
    end

    it "hint builds a hint with name and params" do
      out = df.hint("broadcast")
      expect(rel_type(out)).to eq(:hint)
      expect(rel_body(out).name).to eq("broadcast")
    end

    it "unpivot builds ids, values, and column names" do
      out = df.unpivot(["id"], ["id"], "var", "val")
      expect(rel_type(out)).to eq(:unpivot)
      expect(rel_body(out).ids.size).to eq(1)
      expect(rel_body(out).values.values.size).to eq(1)
      expect(rel_body(out).variable_column_name).to eq("var")
      expect(rel_body(out).value_column_name).to eq("val")
    end

    it "unpivot with nil values omits the values message" do
      out = df.unpivot(["id"], nil, "var", "val")
      expect(rel_body(out).values).to be_nil
    end
  end

  describe "#to / #to_df" do
    it "to_df renames columns positionally" do
      out = df.to_df("a", "b")
      expect(rel_type(out)).to eq(:to_df)
      expect(rel_body(out).column_names).to eq(%w[a b])
    end

    it "exposes the toDF alias" do
      expect(rel_body(df.toDF("a")).column_names).to eq(%w[a])
    end

    it "to applies a target schema" do
      schema = SparkConnect::Types.struct(SparkConnect::Types.field("id", SparkConnect::Types.long))
      out = df.to(schema)
      expect(rel_type(out)).to eq(:to_schema)
      expect(rel_body(out).schema).not_to be_nil
    end
  end

  describe "actions via FakeClient" do
    let(:schema) do
      SparkConnect::Types.struct(SparkConnect::Types.field("id", SparkConnect::Types.long))
    end

    before do
      fake_client.schema = schema
      fake_client.rows = [{ "id" => 1 }, { "id" => 2 }, { "id" => 3 }]
    end

    it "collect returns an Array of Rows" do
      rows = df.collect
      expect(rows).to be_an(Array)
      expect(rows.size).to eq(3)
      expect(rows).to all(be_a(SparkConnect::Row))
      expect(rows.first[0]).to eq(1)
    end

    it "count builds an aggregate plan and returns the integer" do
      result = df.count
      expect(result).to eq(1)
      last = fake_client.last_relation
      expect(last.rel_type).to eq(:aggregate)
      expect(last.aggregate.group_type).to eq(:GROUP_TYPE_GROUPBY)
    end

    it "take returns the first n rows via a limit plan" do
      rows = df.take(2)
      expect(rows.map { |r| r[0] }).to eq([1, 2, 3].first(rows.size))
      expect(fake_client.last_relation.rel_type).to eq(:limit)
    end

    it "first returns a single Row" do
      expect(df.first).to be_a(SparkConnect::Row)
    end

    it "head with no arg returns a single Row, head(n) returns an Array" do
      expect(df.head).to be_a(SparkConnect::Row)
      expect(df.head(2)).to be_an(Array)
    end

    it "show_string builds a show_string plan and returns a String" do
      fake_client.schema = SparkConnect::Types.struct(
        SparkConnect::Types.field("show", SparkConnect::Types.string)
      )
      fake_client.rows = [{ "show" => "+--+\n|id|\n+--+" }]
      out = df.show_string(5)
      expect(out).to be_a(String)
      expect(fake_client.last_relation.rel_type).to eq(:show_string)
      expect(fake_client.last_relation.show_string.num_rows).to eq(5)
    end

    it "schema/columns/dtypes come from analyze" do
      expect(df.schema).to be_a(SparkConnect::Types::StructType)
      expect(df.columns).to eq(%w[id])
      expect(df.dtypes).to eq([%w[id bigint]])
      expect(fake_client.analyze_requests).not_to be_empty
    end

    it "print_schema writes the tree string" do
      io = StringIO.new
      df.print_schema(io)
      expect(io.string).to include("root")
    end
  end
end
# rubocop:enable RSpec/SpecFilePathFormat
