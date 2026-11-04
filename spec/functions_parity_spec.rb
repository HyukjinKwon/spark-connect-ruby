# frozen_string_literal: true

# Function-library parity with PySpark 4.3.0 Spark Connect: completeness, and
# the argument semantics (literal vs column name, defaults, client-side seeds)
# that PySpark uses when building each function's protobuf expression.
RSpec.describe SparkConnect::Functions do
  def uf(col)
    col.to_expr.unresolved_function
  end

  def arg_kinds(col)
    uf(col).arguments.map(&:expr_type)
  end

  # Every public function of pyspark.sql.connect.functions in Spark 4.3.0
  # (except the UDF family, covered separately below).
  PYSPARK_430_FUNCTIONS = %w[
    abs acos acosh add_months aes_decrypt aes_encrypt aggregate any_value approxCountDistinct
    approx_count_distinct approx_percentile array array_agg array_append array_compact array_contains
    array_distinct array_except array_insert array_intersect array_join array_max array_min array_position
    array_prepend array_remove array_repeat array_size array_sort array_union arrays_overlap arrays_zip asc
    asc_nulls_first asc_nulls_last ascii asin asinh assert_true atan atan2 atanh avg base64 bin bit_and
    bit_count bit_get bit_length bit_or bit_xor bitmap_and_agg bitmap_bit_position bitmap_bucket_number
    bitmap_construct_agg bitmap_count bitmap_or_agg bitwiseNOT bitwise_not bool_and bool_or broadcast bround
    btrim bucket call_function call_udf cardinality cbrt ceil ceiling char char_length character_length chr
    coalesce col collate collation collect_list collect_set collect_union column concat concat_ws contains
    conv convert_timezone corr cos cosh cot count countDistinct count_distinct count_if count_min_sketch
    counter_diff covar_pop covar_samp crc32 create_map csc cume_dist curdate current_catalog current_database
    current_date current_path current_schema current_time current_timestamp current_timezone current_user
    date_add date_diff date_format date_from_unix_date date_part date_sub date_trunc dateadd datediff
    datepart day dayname dayofmonth dayofweek dayofyear days decode degrees dense_rank desc desc_nulls_first
    desc_nulls_last e element_at elt encode endswith equal_null every exists exp explode explode_outer expm1
    expr extract factorial filter find_in_set first first_value flatten floor forall format_number
    format_string from_base32 from_csv from_json from_unixtime from_utc_timestamp from_xml get
    get_json_object getbit greatest grouping grouping_id hash hex histogram_numeric hll_sketch_agg
    hll_sketch_estimate hll_union hll_union_agg hmac hour hours hypot ifnull ilike initcap inline
    inline_outer input_file_block_length input_file_block_start input_file_name instr is_valid_utf8
    is_valid_variant is_variant_null isnan isnotnull isnull jaro_winkler_similarity java_method
    json_array_length json_object_keys json_tuple kll_merge_agg_bigint kll_merge_agg_double
    kll_merge_agg_float kll_sketch_agg_bigint kll_sketch_agg_double kll_sketch_agg_float
    kll_sketch_get_n_bigint kll_sketch_get_n_double kll_sketch_get_n_float kll_sketch_get_quantile_bigint
    kll_sketch_get_quantile_double kll_sketch_get_quantile_float kll_sketch_get_rank_bigint
    kll_sketch_get_rank_double kll_sketch_get_rank_float kll_sketch_merge_bigint kll_sketch_merge_double
    kll_sketch_merge_float kll_sketch_to_string_bigint kll_sketch_to_string_double kll_sketch_to_string_float
    kurtosis lag last last_day last_value lcase lead least left length levenshtein like listagg
    listagg_distinct lit ln localtimestamp locate log log10 log1p log2 lower lpad ltrim make_date
    make_dt_interval make_interval make_time make_timestamp make_timestamp_ltz make_timestamp_ntz
    make_valid_utf8 make_ym_interval map_concat map_contains_key map_entries map_filter map_from_arrays
    map_from_entries map_keys map_values map_zip_with mask max max_by md5 mean median min min_by minute mode
    monotonically_increasing_id month monthname months months_between named_struct nanvl negate negative
    next_day now nth_value ntile nullif nullifzero nvl nvl2 octet_length overlay parse_json parse_url
    percent_rank percentile percentile_approx pi pmod posexplode posexplode_outer position positive pow power
    printf product quarter quote radians raise_error rand randn random randstr rank reduce reflect regexp
    regexp_count regexp_extract regexp_extract_all regexp_instr regexp_like regexp_replace regexp_substr
    regr_avgx regr_avgy regr_count regr_intercept regr_r2 regr_slope regr_sxx regr_sxy regr_syy repeat
    replace reverse right rint rlike round row_number rpad rtrim schema_of_csv schema_of_json
    schema_of_variant schema_of_variant_agg schema_of_xml sec second sentences sequence session_user
    session_window sha sha1 sha2 shiftLeft shiftRight shiftRightUnsigned shiftleft shiftright
    shiftrightunsigned shuffle sign signum sin sinh size skewness slice some sort_array soundex
    spark_partition_id split split_part sqrt st_asbinary st_geogfromwkb st_geomfromwkb st_setsrid st_srid
    stack startswith std stddev stddev_pop stddev_samp str_to_map string_agg string_agg_distinct struct
    substr substring substring_index sum sumDistinct sum_distinct tan tanh theta_difference
    theta_intersection theta_intersection_agg theta_sketch_agg theta_sketch_estimate theta_union
    theta_union_agg time_bucket time_diff time_from_micros time_from_millis time_from_seconds time_to_micros
    time_to_millis time_to_seconds time_trunc timestamp_add timestamp_diff timestamp_micros timestamp_millis
    timestamp_nanos timestamp_seconds toDegrees toRadians to_base32 to_binary to_char to_csv to_date to_json
    to_number to_time to_timestamp to_timestamp_ltz to_timestamp_ntz to_unix_timestamp to_utc_timestamp
    to_varchar to_variant_object to_xml transform transform_keys transform_values translate trim trunc
    try_add try_aes_decrypt try_avg try_divide try_element_at try_make_interval try_make_timestamp
    try_make_timestamp_ltz try_make_timestamp_ntz try_mod try_multiply try_parse_json try_parse_url
    try_reflect try_subtract try_sum try_to_binary try_to_date try_to_number try_to_time try_to_timestamp
    try_url_decode try_validate_utf8 try_variant_array_append try_variant_get try_variant_insert
    try_variant_set tuple_difference_double tuple_difference_integer tuple_difference_theta_double
    tuple_difference_theta_integer tuple_intersection_agg_double tuple_intersection_agg_integer
    tuple_intersection_double tuple_intersection_integer tuple_intersection_theta_double
    tuple_intersection_theta_integer tuple_sketch_agg_double tuple_sketch_agg_integer
    tuple_sketch_estimate_double tuple_sketch_estimate_integer tuple_sketch_summary_double
    tuple_sketch_summary_integer tuple_sketch_theta_double tuple_sketch_theta_integer tuple_union_agg_double
    tuple_union_agg_integer tuple_union_double tuple_union_integer tuple_union_theta_double
    tuple_union_theta_integer typeof ucase unbase64 unhex uniform unix_date unix_micros unix_millis
    unix_nanos unix_seconds unix_timestamp unwrap_udt upper url_decode url_encode user uuid validate_utf8
    var_pop var_samp variance variant_array_append variant_delete variant_get variant_insert variant_set
    variant_strip_nulls vector_avg vector_cosine_similarity vector_inner_product vector_l2_distance
    vector_norm vector_normalize vector_sum version weekday weekofyear when width_bucket window window_time
    xpath xpath_boolean xpath_double xpath_float xpath_int xpath_long xpath_number xpath_short xpath_string
    xxhash64 year years zeroifnull zip_with
  ].freeze

  it "defines every PySpark 4.3.0 function" do
    missing = PYSPARK_430_FUNCTIONS.reject { |n| f.respond_to?(n) }
    expect(missing).to be_empty
  end

  it "raises NotImplementedError for the UDF family" do
    %i[udf udtf arrow_udtf].each do |fn|
      expect { f.public_send(fn, ->(x) { x }) }.to raise_error(SparkConnect::NotImplementedError)
    end
  end

  describe "SQL function names that differ from the Ruby name" do
    it "maps bitwise_not/bitwiseNOT to ~, mean to avg, negate to negative, pow/power to power" do
      expect(uf(f.bitwise_not("a")).function_name).to eq("~")
      expect(uf(f.bitwiseNOT("a")).function_name).to eq("~")
      expect(uf(f.mean("a")).function_name).to eq("avg")
      expect(uf(f.negate("a")).function_name).to eq("negative")
      expect(uf(f.pow("a", 2)).function_name).to eq("power")
      expect(uf(f.power("a", 2)).function_name).to eq("power")
    end

    it "maps timestamp_add/timestamp_diff to timestampadd/timestampdiff with a literal unit" do
      add = uf(f.timestamp_add("HOUR", "q", "ts"))
      expect(add.function_name).to eq("timestampadd")
      expect(add.arguments[0].literal.string).to eq("HOUR")
      expect(uf(f.timestamp_diff("DAY", "a", "b")).function_name).to eq("timestampdiff")
    end

    it "maps log(x) to ln and log(base, x) to log with a literal base" do
      expect(uf(f.log("x")).function_name).to eq("ln")
      two = uf(f.log(10, "x"))
      expect(two.function_name).to eq("log")
      expect(two.arguments[0].literal.integer).to eq(10)
      expect(two.arguments[1].unresolved_attribute.unparsed_identifier).to eq("x")
    end

    it "keeps the PySpark camelCase aliases" do
      expect(uf(f.shiftLeft("a", 1)).function_name).to eq("shiftleft")
      expect(uf(f.toDegrees("a")).function_name).to eq("degrees")
      expect(uf(f.sumDistinct("a")).is_distinct).to be(true)
      expect(uf(f.approxCountDistinct("a")).function_name).to eq("approx_count_distinct")
    end
  end

  describe "String arguments that PySpark treats as column names" do
    {
      date_add: %w[d n], date_sub: %w[d n], add_months: %w[d n], array_repeat: %w[a n],
      repeat: %w[s n], get: %w[a i], substring: %w[s p l], slice: %w[a p l],
      overlay: %w[s r p l], regexp_like: %w[s re], regexp_count: %w[s re],
      regexp_substr: %w[s re], regexp_extract_all: %w[s re],
    }.each do |fn, args|
      it "#{fn} resolves every String argument as a column" do
        expect(arg_kinds(f.public_send(fn, *args)).uniq).to eq([:unresolved_attribute])
      end
    end

    it "split keeps the pattern literal but takes a column-name limit" do
      e = uf(f.split("s", ",", "lim"))
      expect(e.arguments.map(&:expr_type)).to eq(%i[unresolved_attribute literal unresolved_attribute])
    end

    it "array_insert takes a column-name position and a literal value" do
      expect(arg_kinds(f.array_insert("a", "p", "v"))).to eq(%i[unresolved_attribute unresolved_attribute literal])
    end

    it "printf and position resolve their String arguments as columns" do
      expect(arg_kinds(f.printf("fmt", "a"))).to eq(%i[unresolved_attribute unresolved_attribute])
      expect(arg_kinds(f.position("sub", "s"))).to eq(%i[unresolved_attribute unresolved_attribute])
    end
  end

  describe "trim functions" do
    it "send the trim characters first, like Spark's trim(trimStr, str)" do
      %i[trim ltrim rtrim].each do |fn|
        e = uf(f.public_send(fn, "s", "chars"))
        expect(e.arguments.map { |a| a.unresolved_attribute.unparsed_identifier }).to eq(%w[chars s])
      end
      expect(uf(f.trim("s")).arguments.size).to eq(1)
    end
  end

  describe "options maps" do
    it "send JSON/CSV/XML options as one map(k, v, ...) argument" do
      [f.from_json("c", "a INT", { "mode" => "FAILFAST" }), f.to_json("c", { "pretty" => true }),
       f.schema_of_json('{"a":1}', { "k" => "v" }), f.from_csv("c", "a INT", { "sep" => ";" }),
       f.to_csv("c", { "sep" => ";" }), f.schema_of_csv("1,2", { "sep" => "," }),
       f.from_xml("c", "a INT", { "rowTag" => "r" }), f.to_xml("c", { "rowTag" => "r" }),].each do |col|
        last = uf(col).arguments.last
        expect(last.unresolved_function.function_name).to eq("map")
        expect(last.unresolved_function.arguments.size).to eq(2)
      end
    end

    it "omits the options argument when none (or an empty Hash) is given" do
      expect(uf(f.to_json("c")).arguments.size).to eq(1)
      expect(uf(f.to_json("c", {})).arguments.size).to eq(1)
    end

    it "uses a DataType's JSON as the schema literal" do
      schema = SparkConnect::Types.struct(SparkConnect::Types.field("a", SparkConnect::Types.integer))
      expect(JSON.parse(uf(f.from_xml("c", schema)).arguments[1].literal.string)).to eq(JSON.parse(schema.json))
      expect(uf(f.from_json("c", f.lit("a INT"))).arguments[1].literal.string).to eq("a INT")
    end
  end

  describe "client-side seeds" do
    def seed_arg(col) = uf(col).arguments.last.literal

    it "seeds rand/randn/random/uuid/shuffle/randstr/uniform/count_min_sketch with a long when none is given" do
      [f.rand, f.randn, f.random, f.uuid, f.shuffle("a"), f.randstr(5), f.uniform(0, 1),
       f.count_min_sketch("a", 0.1, 0.9),].each do |col|
        expect(seed_arg(col).literal_type).to eq(:long)
      end
    end

    it "uses the given seed" do
      expect(seed_arg(f.rand(7)).integer).to eq(7)
      expect(seed_arg(f.uuid(3)).integer).to eq(3)
      expect(seed_arg(f.shuffle("a", 9)).integer).to eq(9)
    end
  end

  describe "optional arguments" do
    it "first/last/mode always send the boolean flag, like PySpark" do
      expect(uf(f.first("a")).arguments[1].literal.boolean).to be(false)
      expect(uf(f.last("a", true)).arguments[1].literal.boolean).to be(true)
      expect(uf(f.mode("a")).arguments[1].literal.boolean).to be(false)
    end

    it "lag/lead omit an unset default and round/bround an unset scale" do
      expect(uf(f.lag("a")).arguments.size).to eq(2)
      expect(uf(f.lead("a", 2, 0)).arguments.size).to eq(3)
      expect(uf(f.round("a")).arguments.size).to eq(1)
      expect(uf(f.bround("a", 2)).arguments[1].literal.integer).to eq(2)
    end

    it "supports the Spark 4.2/4.3 overloads" do
      expect(uf(f.instr("s", "x", 2)).arguments.size).to eq(3)
      four = uf(f.instr("s", "x", nil, 2))
      expect(four.arguments[2].literal.integer).to eq(1) # start defaults to 1 with an occurrence
      expect(uf(f.max_by("v", "o", 3)).arguments[2].literal.integer).to eq(3)
      expect(uf(f.min_by("v", "o")).arguments.size).to eq(2)
      expect(uf(f.regexp_replace("s", "a", "b", 2)).arguments[3].literal.integer).to eq(2)
      expect(uf(f.st_asbinary("g", "NDR")).arguments[1].literal.string).to eq("NDR")
      expect(uf(f.st_geomfromwkb("w", 4326)).arguments[1].literal.integer).to eq(4326)
    end

    it "fills PySpark's defaults for make_*_interval, aes_*, mask, sentences, str_to_map" do
      secs = uf(f.make_dt_interval).arguments.last.literal.decimal
      expect([secs.value, secs.precision, secs.scale]).to eq(["0", 10, 0])
      expect(uf(f.make_interval("y")).arguments.size).to eq(7)
      aes = uf(f.aes_encrypt("in", "key")).arguments.map { |a| a.literal.string if a.expr_type == :literal }
      expect(aes.drop(2)).to eq(["GCM", "DEFAULT", "", ""])
      mask = uf(f.mask("s")).arguments
      expect(mask[1..3].map { |a| a.literal.string }).to eq(%w[X x n])
      expect(mask[4].literal.literal_type).to eq(:null)
      expect(uf(f.str_to_map("t")).arguments.drop(1).map { |a| a.literal.string }).to eq([",", ":"])
      expect(uf(f.sentences("t")).arguments.drop(1).map { |a| a.literal.string }).to eq(["", ""])
    end

    it "fills the Tuple-sketch defaults (lg_nom_entries 12, mode sum)" do
      args = uf(f.tuple_sketch_agg_double("k", "v")).arguments
      expect(args[2].literal.integer).to eq(12)
      expect(args[3].literal.string).to eq("sum")
      expect(uf(f.kll_sketch_agg_bigint("a", 200)).arguments[1].literal.integer).to eq(200)
    end

    it "builds both make_timestamp forms and rejects mixing them" do
      expect(uf(f.make_timestamp("y", "mo", "d", "h", "mi", "s")).arguments.size).to eq(6)
      expect(uf(f.make_timestamp("y", "mo", "d", "h", "mi", "s", timezone: "tz")).arguments.size).to eq(7)
      expect(uf(f.make_timestamp(date: "d", time: "t")).arguments.size).to eq(2)
      expect(uf(f.try_make_timestamp_ntz(date: "d", time: "t")).function_name).to eq("try_make_timestamp_ntz")
      expect { f.make_timestamp("y", "mo", "d", "h", "mi", "s", date: "d") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { f.make_timestamp_ntz }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "variant_set/try_variant_set always send create_if_missing" do
      expect(uf(f.variant_set("v", "$.a", "x")).arguments[3].literal.boolean).to be(true)
      expect(uf(f.try_variant_set("v", "$.a", "x", false)).arguments[3].literal.boolean).to be(false)
      expect(uf(f.variant_get("v", "$.a", "int")).arguments[1].literal.string).to eq("$.a")
    end

    it "builds window and session_window like PySpark" do
      expect(uf(f.window("ts", "10 minutes")).arguments.size).to eq(2)
      start_only = uf(f.window("ts", "10 minutes", nil, "1 minute")).arguments
      expect(start_only.map { |a| a.literal.string if a.expr_type == :literal }.compact)
        .to eq(["10 minutes", "10 minutes", "1 minute"])
      expect(uf(f.session_window("ts", "5 minutes")).arguments[1].literal.string).to eq("5 minutes")
      expect { f.window("ts", 10) }.to raise_error(SparkConnect::IllegalArgumentError)
    end
  end

  describe "special expressions" do
    it "call_function builds a CallFunction expression" do
      e = f.call_function("my_fn", "a", f.lit(1)).to_expr
      expect(e.expr_type).to eq(:call_function)
      expect(e.call_function.function_name).to eq("my_fn")
      expect(e.call_function.arguments.size).to eq(2)
    end

    it "call_udf builds an UnresolvedFunction by name" do
      expect(uf(f.call_udf("my_udf", "a")).function_name).to eq("my_udf")
    end

    it "listagg_distinct/string_agg_distinct are distinct aggregates" do
      expect(uf(f.listagg_distinct("a", ",")).is_distinct).to be(true)
      expect(uf(f.string_agg_distinct("a")).function_name).to eq("string_agg")
    end

    it "percentile builds an array() of literals for a list of percentages" do
      pct = uf(f.percentile("a", [0.25, 0.75])).arguments[1]
      expect(pct.unresolved_function.function_name).to eq("array")
      expect(uf(f.percentile_approx("a", 0.5)).arguments[2].literal.integer).to eq(10_000)
    end

    it "reduce and array_sort accept lambdas" do
      r = uf(f.reduce("arr", f.lit(0), ->(acc, x) { acc + x }))
      expect(r.arguments[2].expr_type).to eq(:lambda_function)
      s = uf(f.array_sort("arr") { |a, b| a - b })
      expect(s.arguments[1].lambda_function.arguments.size).to eq(2)
    end

    it "col supports the t.* star target" do
      star = f.col("t.*").to_expr
      expect(star.expr_type).to eq(:unresolved_star)
      expect(star.unresolved_star.unparsed_target).to eq("t.*")
    end

    it "count of a star column counts rows" do
      expect(uf(f.count(f.col("*"))).arguments[0].literal.integer).to eq(1)
    end
  end

  describe "argument validation" do
    it "rejects invalid arguments like PySpark" do
      expect { f.sha2("a", 100) }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { f.greatest("a") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { f.json_tuple("a") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { f.variant_delete("v") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { f.when("cond", 1) }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { f.broadcast("df") }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { f.percentile("a", 0.5, 1.5) }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { f.raise_error(1) }.to raise_error(SparkConnect::IllegalArgumentError)
      expect { f.transform("a") }.to raise_error(SparkConnect::IllegalArgumentError)
    end
  end
end
