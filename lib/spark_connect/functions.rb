# frozen_string_literal: true

module SparkConnect
  # The standard Spark SQL function library, mirroring PySpark's
  # `pyspark.sql.functions` (Spark Connect flavour). Every function returns a
  # {Column}.
  #
  # Available both as `SparkConnect::Functions` and the shorthand
  # `SparkConnect::F`. All methods are module functions.
  #
  # Arguments follow PySpark's typing exactly, so PySpark code ports verbatim:
  #
  # * A parameter that PySpark types as `ColumnOrName` treats a {String} (or
  #   Symbol) as a **column name** (e.g. `F.sum("salary")`,
  #   `F.regexp_like("s", "pattern_col")`); wrap a literal with {lit}
  #   (`F.regexp_like("s", F.lit("a.*"))`). Non-String values such as numbers
  #   and booleans become literals.
  # * A parameter that PySpark types as a plain Python value (`str`, `int`, ...)
  #   is always a **literal** (e.g. the format of {date_format}, the pattern of
  #   {regexp_extract}, the delimiter of {concat_ws}).
  #
  # Functions that PySpark seeds on the client when no seed is given ({rand},
  # {randn}, {uuid}, {shuffle}, {randstr}, {uniform}, {count_min_sketch}) do the
  # same here, so a DataFrame built with them returns the same values on every
  # action.
  #
  # @example
  #   F = SparkConnect::F
  #   F.col("a") + F.lit(1)
  #   F.when(F.col("x") > 0, "pos").otherwise("non-pos")
  #   F.sum("amount").alias("total")
  module Functions
    Proto = SparkConnect::Proto
    extend self

    # ---- Core constructors -------------------------------------------------

    # A column reference by name. `"*"` selects all columns and `"t.*"` all
    # columns of the (aliased) relation `t`.
    # @return [Column]
    def col(name) = Column.from_name(name.to_s)
    alias column col

    # A literal value column. See {Column.lit} for supported Ruby types.
    # @return [Column]
    def lit(value) = Column.lit(value)

    # Parse a SQL expression string into a {Column}.
    # @return [Column]
    def expr(sql)
      Column.from_expr(Proto::Expression.new(expression_string: Proto::Expression::ExpressionString.new(expression: sql)))
    end

    # @return [Column] an ascending sort order for the named/given column.
    def asc(col) = _col(col).asc
    # @return [Column] a descending sort order for the named/given column.
    def desc(col) = _col(col).desc
    # @return [Column] ascending, nulls first.
    def asc_nulls_first(col) = _col(col).asc_nulls_first
    # @return [Column] ascending, nulls last.
    def asc_nulls_last(col) = _col(col).asc_nulls_last
    # @return [Column] descending, nulls first.
    def desc_nulls_first(col) = _col(col).desc_nulls_first
    # @return [Column] descending, nulls last.
    def desc_nulls_last(col) = _col(col).desc_nulls_last

    # Start a CASE WHEN expression. Chain {Column#when} / {Column#otherwise}.
    #
    # @param condition [Column] a boolean column.
    # @param value [Object] the value (a {Column} or a literal) for this branch.
    # @return [Column]
    def when(condition, value)
      raise IllegalArgumentError, "when() condition must be a Column, got #{condition.class}" unless condition.is_a?(Column)

      Column.invoke("when", condition, lit(value))
    end

    # Call a SQL function (built-in or registered) by name. Unlike
    # {call_udf}, this resolves the name through the session catalog, so a
    # qualified name (`"catalog.db.fn"`) or a built-in works.
    #
    # @param func_name [String]
    # @param cols [Array<Column, String>] arguments; Strings are column names.
    # @return [Column]
    def call_function(func_name, *cols)
      Column.new(Proto::Expression.new(
                   call_function: Proto::CallFunction.new(
                     function_name: func_name.to_s, arguments: cols.map { |c| _col(c).to_expr }
                   )
                 ))
    end

    # Call a function registered on the server (e.g. a UDF registered through
    # another client) by name.
    #
    # @param udf_name [String]
    # @param cols [Array<Column, String>] arguments; Strings are column names.
    # @return [Column]
    def call_udf(udf_name, *cols) = _invoke(udf_name.to_s, *cols)

    # ---- Aggregate / counting ---------------------------------------------

    # @return [Column] count of rows (or non-null values of a column). `"*"`
    #   (or `F.col("*")`) counts all rows.
    def count(col)
      return Column.invoke("count", lit(1)) if col.to_s == "*" || _star?(col)

      Column.invoke("count", _col(col))
    end

    # @return [Column] count of distinct combinations of the given columns.
    def count_distinct(col, *cols)
      Column.invoke("count", *[col, *cols].map { |c| _col(c) }, is_distinct: true)
    end
    alias countDistinct count_distinct

    # @return [Column] approximate distinct count (optionally with relative SD).
    def approx_count_distinct(col, rsd = nil)
      rsd.nil? ? Column.invoke("approx_count_distinct", _col(col)) : Column.invoke("approx_count_distinct", _col(col), lit(rsd))
    end
    alias approxCountDistinct approx_count_distinct

    # @return [Column] sum of distinct values.
    def sum_distinct(col) = Column.invoke("sum", _col(col), is_distinct: true)
    alias sumDistinct sum_distinct

    # @return [Column] the average of a column (alias of {avg}).
    def mean(col) = avg(col)

    # @param ignorenulls [Boolean] skip nulls and return the first non-null value.
    # @return [Column] the first value in a group.
    def first(col, ignorenulls = false) = Column.invoke("first", _col(col), lit(ignorenulls))

    # @param ignorenulls [Boolean] skip nulls and return the last non-null value.
    # @return [Column] the last value in a group.
    def last(col, ignorenulls = false) = Column.invoke("last", _col(col), lit(ignorenulls))

    # The most frequent value in a group.
    #
    # @param deterministic [Boolean] break ties deterministically (lowest value).
    # @note The two-argument form requires a Spark 4.0+ server.
    # @return [Column]
    def mode(col, deterministic = false) = Column.invoke("mode", _col(col), lit(deterministic))

    # The value of `col` associated with the maximum `ord`. With `k`, returns an
    # array of the values for the top `k` orderings (Spark 4.2+).
    # @return [Column]
    def max_by(col, ord, k = nil)
      k.nil? ? _invoke("max_by", col, ord) : Column.invoke("max_by", _col(col), _col(ord), lit(k))
    end

    # The value of `col` associated with the minimum `ord`. With `k`, returns an
    # array of the values for the bottom `k` orderings (Spark 4.2+).
    # @return [Column]
    def min_by(col, ord, k = nil)
      k.nil? ? _invoke("min_by", col, ord) : Column.invoke("min_by", _col(col), _col(ord), lit(k))
    end

    # The exact percentile(s) of a numeric column.
    #
    # @param percentage [Float, Array<Float>, Column] value(s) in 0.0..1.0.
    # @param frequency [Integer, Column] a positive weight per row.
    # @return [Column]
    def percentile(col, percentage, frequency = 1)
      unless frequency.is_a?(Integer) || frequency.is_a?(Column)
        raise IllegalArgumentError, "percentile frequency must be an Integer or Column, got #{frequency.class}"
      end

      Column.invoke("percentile", _col(col), _percentage(percentage), lit(frequency))
    end

    # The approximate percentile(s) of a numeric column.
    #
    # @param percentage [Float, Array<Float>, Column] value(s) in 0.0..1.0.
    # @param accuracy [Integer, Column] higher is more accurate (default 10000).
    # @return [Column]
    def percentile_approx(col, percentage, accuracy = 10_000)
      Column.invoke("percentile_approx", _col(col), _percentage(percentage), lit(accuracy))
    end

    # Alias of {percentile_approx} under the `approx_percentile` SQL name.
    # @return [Column]
    def approx_percentile(col, percentage, accuracy = 10_000)
      Column.invoke("approx_percentile", _col(col), _percentage(percentage), lit(accuracy))
    end

    # A count-min sketch of a column (binary), for approximate frequencies.
    #
    # @param eps [Float, Column] relative error, in (0, 1).
    # @param confidence [Float, Column] confidence, in (0, 1).
    # @param seed [Integer, Column, nil] random seed (generated when omitted).
    # @return [Column]
    def count_min_sketch(col, eps, confidence, seed = nil)
      Column.invoke("count_min_sketch", _col(col), lit(eps), lit(confidence), _seed(seed))
    end

    # Concatenate the non-null values of a group, separated by `delimiter`.
    # @param delimiter [String, Column, nil] a literal separator.
    # @return [Column]
    def listagg(col, delimiter = nil) = _agg_with_delimiter("listagg", col, delimiter)

    # Like {listagg} over distinct values.
    # @return [Column]
    def listagg_distinct(col, delimiter = nil) = _agg_with_delimiter("listagg", col, delimiter, is_distinct: true)

    # Alias of {listagg} under the `string_agg` SQL name.
    # @return [Column]
    def string_agg(col, delimiter = nil) = _agg_with_delimiter("string_agg", col, delimiter)

    # Like {string_agg} over distinct values.
    # @return [Column]
    def string_agg_distinct(col, delimiter = nil) = _agg_with_delimiter("string_agg", col, delimiter, is_distinct: true)

    # ---- Sketches: HLL, KLL, Theta, Tuple ---------------------------------

    # An HLL sketch (binary) of a column.
    # @param lg_config_k [Integer, Column, nil] log2 of the number of buckets.
    # @return [Column]
    def hll_sketch_agg(col, lg_config_k = nil) = _optional_literal_tail("hll_sketch_agg", [col], [lg_config_k])

    # The union of two HLL sketches.
    # @param allow_different_lg_config_k [Boolean, nil]
    # @return [Column]
    def hll_union(col1, col2, allow_different_lg_config_k = nil)
      _optional_literal_tail("hll_union", [col1, col2], [allow_different_lg_config_k])
    end

    # Aggregate union of HLL sketches.
    # @param allow_different_lg_config_k [Boolean, Column, nil]
    # @return [Column]
    def hll_union_agg(col, allow_different_lg_config_k = nil)
      _optional_literal_tail("hll_union_agg", [col], [allow_different_lg_config_k])
    end

    %w[bigint double float].each do |kind|
      define_method("kll_sketch_agg_#{kind}") { |col, k = nil| _optional_literal_tail("kll_sketch_agg_#{kind}", [col], [k]) }
      define_method("kll_merge_agg_#{kind}") { |col, k = nil| _optional_literal_tail("kll_merge_agg_#{kind}", [col], [k]) }
    end

    # @!method kll_sketch_agg_bigint(col, k = nil)
    #   A KLL quantiles sketch of BIGINT values. @return [Column]
    # @!method kll_sketch_agg_double(col, k = nil)
    #   A KLL quantiles sketch of DOUBLE values. @return [Column]
    # @!method kll_sketch_agg_float(col, k = nil)
    #   A KLL quantiles sketch of FLOAT values. @return [Column]
    # @!method kll_merge_agg_bigint(col, k = nil)
    #   Merge BIGINT KLL sketches. @return [Column]
    # @!method kll_merge_agg_double(col, k = nil)
    #   Merge DOUBLE KLL sketches. @return [Column]
    # @!method kll_merge_agg_float(col, k = nil)
    #   Merge FLOAT KLL sketches. @return [Column]

    # A Theta sketch (binary) of a column.
    # @param lg_nom_entries [Integer, Column, nil]
    # @return [Column]
    def theta_sketch_agg(col, lg_nom_entries = nil) = _optional_literal_tail("theta_sketch_agg", [col], [lg_nom_entries])

    # The union of two Theta sketches.
    # @return [Column]
    def theta_union(col1, col2, lg_nom_entries = nil) = _optional_literal_tail("theta_union", [col1, col2], [lg_nom_entries])

    # Aggregate union of Theta sketches.
    # @return [Column]
    def theta_union_agg(col, lg_nom_entries = nil) = _optional_literal_tail("theta_union_agg", [col], [lg_nom_entries])

    %w[double integer].each do |kind|
      define_method("tuple_intersection_agg_#{kind}") do |col, mode = nil|
        _optional_literal_tail("tuple_intersection_agg_#{kind}", [col], [mode])
      end
      define_method("tuple_intersection_#{kind}") do |col1, col2, mode = nil|
        _optional_literal_tail("tuple_intersection_#{kind}", [col1, col2], [mode])
      end
      define_method("tuple_intersection_theta_#{kind}") do |col1, col2, mode = nil|
        _optional_literal_tail("tuple_intersection_theta_#{kind}", [col1, col2], [mode])
      end
      define_method("tuple_sketch_summary_#{kind}") do |col, mode = nil|
        _optional_literal_tail("tuple_sketch_summary_#{kind}", [col], [mode])
      end
      define_method("tuple_sketch_agg_#{kind}") do |key, summary, lg_nom_entries = nil, mode = nil|
        Column.invoke("tuple_sketch_agg_#{kind}", _col(key), _col(summary), *_tuple_defaults(lg_nom_entries, mode))
      end
      define_method("tuple_union_agg_#{kind}") do |col, lg_nom_entries = nil, mode = nil|
        Column.invoke("tuple_union_agg_#{kind}", _col(col), *_tuple_defaults(lg_nom_entries, mode))
      end
      define_method("tuple_union_#{kind}") do |col1, col2, lg_nom_entries = nil, mode = nil|
        Column.invoke("tuple_union_#{kind}", _col(col1), _col(col2), *_tuple_defaults(lg_nom_entries, mode))
      end
      define_method("tuple_union_theta_#{kind}") do |col1, col2, lg_nom_entries = nil, mode = nil|
        Column.invoke("tuple_union_theta_#{kind}", _col(col1), _col(col2), *_tuple_defaults(lg_nom_entries, mode))
      end
    end

    # @!method tuple_sketch_agg_double(key, summary, lg_nom_entries = nil, mode = nil)
    #   A Tuple sketch with DOUBLE summaries (`lg_nom_entries` defaults to 12, `mode` to `"sum"`). @return [Column]
    # @!method tuple_sketch_agg_integer(key, summary, lg_nom_entries = nil, mode = nil)
    #   A Tuple sketch with INTEGER summaries (`lg_nom_entries` defaults to 12, `mode` to `"sum"`). @return [Column]
    # @!method tuple_union_agg_double(col, lg_nom_entries = nil, mode = nil)
    #   Aggregate union of DOUBLE Tuple sketches. @return [Column]
    # @!method tuple_union_agg_integer(col, lg_nom_entries = nil, mode = nil)
    #   Aggregate union of INTEGER Tuple sketches. @return [Column]
    # @!method tuple_union_double(col1, col2, lg_nom_entries = nil, mode = nil)
    #   Union of two DOUBLE Tuple sketches. @return [Column]
    # @!method tuple_union_integer(col1, col2, lg_nom_entries = nil, mode = nil)
    #   Union of two INTEGER Tuple sketches. @return [Column]
    # @!method tuple_union_theta_double(col1, col2, lg_nom_entries = nil, mode = nil)
    #   Union of a DOUBLE Tuple sketch with a Theta sketch. @return [Column]
    # @!method tuple_union_theta_integer(col1, col2, lg_nom_entries = nil, mode = nil)
    #   Union of an INTEGER Tuple sketch with a Theta sketch. @return [Column]
    # @!method tuple_intersection_agg_double(col, mode = nil)
    #   Aggregate intersection of DOUBLE Tuple sketches. @return [Column]
    # @!method tuple_intersection_agg_integer(col, mode = nil)
    #   Aggregate intersection of INTEGER Tuple sketches. @return [Column]
    # @!method tuple_intersection_double(col1, col2, mode = nil)
    #   Intersection of two DOUBLE Tuple sketches. @return [Column]
    # @!method tuple_intersection_integer(col1, col2, mode = nil)
    #   Intersection of two INTEGER Tuple sketches. @return [Column]
    # @!method tuple_intersection_theta_double(col1, col2, mode = nil)
    #   Intersection of a DOUBLE Tuple sketch with a Theta sketch. @return [Column]
    # @!method tuple_intersection_theta_integer(col1, col2, mode = nil)
    #   Intersection of an INTEGER Tuple sketch with a Theta sketch. @return [Column]
    # @!method tuple_sketch_summary_double(col, mode = nil)
    #   The aggregated DOUBLE summary of a Tuple sketch. @return [Column]
    # @!method tuple_sketch_summary_integer(col, mode = nil)
    #   The aggregated INTEGER summary of a Tuple sketch. @return [Column]

    # ---- Math / rounding ---------------------------------------------------

    # HALF_UP rounding to `scale` decimal places (0 when omitted).
    # @param scale [Integer, Column, String, nil]
    # @return [Column]
    def round(col, scale = nil) = scale.nil? ? _invoke("round", col) : _invoke("round", col, scale)

    # HALF_EVEN ("banker's") rounding to `scale` decimal places.
    # @param scale [Integer, Column, String, nil]
    # @return [Column]
    def bround(col, scale = nil) = scale.nil? ? _invoke("bround", col) : _invoke("bround", col, scale)

    # Logarithm. With one argument, the natural logarithm of `arg1`; with two,
    # the logarithm of `arg2` in base `arg1` (a literal).
    # @return [Column]
    def log(arg1, arg2 = nil)
      arg2.nil? ? Column.invoke("ln", _col(arg1)) : Column.invoke("log", lit(arg1), _col(arg2))
    end

    # `col1` raised to the power of `col2` (numbers are literals, Strings columns).
    # @return [Column]
    def pow(col1, col2) = _invoke("power", col1, col2)
    alias power pow

    # @return [Column] the bitwise NOT of a column.
    def bitwise_not(col) = _invoke("~", col)
    alias bitwiseNOT bitwise_not

    # @return [Column] the negation of a column (alias of {negative}).
    def negate(col) = negative(col)

    # @return [Column] left shift by a literal bit count.
    def shiftleft(col, num_bits) = Column.invoke("shiftleft", _col(col), lit(num_bits))
    # @return [Column] signed right shift by a literal bit count.
    def shiftright(col, num_bits) = Column.invoke("shiftright", _col(col), lit(num_bits))
    # @return [Column] unsigned right shift by a literal bit count.
    def shiftrightunsigned(col, num_bits) = Column.invoke("shiftrightunsigned", _col(col), lit(num_bits))
    alias shiftLeft shiftleft
    alias shiftRight shiftright
    alias shiftRightUnsigned shiftrightunsigned

    # @return [Column] radians converted to degrees (deprecated PySpark alias of {degrees}).
    def toDegrees(col) = degrees(col)
    # @return [Column] degrees converted to radians (deprecated PySpark alias of {radians}).
    def toRadians(col) = radians(col)

    # @return [Column] convert a number string from `from_base` to `to_base`.
    def conv(col, from_base, to_base) = Column.invoke("conv", _col(col), lit(from_base), lit(to_base))

    SHA2_BIT_LENGTHS = [0, 224, 256, 384, 512].freeze

    # SHA-2 hash with the given bit length.
    # @param num_bits [Integer] one of 0 (=256), 224, 256, 384, 512.
    # @return [Column]
    def sha2(col, num_bits)
      unless SHA2_BIT_LENGTHS.include?(num_bits)
        raise IllegalArgumentError, "sha2 num_bits must be one of #{SHA2_BIT_LENGTHS.inspect}, got #{num_bits.inspect}"
      end

      Column.invoke("sha2", _col(col), lit(num_bits))
    end

    # @return [Column] the greatest value of the given columns (at least two), skipping nulls.
    def greatest(*cols) = _at_least_two("greatest", cols)

    # @return [Column] the least value of the given columns (at least two), skipping nulls.
    def least(*cols) = _at_least_two("least", cols)

    # ---- Conditionals / null handling / errors ----------------------------

    # @return [Column] first non-null among the given columns.
    def coalesce(*cols) = _invoke("coalesce", *cols)
    # @return [Column] `col1` if it is not NaN, else `col2`.
    def nanvl(col1, col2) = _invoke("nanvl", col1, col2)

    # Raise an error when `col` is not true.
    # @param err_msg [String, Column, nil] the error message (a literal).
    # @return [Column]
    def assert_true(col, err_msg = nil)
      return _invoke("assert_true", col) if err_msg.nil?

      _check_str_or_col("err_msg", err_msg)
      Column.invoke("assert_true", _col(col), lit(err_msg))
    end

    # Raise an error with the given message.
    # @param err_msg [String, Column] the error message (a literal).
    # @return [Column]
    def raise_error(err_msg)
      _check_str_or_col("err_msg", err_msg)
      Column.invoke("raise_error", lit(err_msg))
    end

    # ---- Constructors of complex types ------------------------------------

    # @return [Column] a struct from the given columns (or one Array of them).
    def struct(*cols) = _invoke("struct", *_unwrap_list(cols))
    # @return [Column] an array from the given columns (or one Array of them).
    def array(*cols) = _invoke("array", *_unwrap_list(cols))
    # @return [Column] a map from alternating key/value columns (or one Array of them).
    def create_map(*cols) = _invoke("map", *_unwrap_list(cols))
    # @return [Column] a map from two array columns (keys, values).
    def map_from_arrays(keys, values) = _invoke("map_from_arrays", keys, values)
    # @return [Column] a named struct from alternating name/value arguments.
    def named_struct(*cols) = _invoke("named_struct", *cols)
    # @return [Column] the union of the given maps (or one Array of them).
    def map_concat(*cols) = _invoke("map_concat", *_unwrap_list(cols))

    # ---- String functions --------------------------------------------------

    # @return [Column] concatenation of columns separated by literal `sep`.
    def concat_ws(sep, *cols) = Column.invoke("concat_ws", lit(sep), *cols.map { |c| _col(c) })
    # @return [Column] printf-style formatting using literal `fmt`.
    def format_string(fmt, *cols) = Column.invoke("format_string", lit(fmt), *cols.map { |c| _col(c) })
    # @return [Column] number formatted to `d` decimal places.
    def format_number(col, d) = Column.invoke("format_number", _col(col), lit(d))

    # Substring of length `len` from 1-based `pos`.
    # @param pos [Integer, Column, String] Strings are column names.
    # @param len [Integer, Column, String] Strings are column names.
    # @return [Column]
    def substring(col, pos, len) = _invoke("substring", col, pos, len)

    # @return [Column] substring before the `count`-th occurrence of literal `delim`.
    def substring_index(col, delim, count) = Column.invoke("substring_index", _col(col), lit(delim), lit(count))

    # The 1-based position of `substr` within `col` (0 if absent). `start`
    # begins the search at a 1-based position and `occurrence` picks the n-th
    # match (Spark 4.3+ for those two).
    #
    # @param substr [String, Column] a literal substring.
    # @param start [Integer, Column, nil]
    # @param occurrence [Integer, Column, nil]
    # @return [Column]
    def instr(col, substr, start = nil, occurrence = nil)
      if start.nil? && occurrence.nil?
        Column.invoke("instr", _col(col), lit(substr))
      elsif occurrence.nil?
        Column.invoke("instr", _col(col), lit(substr), lit(start))
      else
        Column.invoke("instr", _col(col), lit(substr), lit(start.nil? ? 1 : start), lit(occurrence))
      end
    end

    # @return [Column] 1-based position of literal `substr` in `col` at/after `pos`.
    def locate(substr, col, pos = 1) = Column.invoke("locate", lit(substr), _col(col), lit(pos))
    # @return [Column] left-padded string (`len` and `pad` are literals).
    def lpad(col, len, pad) = Column.invoke("lpad", _col(col), lit(len), lit(pad))
    # @return [Column] right-padded string (`len` and `pad` are literals).
    def rpad(col, len, pad) = Column.invoke("rpad", _col(col), lit(len), lit(pad))

    # @param n [Integer, Column, String] repeat count; Strings are column names.
    # @return [Column] the string repeated `n` times.
    def repeat(col, n) = _invoke("repeat", col, n)

    # Split `col` around matches of the literal regex `pattern`.
    # @param limit [Integer, Column, String] at most `limit` parts (-1 = no
    #   limit); Strings are column names.
    # @return [Column]
    def split(col, pattern, limit = -1) = Column.invoke("split", _col(col), lit(pattern), _col(limit))

    # @return [Column] characters of `col` matching `matching` replaced per `replace` (literals).
    def translate(col, matching, replace) = Column.invoke("translate", _col(col), lit(matching), lit(replace))

    # The `idx`-th group of the literal regex `pattern` matched in `col`.
    # @return [Column]
    def regexp_extract(col, pattern, idx = 0) = Column.invoke("regexp_extract", _col(col), lit(pattern), lit(idx))

    # All matches of group `idx` of `regexp` in `col`.
    # @param regexp [Column, String] a Column (e.g. `F.lit("(\\d+)")`); Strings are column names.
    # @param idx [Integer, Column, nil] the group index (1 when omitted).
    # @return [Column]
    def regexp_extract_all(col, regexp, idx = nil)
      idx.nil? ? _invoke("regexp_extract_all", col, regexp) : Column.invoke("regexp_extract_all", _col(col), _col(regexp), lit(idx))
    end

    # Replace all substrings of `col` matching `pattern` with `replacement`
    # (both literals or Columns). `position` starts the search at a 1-based
    # character position (Spark 4.3+).
    # @return [Column]
    def regexp_replace(col, pattern, replacement, position = nil)
      args = [_col(col), lit(pattern), lit(replacement)]
      args << lit(position) unless position.nil?
      Column.invoke("regexp_replace", *args)
    end

    # The 1-based position of the first match of `regexp` in `str` (0 if none).
    # @param regexp [Column, String] Strings are column names.
    # @param idx [Integer, Column, nil] the regex group index.
    # @return [Column]
    def regexp_instr(str, regexp, idx = nil)
      idx.nil? ? _invoke("regexp_instr", str, regexp) : Column.invoke("regexp_instr", _col(str), _col(regexp), lit(idx))
    end

    # Overlay `replace` into `src` at 1-based `pos` for `len` characters.
    # @param pos [Integer, Column, String] Strings are column names.
    # @param len [Integer, Column, String] -1 replaces `replace.length` characters.
    # @return [Column]
    def overlay(src, replace, pos, len = -1) = _invoke("overlay", src, replace, pos, len)

    # Trim spaces (or the characters of `trim`) from both ends of `col`.
    # @param trim [Column, String, nil] the trim characters; Strings are column names.
    # @return [Column]
    def trim(col, trim = nil) = _trim("trim", col, trim)
    # Trim spaces (or the characters of `trim`) from the start of `col`.
    # @return [Column]
    def ltrim(col, trim = nil) = _trim("ltrim", col, trim)
    # Trim spaces (or the characters of `trim`) from the end of `col`.
    # @return [Column]
    def rtrim(col, trim = nil) = _trim("rtrim", col, trim)

    # The Levenshtein distance between two strings; with `threshold`, -1 when
    # the distance exceeds it.
    # @return [Column]
    def levenshtein(left, right, threshold = nil)
      _optional_literal_tail("levenshtein", [left, right], [threshold])
    end

    # @return [Column] `col` decoded from binary using the literal `charset`.
    def decode(col, charset) = Column.invoke("decode", _col(col), lit(charset))
    # @return [Column] `col` encoded to binary using the literal `charset`.
    def encode(col, charset) = Column.invoke("encode", _col(col), lit(charset))
    # @return [Column] `col` with the literal `collation` applied.
    def collate(col, collation) = Column.invoke("collate", _col(col), lit(collation))

    # Split a string into arrays of sentences of words.
    # @param language [Column, String, nil] Strings are column names (default `""`).
    # @param country [Column, String, nil] Strings are column names (default `""`).
    # @return [Column]
    def sentences(string, language = nil, country = nil)
      Column.invoke("sentences", _col(string), _col_or_default(language, ""), _col_or_default(country, ""))
    end

    # Convert a delimited string into a map.
    # @param pair_delim [Column, String, nil] Strings are column names (default `","`).
    # @param key_value_delim [Column, String, nil] Strings are column names (default `":"`).
    # @return [Column]
    def str_to_map(text, pair_delim = nil, key_value_delim = nil)
      Column.invoke("str_to_map", _col(text), _col_or_default(pair_delim, ","), _col_or_default(key_value_delim, ":"))
    end

    # Mask a string: upper-case letters to `upper_char` (default `"X"`),
    # lower-case to `lower_char` (`"x"`), digits to `digit_char` (`"n"`), and
    # other characters to `other_char` (unchanged). Given values are Columns
    # or column names.
    # @return [Column]
    def mask(col, upper_char = nil, lower_char = nil, digit_char = nil, other_char = nil)
      Column.invoke("mask", _col(col), _col_or_default(upper_char, "X"), _col_or_default(lower_char, "x"),
                    _col_or_default(digit_char, "n"), other_char.nil? ? lit(nil) : _col(other_char))
    end

    # AES-encrypt `input` with `key` (`mode` defaults to `"GCM"`, `padding` to
    # `"DEFAULT"`, `iv` and `aad` to empty). Given values are Columns or column names.
    # @return [Column]
    def aes_encrypt(input, key, mode = nil, padding = nil, iv = nil, aad = nil)
      Column.invoke("aes_encrypt", _col(input), _col(key), _col_or_default(mode, "GCM"),
                    _col_or_default(padding, "DEFAULT"), _col_or_default(iv, ""), _col_or_default(aad, ""))
    end

    # AES-decrypt `input` with `key` (see {aes_encrypt} for the defaults).
    # @return [Column]
    def aes_decrypt(input, key, mode = nil, padding = nil, aad = nil) = _aes_decrypt("aes_decrypt", input, key, mode, padding, aad)

    # Like {aes_decrypt} but returns null on failure.
    # @return [Column]
    def try_aes_decrypt(input, key, mode = nil, padding = nil, aad = nil)
      _aes_decrypt("try_aes_decrypt", input, key, mode, padding, aad)
    end

    # ---- Date / time functions --------------------------------------------

    # @return [Column] `col` formatted with the literal `fmt`.
    def date_format(col, fmt) = Column.invoke("date_format", _col(col), lit(fmt))
    # @return [Column] `col` parsed to a date (optionally with literal `fmt`).
    def to_date(col, fmt = nil) = fmt ? Column.invoke("to_date", _col(col), lit(fmt)) : Column.invoke("to_date", _col(col))
    # @return [Column] `col` parsed to a timestamp (optionally with literal `fmt`).
    def to_timestamp(col, fmt = nil) = fmt ? Column.invoke("to_timestamp", _col(col), lit(fmt)) : Column.invoke("to_timestamp", _col(col))
    # @return [Column] like {to_date} but null on failure.
    def try_to_date(col, fmt = nil) = fmt ? Column.invoke("try_to_date", _col(col), lit(fmt)) : Column.invoke("try_to_date", _col(col))

    # @param days [Integer, Column, String] Strings are column names.
    # @return [Column] the date `days` days after `col`.
    def date_add(col, days) = _invoke("date_add", col, days)
    # @return [Column] alias of {date_add} under the `dateadd` SQL name.
    def dateadd(col, days) = _invoke("dateadd", col, days)
    # @return [Column] the date `days` days before `col`.
    def date_sub(col, days) = _invoke("date_sub", col, days)
    # @param months [Integer, Column, String] Strings are column names.
    # @return [Column] the date `months` months after `col`.
    def add_months(col, months) = _invoke("add_months", col, months)
    # @return [Column] the number of days from `start_col` to `end_col`.
    def datediff(end_col, start_col) = _invoke("datediff", end_col, start_col)
    # @return [Column] months between two dates (rounded to 8 digits unless `round_off` is false).
    def months_between(d1, d2, round_off = true) = Column.invoke("months_between", _col(d1), _col(d2), lit(round_off))
    # @return [Column] the first date after `col` that is the literal `day_of_week`.
    def next_day(col, day_of_week) = Column.invoke("next_day", _col(col), lit(day_of_week))
    # @return [Column] the date truncated to the literal unit `fmt`.
    def trunc(col, fmt) = Column.invoke("trunc", _col(col), lit(fmt))
    # @return [Column] the timestamp truncated to the literal unit `fmt`.
    def date_trunc(fmt, col) = Column.invoke("date_trunc", lit(fmt), _col(col))
    # @return [Column] epoch seconds formatted with the literal `fmt`.
    def from_unixtime(col, fmt = "yyyy-MM-dd HH:mm:ss") = Column.invoke("from_unixtime", _col(col), lit(fmt))

    # @return [Column] the current epoch seconds, or `col` parsed with the literal `fmt`.
    def unix_timestamp(col = nil, fmt = "yyyy-MM-dd HH:mm:ss")
      col.nil? ? Column.invoke("unix_timestamp") : Column.invoke("unix_timestamp", _col(col), lit(fmt))
    end

    # @param tz [String, Column] a literal time zone.
    # @return [Column] a UTC timestamp rendered in `tz`.
    def from_utc_timestamp(col, tz) = Column.invoke("from_utc_timestamp", _col(col), lit(tz))
    # @param tz [String, Column] a literal time zone.
    # @return [Column] a `tz` timestamp rendered in UTC.
    def to_utc_timestamp(col, tz) = Column.invoke("to_utc_timestamp", _col(col), lit(tz))
    # @return [Column] a date from year, month, and day columns.
    def make_date(year, month, day) = _invoke("make_date", year, month, day)

    # A day-time interval. Omitted parts are 0; given parts are Columns or column names.
    # @return [Column]
    def make_dt_interval(days = nil, hours = nil, mins = nil, secs = nil)
      Column.invoke("make_dt_interval", _col_or_default(days, 0), _col_or_default(hours, 0), _col_or_default(mins, 0),
                    _col_or_default(secs, BigDecimal("0")))
    end

    # A calendar interval. Omitted parts are 0; given parts are Columns or column names.
    # @return [Column]
    def make_interval(years = nil, months = nil, weeks = nil, days = nil, hours = nil, mins = nil, secs = nil)
      _make_interval("make_interval", years, months, weeks, days, hours, mins, secs)
    end

    # Like {make_interval} but null on overflow.
    # @return [Column]
    def try_make_interval(years = nil, months = nil, weeks = nil, days = nil, hours = nil, mins = nil, secs = nil)
      _make_interval("try_make_interval", years, months, weeks, days, hours, mins, secs)
    end

    # A year-month interval. Omitted parts are 0.
    # @return [Column]
    def make_ym_interval(years = nil, months = nil)
      Column.invoke("make_ym_interval", _col_or_default(years, 0), _col_or_default(months, 0))
    end

    # A timestamp from parts, either `(years, months, days, hours, mins, secs[, timezone])`
    # or `(date:, time:[, timezone:])` (the latter requires Spark 4.1+).
    #
    # @example
    #   F.make_timestamp("y", "mo", "d", "h", "mi", "s")
    #   F.make_timestamp(date: "d", time: "t", timezone: F.lit("UTC"))
    # @return [Column]
    def make_timestamp(*parts, date: nil, time: nil, timezone: nil)
      _make_timestamp("make_timestamp", parts, date, time, timezone, allow_timezone: true)
    end

    # Like {make_timestamp} but null on invalid input.
    # @return [Column]
    def try_make_timestamp(*parts, date: nil, time: nil, timezone: nil)
      _make_timestamp("try_make_timestamp", parts, date, time, timezone, allow_timezone: true)
    end

    # A TIMESTAMP_NTZ from `(years, months, days, hours, mins, secs)` or `(date:, time:)`.
    # @return [Column]
    def make_timestamp_ntz(*parts, date: nil, time: nil)
      _make_timestamp("make_timestamp_ntz", parts, date, time, nil, allow_timezone: false)
    end

    # Like {make_timestamp_ntz} but null on invalid input.
    # @return [Column]
    def try_make_timestamp_ntz(*parts, date: nil, time: nil)
      _make_timestamp("try_make_timestamp_ntz", parts, date, time, nil, allow_timezone: false)
    end

    # The current time of day (TIME type, Spark 4.1+), optionally with a
    # literal fractional-seconds `precision` (0-6).
    # @return [Column]
    def current_time(precision = nil) = precision.nil? ? Column.invoke("current_time") : Column.invoke("current_time", lit(precision))

    # Add `quantity` units (a literal such as `"HOUR"`) to `ts`.
    # @return [Column]
    def timestamp_add(unit, quantity, ts) = Column.invoke("timestampadd", lit(unit), _col(quantity), _col(ts))

    # The difference between two timestamps in the literal `unit`.
    # @return [Column]
    def timestamp_diff(unit, start, end_) = Column.invoke("timestampdiff", lit(unit), _col(start), _col(end_))

    # Convert `source_ts` from `source_tz` (or the session time zone when nil)
    # to `target_tz`. Time zones are Columns (e.g. `F.lit("UTC")`) or column names.
    # @return [Column]
    def convert_timezone(source_tz, target_tz, source_ts)
      source_tz.nil? ? _invoke("convert_timezone", target_tz, source_ts) : _invoke("convert_timezone", source_tz, target_tz, source_ts)
    end

    # Bucketize rows into (tumbling or sliding) time windows.
    #
    # @param time_column [Column, String]
    # @param window_duration [String] e.g. `"10 minutes"`.
    # @param slide_duration [String, nil] sliding interval (tumbling when nil).
    # @param start_time [String, nil] offset of window starts.
    # @return [Column]
    def window(time_column, window_duration, slide_duration = nil, start_time = nil)
      _check_str("window_duration", window_duration)
      _check_str("slide_duration", slide_duration) unless slide_duration.nil?
      _check_str("start_time", start_time) unless start_time.nil?
      args = [_col(time_column), lit(window_duration)]
      if slide_duration && start_time then args += [lit(slide_duration), lit(start_time)]
      elsif slide_duration then args << lit(slide_duration)
      elsif start_time then args += [lit(window_duration), lit(start_time)]
      end
      Column.invoke("window", *args)
    end

    # Bucketize rows into session windows closed after a gap of inactivity.
    # @param gap_duration [String, Column] e.g. `"5 minutes"` (a literal) or a Column.
    # @return [Column]
    def session_window(time_column, gap_duration)
      _check_str_or_col("gap_duration", gap_duration)
      Column.invoke("session_window", _col(time_column), lit(gap_duration))
    end

    # The `bucket(num_buckets, col)` partition transform for
    # {DataFrameWriterV2#partitioned_by}.
    # @param num_buckets [Integer, Column]
    # @return [Column]
    def bucket(num_buckets, col)
      unless num_buckets.is_a?(Integer) || num_buckets.is_a?(Column)
        raise IllegalArgumentError, "bucket num_buckets must be an Integer or Column, got #{num_buckets.class}"
      end

      Column.invoke("bucket", lit(num_buckets), _col(col))
    end

    # ---- JSON / CSV / XML --------------------------------------------------

    # @return [Column] the value at the literal JSON `path` in `col`.
    def get_json_object(col, path) = Column.invoke("get_json_object", _col(col), lit(path))

    # @return [Column] the given top-level literal `fields` of a JSON string.
    def json_tuple(col, *fields)
      raise IllegalArgumentError, "json_tuple requires at least one field" if fields.empty?

      Column.invoke("json_tuple", _col(col), *fields.map { |f| lit(f) })
    end

    # Parse a JSON string column into a struct/array/map.
    # @param schema [Types::DataType, String, Column] a DataType, or a DDL/JSON schema string (a literal).
    # @param options [Hash, nil] JSON options.
    # @return [Column]
    def from_json(col, schema, options = nil) = _from_text("from_json", col, _schema_literal(schema), options)

    # @param options [Hash, nil] JSON options.
    # @return [Column] `col` (a struct/array/map) as a JSON string.
    def to_json(col, options = nil) = _with_options("to_json", [_col(col)], options)

    # @param json [String, Column] a literal JSON sample.
    # @return [Column] the DDL schema of `json`.
    def schema_of_json(json, options = nil) = _schema_of("schema_of_json", "json", json, options)

    # Parse a CSV string column into a struct.
    # @param schema [String, Column] a DDL schema string (a literal) or Column.
    # @return [Column]
    def from_csv(col, schema, options = nil)
      _check_str_or_col("schema", schema)
      _from_text("from_csv", col, lit(schema), options)
    end

    # @return [Column] `col` (a struct) as a CSV string.
    def to_csv(col, options = nil) = _with_options("to_csv", [_col(col)], options)

    # @param csv [String, Column] a literal CSV sample.
    # @return [Column] the DDL schema of `csv`.
    def schema_of_csv(csv, options = nil) = _schema_of("schema_of_csv", "csv", csv, options)

    # Parse an XML string column into a struct.
    # @param schema [Types::StructType, String, Column] a StructType, or a DDL/JSON schema string (a literal).
    # @return [Column]
    def from_xml(col, schema, options = nil) = _from_text("from_xml", col, _schema_literal(schema), options)

    # @return [Column] `col` (a struct) as an XML string.
    def to_xml(col, options = nil) = _with_options("to_xml", [_col(col)], options)

    # @param xml [String, Column] a literal XML sample.
    # @return [Column] the DDL schema of `xml`.
    def schema_of_xml(xml, options = nil) = _schema_of("schema_of_xml", "xml", xml, options)

    # ---- Variant -----------------------------------------------------------

    # Extract the value at `path` of a VARIANT column, cast to the literal `target_type`.
    # @param path [String, Column] a literal path (e.g. `"$.a"`) or a Column.
    # @return [Column]
    def variant_get(v, path, target_type) = Column.invoke("variant_get", _col(v), lit(path), lit(target_type))
    # Like {variant_get} but null on cast failure.
    # @return [Column]
    def try_variant_get(v, path, target_type) = Column.invoke("try_variant_get", _col(v), lit(path), lit(target_type))

    # Append `value` to the array at `path` of a VARIANT (Spark 4.3+).
    # @return [Column]
    def variant_array_append(v, path, value) = Column.invoke("variant_array_append", _col(v), lit(path), _col(value))
    # Like {variant_array_append} but null on failure.
    # @return [Column]
    def try_variant_array_append(v, path, value) = Column.invoke("try_variant_array_append", _col(v), lit(path), _col(value))
    # Insert `value` at `path` of a VARIANT (Spark 4.3+).
    # @return [Column]
    def variant_insert(v, path, value) = Column.invoke("variant_insert", _col(v), lit(path), _col(value))
    # Like {variant_insert} but null on failure.
    # @return [Column]
    def try_variant_insert(v, path, value) = Column.invoke("try_variant_insert", _col(v), lit(path), _col(value))

    # Set `value` at `path` of a VARIANT, creating missing parents unless
    # `create_if_missing` is false (Spark 4.3+).
    # @return [Column]
    def variant_set(v, path, value, create_if_missing = true)
      Column.invoke("variant_set", _col(v), lit(path), _col(value), lit(create_if_missing))
    end

    # Like {variant_set} but null on failure.
    # @return [Column]
    def try_variant_set(v, path, value, create_if_missing = true)
      Column.invoke("try_variant_set", _col(v), lit(path), _col(value), lit(create_if_missing))
    end

    # Delete one or more `paths` (literals or Columns) from a VARIANT (Spark 4.3+).
    # @return [Column]
    def variant_delete(v, *paths)
      raise IllegalArgumentError, "variant_delete requires at least one path" if paths.empty?

      Column.invoke("variant_delete", _col(v), *paths.map { |p| lit(p) })
    end

    # Remove null object fields (and, unless `include_arrays` is false, null
    # array elements) from a VARIANT (Spark 4.3+).
    # @return [Column]
    def variant_strip_nulls(v, include_arrays = true) = Column.invoke("variant_strip_nulls", _col(v), lit(include_arrays))

    # ---- Array / map functions ---------------------------------------------

    # @return [Column] whether the array contains the literal `value`.
    def array_contains(col, value) = Column.invoke("array_contains", _col(col), lit(value))
    # @return [Column] the 1-based position of the literal `value` in the array (0 if absent).
    def array_position(col, value) = Column.invoke("array_position", _col(col), lit(value))
    # @return [Column] the array without the literal `element`.
    def array_remove(col, element) = Column.invoke("array_remove", _col(col), lit(element))
    # @param count [Integer, Column, String] Strings are column names.
    # @return [Column] an array with `col` repeated `count` times.
    def array_repeat(col, count) = _invoke("array_repeat", col, count)
    # @return [Column] the array with the literal `value` appended.
    def array_append(col, value) = Column.invoke("array_append", _col(col), lit(value))
    # @return [Column] the array with the literal `value` prepended.
    def array_prepend(col, value) = Column.invoke("array_prepend", _col(col), lit(value))
    # @param pos [Integer, Column, String] 1-based position; Strings are column names.
    # @return [Column] the array with the literal `value` inserted at `pos`.
    def array_insert(col, pos, value) = Column.invoke("array_insert", _col(col), _col(pos), lit(value))

    # @return [Column] the array elements joined by the literal `delimiter`.
    def array_join(col, delimiter, null_replacement = nil)
      if null_replacement.nil?
        Column.invoke("array_join", _col(col), lit(delimiter))
      else
        Column.invoke("array_join", _col(col), lit(delimiter), lit(null_replacement))
      end
    end

    # @return [Column] the element at the literal 1-based index (or map key) `extraction`.
    def element_at(col, extraction) = Column.invoke("element_at", _col(col), lit(extraction))

    # @param start [Integer, Column, String] Strings are column names.
    # @param length [Integer, Column, String] Strings are column names.
    # @return [Column] a sub-array from 1-based `start` of `length` elements.
    def slice(col, start, length)
      [start, length].each do |v|
        next if v.is_a?(Integer) || v.is_a?(Column) || v.is_a?(String) || v.is_a?(Symbol)

        raise IllegalArgumentError, "slice start/length must be an Integer, Column, or String, got #{v.class}"
      end
      _invoke("slice", col, start, length)
    end

    # @return [Column] an array from `start` to `stop` (inclusive), by `step`.
    def sequence(start, stop, step = nil) = step.nil? ? _invoke("sequence", start, stop) : _invoke("sequence", start, stop, step)

    # @return [Column] whether the map contains the literal `key`.
    def map_contains_key(col, key) = Column.invoke("map_contains_key", _col(col), lit(key))

    # The element at 0-based `index` (null when out of bounds).
    # @param index [Integer, Column, String] Strings are column names.
    # @return [Column]
    def get(col, index) = _invoke("get", col, index)

    # Sort an array ascending (nulls last), or with a comparator lambda/block
    # returning a negative, zero, or positive integer.
    #
    # @example
    #   F.array_sort("arr") { |l, r| F.when(l < r, 1).otherwise(-1) }
    # @return [Column]
    def array_sort(col, comparator = nil, &block)
      fn = comparator || block
      fn ? Column.invoke("array_sort", _col(col), _lambda(fn)) : _invoke("array_sort", col)
    end

    # A random permutation of an array.
    # @param seed [Integer, Column, nil] random seed (generated when omitted).
    # @note The seeded form requires a Spark 4.0+ server.
    # @return [Column]
    def shuffle(col, seed = nil) = Column.invoke("shuffle", _col(col), _seed(seed))

    # @return [Column] the array sorted (ascending unless `asc` is false).
    def sort_array(col, asc = true) = Column.invoke("sort_array", _col(col), lit(asc))

    # ---- Window / analytic functions --------------------------------------

    # @return [Column] the value `offset` rows before the current row (or `default`).
    def lag(col, offset = 1, default = nil) = _offset_fn("lag", col, offset, default)
    # @return [Column] the value `offset` rows after the current row (or `default`).
    def lead(col, offset = 1, default = nil) = _offset_fn("lead", col, offset, default)
    # @return [Column] the n-tile group id (1..n) of the row.
    def ntile(n) = Column.invoke("ntile", lit(n))

    # @return [Column] the value at the `offset`-th row of the window frame.
    def nth_value(col, offset, ignore_nulls = false)
      args = [_col(col), lit(offset)]
      args << lit(ignore_nulls) unless ignore_nulls.nil?
      Column.invoke("nth_value", *args)
    end

    # ---- Randomness --------------------------------------------------------

    # Uniformly distributed values in [0.0, 1.0).
    # @param seed [Integer, nil] random seed (generated when omitted).
    # @return [Column]
    def rand(seed = nil) = Column.invoke("rand", _seed(seed))
    alias random rand

    # Standard-normal distributed values.
    # @param seed [Integer, nil] random seed (generated when omitted).
    # @return [Column]
    def randn(seed = nil) = Column.invoke("randn", _seed(seed))

    # A random UUID string per row.
    # @param seed [Integer, Column, nil] random seed (generated when omitted).
    # @note Requires a Spark 4.0+ server, since a seed is always sent.
    # @return [Column]
    def uuid(seed = nil) = Column.invoke("uuid", _seed(seed))

    # A random alphanumeric string of the literal `length`.
    # @param seed [Integer, Column, nil] random seed (generated when omitted).
    # @return [Column]
    def randstr(length, seed = nil) = Column.invoke("randstr", lit(length), _seed(seed))

    # Uniformly distributed values in [`min`, `max`) (literals or Columns).
    # @param seed [Integer, Column, nil] random seed (generated when omitted).
    # @return [Column]
    def uniform(min, max, seed = nil) = Column.invoke("uniform", lit(min), lit(max), _seed(seed))

    # ---- Higher-order (lambda) functions -----------------------------------

    # Transform each element of an array. The block receives a {Column} (and
    # optionally the index) and returns a {Column}.
    # @yieldparam element [Column]
    # @return [Column]
    def transform(col, &block) = Column.invoke("transform", _col(col), _lambda(block))
    # @return [Column] whether any element satisfies the block.
    def exists(col, &block) = Column.invoke("exists", _col(col), _lambda(block))
    # @return [Column] whether every element satisfies the block.
    def forall(col, &block) = Column.invoke("forall", _col(col), _lambda(block))
    # @return [Column] the elements satisfying the block (which may take the index too).
    def filter(col, &block) = Column.invoke("filter", _col(col), _lambda(block))
    # @return [Column] two arrays merged element-wise by the block.
    def zip_with(left, right, &block) = Column.invoke("zip_with", _col(left), _col(right), _lambda(block))
    # @return [Column] the map with keys transformed by the block `(k, v) -> k'`.
    def transform_keys(col, &block) = Column.invoke("transform_keys", _col(col), _lambda(block))
    # @return [Column] the map with values transformed by the block `(k, v) -> v'`.
    def transform_values(col, &block) = Column.invoke("transform_values", _col(col), _lambda(block))
    # @return [Column] the map entries satisfying the block `(k, v) -> bool`.
    def map_filter(col, &block) = Column.invoke("map_filter", _col(col), _lambda(block))
    # @return [Column] two maps merged by key with the block `(k, v1, v2) -> v`.
    def map_zip_with(c1, c2, &block) = Column.invoke("map_zip_with", _col(c1), _col(c2), _lambda(block))

    # Aggregate (fold) an array. `merge` combines accumulator and element;
    # optional `finish` post-processes the result.
    # @return [Column]
    def aggregate(col, initial, merge, finish = nil) = _fold("aggregate", col, initial, merge, finish)

    # Alias of {aggregate} under the `reduce` SQL name.
    # @return [Column]
    def reduce(col, initial, merge, finish = nil) = _fold("reduce", col, initial, merge, finish)

    # ---- Geospatial --------------------------------------------------------

    # The WKB of a GEOMETRY/GEOGRAPHY value, optionally with an `endianness`
    # (`"NDR"`/`"XDR"`, a literal) (Spark 4.2+).
    # @return [Column]
    def st_asbinary(geo, endianness = nil)
      return _invoke("st_asbinary", geo) if endianness.nil?

      Column.invoke("st_asbinary", _col(geo), endianness.is_a?(String) ? lit(endianness) : _col(endianness))
    end

    # A GEOMETRY from WKB, optionally with a spatial reference id `srid`.
    # @param srid [Integer, Column, String, nil] Strings are column names.
    # @return [Column]
    def st_geomfromwkb(wkb, srid = nil) = srid.nil? ? _invoke("st_geomfromwkb", wkb) : _invoke("st_geomfromwkb", wkb, srid)

    # A GEOMETRY/GEOGRAPHY with its SRID replaced by `srid`.
    # @param srid [Integer, Column, String] Strings are column names.
    # @return [Column]
    def st_setsrid(geo, srid) = _invoke("st_setsrid", geo, srid)

    # ---- DataFrame-level helper / unsupported ------------------------------

    # Mark a DataFrame for broadcast (map-side) join.
    # @param df [DataFrame]
    # @return [DataFrame]
    def broadcast(df)
      raise IllegalArgumentError, "broadcast expects a DataFrame, got #{df.class}" unless df.is_a?(DataFrame)

      df.hint("broadcast")
    end

    # UDFs require a server-side execution environment (Python/Scala) and are not
    # supported by the pure-Ruby client.
    def udf(*_args)
      raise NotImplementedError, "User-defined functions are not supported by the Ruby Spark Connect client"
    end

    # User-defined table functions are not supported by the pure-Ruby client.
    def udtf(*_args)
      raise NotImplementedError, "User-defined table functions are not supported by the Ruby Spark Connect client"
    end

    # Arrow user-defined table functions are not supported by the pure-Ruby client.
    def arrow_udtf(*_args)
      raise NotImplementedError, "User-defined table functions are not supported by the Ruby Spark Connect client"
    end

    # The following functions are generated programmatically below
    # (`UNIFORM` and `NO_ARG`). The `@!method` directives document them so they
    # appear in the API reference; each returns a {Column}.
    #
    # @!method abs(*cols)
    #   The Spark SQL `abs` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method acos(*cols)
    #   The Spark SQL `acos` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method acosh(*cols)
    #   The Spark SQL `acosh` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method any_value(*cols)
    #   The Spark SQL `any_value` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method array_agg(*cols)
    #   The Spark SQL `array_agg` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method array_compact(*cols)
    #   The Spark SQL `array_compact` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method array_distinct(*cols)
    #   The Spark SQL `array_distinct` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method array_except(*cols)
    #   The Spark SQL `array_except` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method array_intersect(*cols)
    #   The Spark SQL `array_intersect` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method array_max(*cols)
    #   The Spark SQL `array_max` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method array_min(*cols)
    #   The Spark SQL `array_min` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method array_size(*cols)
    #   The Spark SQL `array_size` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method array_union(*cols)
    #   The Spark SQL `array_union` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method arrays_overlap(*cols)
    #   The Spark SQL `arrays_overlap` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method arrays_zip(*cols)
    #   The Spark SQL `arrays_zip` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method ascii(*cols)
    #   The Spark SQL `ascii` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method asin(*cols)
    #   The Spark SQL `asin` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method asinh(*cols)
    #   The Spark SQL `asinh` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method atan(*cols)
    #   The Spark SQL `atan` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method atan2(*cols)
    #   The Spark SQL `atan2` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method atanh(*cols)
    #   The Spark SQL `atanh` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method avg(*cols)
    #   The Spark SQL `avg` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method base64(*cols)
    #   The Spark SQL `base64` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bin(*cols)
    #   The Spark SQL `bin` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bit_and(*cols)
    #   The Spark SQL `bit_and` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bit_count(*cols)
    #   The Spark SQL `bit_count` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bit_get(*cols)
    #   The Spark SQL `bit_get` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bit_length(*cols)
    #   The Spark SQL `bit_length` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bit_or(*cols)
    #   The Spark SQL `bit_or` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bit_xor(*cols)
    #   The Spark SQL `bit_xor` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bitmap_and_agg(*cols)
    #   The Spark SQL `bitmap_and_agg` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bitmap_bit_position(*cols)
    #   The Spark SQL `bitmap_bit_position` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bitmap_bucket_number(*cols)
    #   The Spark SQL `bitmap_bucket_number` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bitmap_construct_agg(*cols)
    #   The Spark SQL `bitmap_construct_agg` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bitmap_count(*cols)
    #   The Spark SQL `bitmap_count` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bitmap_or_agg(*cols)
    #   The Spark SQL `bitmap_or_agg` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bool_and(*cols)
    #   The Spark SQL `bool_and` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method bool_or(*cols)
    #   The Spark SQL `bool_or` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method btrim(*cols)
    #   The Spark SQL `btrim` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method cardinality(*cols)
    #   The Spark SQL `cardinality` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method cbrt(*cols)
    #   The Spark SQL `cbrt` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method ceil(*cols)
    #   The Spark SQL `ceil` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method ceiling(*cols)
    #   The Spark SQL `ceiling` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method char(*cols)
    #   The Spark SQL `char` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method char_length(*cols)
    #   The Spark SQL `char_length` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method character_length(*cols)
    #   The Spark SQL `character_length` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method chr(*cols)
    #   The Spark SQL `chr` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method collation(*cols)
    #   The Spark SQL `collation` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method collect_list(*cols)
    #   The Spark SQL `collect_list` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method collect_set(*cols)
    #   The Spark SQL `collect_set` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method collect_union(*cols)
    #   The Spark SQL `collect_union` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method concat(*cols)
    #   The Spark SQL `concat` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method contains(*cols)
    #   The Spark SQL `contains` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method corr(*cols)
    #   The Spark SQL `corr` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method cos(*cols)
    #   The Spark SQL `cos` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method cosh(*cols)
    #   The Spark SQL `cosh` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method cot(*cols)
    #   The Spark SQL `cot` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method count_if(*cols)
    #   The Spark SQL `count_if` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method counter_diff(*cols)
    #   The Spark SQL `counter_diff` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method covar_pop(*cols)
    #   The Spark SQL `covar_pop` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method covar_samp(*cols)
    #   The Spark SQL `covar_samp` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method crc32(*cols)
    #   The Spark SQL `crc32` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method csc(*cols)
    #   The Spark SQL `csc` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method date_diff(*cols)
    #   The Spark SQL `date_diff` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method date_from_unix_date(*cols)
    #   The Spark SQL `date_from_unix_date` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method date_part(*cols)
    #   The Spark SQL `date_part` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method datepart(*cols)
    #   The Spark SQL `datepart` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method day(*cols)
    #   The Spark SQL `day` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method dayname(*cols)
    #   The Spark SQL `dayname` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method dayofmonth(*cols)
    #   The Spark SQL `dayofmonth` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method dayofweek(*cols)
    #   The Spark SQL `dayofweek` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method dayofyear(*cols)
    #   The Spark SQL `dayofyear` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method days(*cols)
    #   The Spark SQL `days` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method degrees(*cols)
    #   The Spark SQL `degrees` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method elt(*cols)
    #   The Spark SQL `elt` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method endswith(*cols)
    #   The Spark SQL `endswith` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method equal_null(*cols)
    #   The Spark SQL `equal_null` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method every(*cols)
    #   The Spark SQL `every` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method exp(*cols)
    #   The Spark SQL `exp` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method explode(*cols)
    #   The Spark SQL `explode` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method explode_outer(*cols)
    #   The Spark SQL `explode_outer` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method expm1(*cols)
    #   The Spark SQL `expm1` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method extract(*cols)
    #   The Spark SQL `extract` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method factorial(*cols)
    #   The Spark SQL `factorial` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method find_in_set(*cols)
    #   The Spark SQL `find_in_set` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method first_value(*cols)
    #   The Spark SQL `first_value` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method flatten(*cols)
    #   The Spark SQL `flatten` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method floor(*cols)
    #   The Spark SQL `floor` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method from_base32(*cols)
    #   The Spark SQL `from_base32` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method getbit(*cols)
    #   The Spark SQL `getbit` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method grouping(*cols)
    #   The Spark SQL `grouping` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method grouping_id(*cols)
    #   The Spark SQL `grouping_id` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method hash(*cols)
    #   The Spark SQL `hash` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method hex(*cols)
    #   The Spark SQL `hex` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method histogram_numeric(*cols)
    #   The Spark SQL `histogram_numeric` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method hll_sketch_estimate(*cols)
    #   The Spark SQL `hll_sketch_estimate` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method hmac(*cols)
    #   The Spark SQL `hmac` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method hour(*cols)
    #   The Spark SQL `hour` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method hours(*cols)
    #   The Spark SQL `hours` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method hypot(*cols)
    #   The Spark SQL `hypot` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method ifnull(*cols)
    #   The Spark SQL `ifnull` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method ilike(*cols)
    #   The Spark SQL `ilike` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method initcap(*cols)
    #   The Spark SQL `initcap` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method inline(*cols)
    #   The Spark SQL `inline` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method inline_outer(*cols)
    #   The Spark SQL `inline_outer` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method is_valid_utf8(*cols)
    #   The Spark SQL `is_valid_utf8` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method is_valid_variant(*cols)
    #   The Spark SQL `is_valid_variant` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method is_variant_null(*cols)
    #   The Spark SQL `is_variant_null` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method isnan(*cols)
    #   The Spark SQL `isnan` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method isnotnull(*cols)
    #   The Spark SQL `isnotnull` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method isnull(*cols)
    #   The Spark SQL `isnull` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method jaro_winkler_similarity(*cols)
    #   The Spark SQL `jaro_winkler_similarity` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method java_method(*cols)
    #   The Spark SQL `java_method` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method json_array_length(*cols)
    #   The Spark SQL `json_array_length` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method json_object_keys(*cols)
    #   The Spark SQL `json_object_keys` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_get_n_bigint(*cols)
    #   The Spark SQL `kll_sketch_get_n_bigint` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_get_n_double(*cols)
    #   The Spark SQL `kll_sketch_get_n_double` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_get_n_float(*cols)
    #   The Spark SQL `kll_sketch_get_n_float` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_get_quantile_bigint(*cols)
    #   The Spark SQL `kll_sketch_get_quantile_bigint` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_get_quantile_double(*cols)
    #   The Spark SQL `kll_sketch_get_quantile_double` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_get_quantile_float(*cols)
    #   The Spark SQL `kll_sketch_get_quantile_float` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_get_rank_bigint(*cols)
    #   The Spark SQL `kll_sketch_get_rank_bigint` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_get_rank_double(*cols)
    #   The Spark SQL `kll_sketch_get_rank_double` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_get_rank_float(*cols)
    #   The Spark SQL `kll_sketch_get_rank_float` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_merge_bigint(*cols)
    #   The Spark SQL `kll_sketch_merge_bigint` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_merge_double(*cols)
    #   The Spark SQL `kll_sketch_merge_double` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_merge_float(*cols)
    #   The Spark SQL `kll_sketch_merge_float` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_to_string_bigint(*cols)
    #   The Spark SQL `kll_sketch_to_string_bigint` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_to_string_double(*cols)
    #   The Spark SQL `kll_sketch_to_string_double` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kll_sketch_to_string_float(*cols)
    #   The Spark SQL `kll_sketch_to_string_float` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method kurtosis(*cols)
    #   The Spark SQL `kurtosis` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method last_day(*cols)
    #   The Spark SQL `last_day` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method last_value(*cols)
    #   The Spark SQL `last_value` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method lcase(*cols)
    #   The Spark SQL `lcase` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method left(*cols)
    #   The Spark SQL `left` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method length(*cols)
    #   The Spark SQL `length` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method like(*cols)
    #   The Spark SQL `like` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method ln(*cols)
    #   The Spark SQL `ln` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method log10(*cols)
    #   The Spark SQL `log10` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method log1p(*cols)
    #   The Spark SQL `log1p` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method log2(*cols)
    #   The Spark SQL `log2` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method lower(*cols)
    #   The Spark SQL `lower` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method make_time(*cols)
    #   The Spark SQL `make_time` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method make_timestamp_ltz(*cols)
    #   The Spark SQL `make_timestamp_ltz` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method make_valid_utf8(*cols)
    #   The Spark SQL `make_valid_utf8` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method map_entries(*cols)
    #   The Spark SQL `map_entries` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method map_from_entries(*cols)
    #   The Spark SQL `map_from_entries` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method map_keys(*cols)
    #   The Spark SQL `map_keys` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method map_values(*cols)
    #   The Spark SQL `map_values` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method max(*cols)
    #   The Spark SQL `max` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method md5(*cols)
    #   The Spark SQL `md5` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method median(*cols)
    #   The Spark SQL `median` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method min(*cols)
    #   The Spark SQL `min` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method minute(*cols)
    #   The Spark SQL `minute` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method month(*cols)
    #   The Spark SQL `month` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method monthname(*cols)
    #   The Spark SQL `monthname` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method months(*cols)
    #   The Spark SQL `months` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method negative(*cols)
    #   The Spark SQL `negative` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method nullif(*cols)
    #   The Spark SQL `nullif` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method nullifzero(*cols)
    #   The Spark SQL `nullifzero` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method nvl(*cols)
    #   The Spark SQL `nvl` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method nvl2(*cols)
    #   The Spark SQL `nvl2` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method octet_length(*cols)
    #   The Spark SQL `octet_length` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method parse_json(*cols)
    #   The Spark SQL `parse_json` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method parse_url(*cols)
    #   The Spark SQL `parse_url` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method pmod(*cols)
    #   The Spark SQL `pmod` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method posexplode(*cols)
    #   The Spark SQL `posexplode` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method posexplode_outer(*cols)
    #   The Spark SQL `posexplode_outer` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method position(*cols)
    #   The Spark SQL `position` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method positive(*cols)
    #   The Spark SQL `positive` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method printf(*cols)
    #   The Spark SQL `printf` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method product(*cols)
    #   The Spark SQL `product` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method quarter(*cols)
    #   The Spark SQL `quarter` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method quote(*cols)
    #   The Spark SQL `quote` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method radians(*cols)
    #   The Spark SQL `radians` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method reflect(*cols)
    #   The Spark SQL `reflect` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regexp(*cols)
    #   The Spark SQL `regexp` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regexp_count(*cols)
    #   The Spark SQL `regexp_count` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regexp_like(*cols)
    #   The Spark SQL `regexp_like` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regexp_substr(*cols)
    #   The Spark SQL `regexp_substr` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regr_avgx(*cols)
    #   The Spark SQL `regr_avgx` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regr_avgy(*cols)
    #   The Spark SQL `regr_avgy` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regr_count(*cols)
    #   The Spark SQL `regr_count` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regr_intercept(*cols)
    #   The Spark SQL `regr_intercept` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regr_r2(*cols)
    #   The Spark SQL `regr_r2` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regr_slope(*cols)
    #   The Spark SQL `regr_slope` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regr_sxx(*cols)
    #   The Spark SQL `regr_sxx` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regr_sxy(*cols)
    #   The Spark SQL `regr_sxy` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method regr_syy(*cols)
    #   The Spark SQL `regr_syy` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method replace(*cols)
    #   The Spark SQL `replace` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method reverse(*cols)
    #   The Spark SQL `reverse` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method right(*cols)
    #   The Spark SQL `right` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method rint(*cols)
    #   The Spark SQL `rint` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method rlike(*cols)
    #   The Spark SQL `rlike` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method schema_of_variant(*cols)
    #   The Spark SQL `schema_of_variant` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method schema_of_variant_agg(*cols)
    #   The Spark SQL `schema_of_variant_agg` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method sec(*cols)
    #   The Spark SQL `sec` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method second(*cols)
    #   The Spark SQL `second` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method sha(*cols)
    #   The Spark SQL `sha` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method sha1(*cols)
    #   The Spark SQL `sha1` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method sign(*cols)
    #   The Spark SQL `sign` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method signum(*cols)
    #   The Spark SQL `signum` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method sin(*cols)
    #   The Spark SQL `sin` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method sinh(*cols)
    #   The Spark SQL `sinh` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method size(*cols)
    #   The Spark SQL `size` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method skewness(*cols)
    #   The Spark SQL `skewness` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method some(*cols)
    #   The Spark SQL `some` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method soundex(*cols)
    #   The Spark SQL `soundex` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method split_part(*cols)
    #   The Spark SQL `split_part` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method sqrt(*cols)
    #   The Spark SQL `sqrt` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method st_geogfromwkb(*cols)
    #   The Spark SQL `st_geogfromwkb` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method st_srid(*cols)
    #   The Spark SQL `st_srid` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method stack(*cols)
    #   The Spark SQL `stack` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method startswith(*cols)
    #   The Spark SQL `startswith` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method std(*cols)
    #   The Spark SQL `std` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method stddev(*cols)
    #   The Spark SQL `stddev` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method stddev_pop(*cols)
    #   The Spark SQL `stddev_pop` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method stddev_samp(*cols)
    #   The Spark SQL `stddev_samp` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method substr(*cols)
    #   The Spark SQL `substr` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method sum(*cols)
    #   The Spark SQL `sum` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method tan(*cols)
    #   The Spark SQL `tan` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method tanh(*cols)
    #   The Spark SQL `tanh` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method theta_difference(*cols)
    #   The Spark SQL `theta_difference` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method theta_intersection(*cols)
    #   The Spark SQL `theta_intersection` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method theta_intersection_agg(*cols)
    #   The Spark SQL `theta_intersection_agg` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method theta_sketch_estimate(*cols)
    #   The Spark SQL `theta_sketch_estimate` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method time_bucket(*cols)
    #   The Spark SQL `time_bucket` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method time_diff(*cols)
    #   The Spark SQL `time_diff` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method time_from_micros(*cols)
    #   The Spark SQL `time_from_micros` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method time_from_millis(*cols)
    #   The Spark SQL `time_from_millis` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method time_from_seconds(*cols)
    #   The Spark SQL `time_from_seconds` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method time_to_micros(*cols)
    #   The Spark SQL `time_to_micros` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method time_to_millis(*cols)
    #   The Spark SQL `time_to_millis` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method time_to_seconds(*cols)
    #   The Spark SQL `time_to_seconds` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method time_trunc(*cols)
    #   The Spark SQL `time_trunc` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method timestamp_micros(*cols)
    #   The Spark SQL `timestamp_micros` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method timestamp_millis(*cols)
    #   The Spark SQL `timestamp_millis` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method timestamp_nanos(*cols)
    #   The Spark SQL `timestamp_nanos` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method timestamp_seconds(*cols)
    #   The Spark SQL `timestamp_seconds` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method to_base32(*cols)
    #   The Spark SQL `to_base32` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method to_binary(*cols)
    #   The Spark SQL `to_binary` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method to_char(*cols)
    #   The Spark SQL `to_char` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method to_number(*cols)
    #   The Spark SQL `to_number` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method to_time(*cols)
    #   The Spark SQL `to_time` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method to_timestamp_ltz(*cols)
    #   The Spark SQL `to_timestamp_ltz` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method to_timestamp_ntz(*cols)
    #   The Spark SQL `to_timestamp_ntz` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method to_unix_timestamp(*cols)
    #   The Spark SQL `to_unix_timestamp` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method to_varchar(*cols)
    #   The Spark SQL `to_varchar` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method to_variant_object(*cols)
    #   The Spark SQL `to_variant_object` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_add(*cols)
    #   The Spark SQL `try_add` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_avg(*cols)
    #   The Spark SQL `try_avg` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_divide(*cols)
    #   The Spark SQL `try_divide` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_element_at(*cols)
    #   The Spark SQL `try_element_at` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_make_timestamp_ltz(*cols)
    #   The Spark SQL `try_make_timestamp_ltz` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_mod(*cols)
    #   The Spark SQL `try_mod` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_multiply(*cols)
    #   The Spark SQL `try_multiply` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_parse_json(*cols)
    #   The Spark SQL `try_parse_json` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_parse_url(*cols)
    #   The Spark SQL `try_parse_url` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_reflect(*cols)
    #   The Spark SQL `try_reflect` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_subtract(*cols)
    #   The Spark SQL `try_subtract` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_sum(*cols)
    #   The Spark SQL `try_sum` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_to_binary(*cols)
    #   The Spark SQL `try_to_binary` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_to_number(*cols)
    #   The Spark SQL `try_to_number` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_to_time(*cols)
    #   The Spark SQL `try_to_time` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_to_timestamp(*cols)
    #   The Spark SQL `try_to_timestamp` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_url_decode(*cols)
    #   The Spark SQL `try_url_decode` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method try_validate_utf8(*cols)
    #   The Spark SQL `try_validate_utf8` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method tuple_difference_double(*cols)
    #   The Spark SQL `tuple_difference_double` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method tuple_difference_integer(*cols)
    #   The Spark SQL `tuple_difference_integer` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method tuple_difference_theta_double(*cols)
    #   The Spark SQL `tuple_difference_theta_double` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method tuple_difference_theta_integer(*cols)
    #   The Spark SQL `tuple_difference_theta_integer` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method tuple_sketch_estimate_double(*cols)
    #   The Spark SQL `tuple_sketch_estimate_double` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method tuple_sketch_estimate_integer(*cols)
    #   The Spark SQL `tuple_sketch_estimate_integer` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method tuple_sketch_theta_double(*cols)
    #   The Spark SQL `tuple_sketch_theta_double` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method tuple_sketch_theta_integer(*cols)
    #   The Spark SQL `tuple_sketch_theta_integer` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method typeof(*cols)
    #   The Spark SQL `typeof` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method ucase(*cols)
    #   The Spark SQL `ucase` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method unbase64(*cols)
    #   The Spark SQL `unbase64` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method unhex(*cols)
    #   The Spark SQL `unhex` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method unix_date(*cols)
    #   The Spark SQL `unix_date` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method unix_micros(*cols)
    #   The Spark SQL `unix_micros` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method unix_millis(*cols)
    #   The Spark SQL `unix_millis` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method unix_nanos(*cols)
    #   The Spark SQL `unix_nanos` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method unix_seconds(*cols)
    #   The Spark SQL `unix_seconds` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method unwrap_udt(*cols)
    #   The Spark SQL `unwrap_udt` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method upper(*cols)
    #   The Spark SQL `upper` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method url_decode(*cols)
    #   The Spark SQL `url_decode` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method url_encode(*cols)
    #   The Spark SQL `url_encode` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method validate_utf8(*cols)
    #   The Spark SQL `validate_utf8` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method var_pop(*cols)
    #   The Spark SQL `var_pop` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method var_samp(*cols)
    #   The Spark SQL `var_samp` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method variance(*cols)
    #   The Spark SQL `variance` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method vector_avg(*cols)
    #   The Spark SQL `vector_avg` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method vector_cosine_similarity(*cols)
    #   The Spark SQL `vector_cosine_similarity` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method vector_inner_product(*cols)
    #   The Spark SQL `vector_inner_product` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method vector_l2_distance(*cols)
    #   The Spark SQL `vector_l2_distance` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method vector_norm(*cols)
    #   The Spark SQL `vector_norm` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method vector_normalize(*cols)
    #   The Spark SQL `vector_normalize` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method vector_sum(*cols)
    #   The Spark SQL `vector_sum` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method weekday(*cols)
    #   The Spark SQL `weekday` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method weekofyear(*cols)
    #   The Spark SQL `weekofyear` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method width_bucket(*cols)
    #   The Spark SQL `width_bucket` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method window_time(*cols)
    #   The Spark SQL `window_time` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method xpath(*cols)
    #   The Spark SQL `xpath` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method xpath_boolean(*cols)
    #   The Spark SQL `xpath_boolean` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method xpath_double(*cols)
    #   The Spark SQL `xpath_double` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method xpath_float(*cols)
    #   The Spark SQL `xpath_float` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method xpath_int(*cols)
    #   The Spark SQL `xpath_int` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method xpath_long(*cols)
    #   The Spark SQL `xpath_long` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method xpath_number(*cols)
    #   The Spark SQL `xpath_number` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method xpath_short(*cols)
    #   The Spark SQL `xpath_short` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method xpath_string(*cols)
    #   The Spark SQL `xpath_string` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method xxhash64(*cols)
    #   The Spark SQL `xxhash64` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method year(*cols)
    #   The Spark SQL `year` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method years(*cols)
    #   The Spark SQL `years` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method zeroifnull(*cols)
    #   The Spark SQL `zeroifnull` function. String arguments are treated as column names.
    #   @return [Column]
    # @!method cume_dist
    #   The Spark SQL `cume_dist` function (takes no arguments).
    #   @return [Column]
    # @!method curdate
    #   The Spark SQL `curdate` function (takes no arguments).
    #   @return [Column]
    # @!method current_catalog
    #   The Spark SQL `current_catalog` function (takes no arguments).
    #   @return [Column]
    # @!method current_database
    #   The Spark SQL `current_database` function (takes no arguments).
    #   @return [Column]
    # @!method current_date
    #   The Spark SQL `current_date` function (takes no arguments).
    #   @return [Column]
    # @!method current_path
    #   The Spark SQL `current_path` function (takes no arguments).
    #   @return [Column]
    # @!method current_schema
    #   The Spark SQL `current_schema` function (takes no arguments).
    #   @return [Column]
    # @!method current_timestamp
    #   The Spark SQL `current_timestamp` function (takes no arguments).
    #   @return [Column]
    # @!method current_timezone
    #   The Spark SQL `current_timezone` function (takes no arguments).
    #   @return [Column]
    # @!method current_user
    #   The Spark SQL `current_user` function (takes no arguments).
    #   @return [Column]
    # @!method dense_rank
    #   The Spark SQL `dense_rank` function (takes no arguments).
    #   @return [Column]
    # @!method e
    #   The Spark SQL `e` function (takes no arguments).
    #   @return [Column]
    # @!method input_file_block_length
    #   The Spark SQL `input_file_block_length` function (takes no arguments).
    #   @return [Column]
    # @!method input_file_block_start
    #   The Spark SQL `input_file_block_start` function (takes no arguments).
    #   @return [Column]
    # @!method input_file_name
    #   The Spark SQL `input_file_name` function (takes no arguments).
    #   @return [Column]
    # @!method localtimestamp
    #   The Spark SQL `localtimestamp` function (takes no arguments).
    #   @return [Column]
    # @!method monotonically_increasing_id
    #   The Spark SQL `monotonically_increasing_id` function (takes no arguments).
    #   @return [Column]
    # @!method now
    #   The Spark SQL `now` function (takes no arguments).
    #   @return [Column]
    # @!method percent_rank
    #   The Spark SQL `percent_rank` function (takes no arguments).
    #   @return [Column]
    # @!method pi
    #   The Spark SQL `pi` function (takes no arguments).
    #   @return [Column]
    # @!method rank
    #   The Spark SQL `rank` function (takes no arguments).
    #   @return [Column]
    # @!method row_number
    #   The Spark SQL `row_number` function (takes no arguments).
    #   @return [Column]
    # @!method session_user
    #   The Spark SQL `session_user` function (takes no arguments).
    #   @return [Column]
    # @!method spark_partition_id
    #   The Spark SQL `spark_partition_id` function (takes no arguments).
    #   @return [Column]
    # @!method user
    #   The Spark SQL `user` function (takes no arguments).
    #   @return [Column]
    # @!method version
    #   The Spark SQL `version` function (takes no arguments).
    #   @return [Column]
    # ---- Generated uniform functions --------------------------------------
    # Functions whose arguments are all ColumnOrName (a String denotes a column
    # name; other values become literals). Defined programmatically to keep the
    # surface complete and compact.

    UNIFORM = %w[
      abs acos acosh any_value array_agg array_compact array_distinct array_except array_intersect array_max
      array_min array_size array_union arrays_overlap arrays_zip ascii asin asinh atan atan2 atanh avg base64 bin
      bit_and bit_count bit_get bit_length bit_or bit_xor bitmap_and_agg bitmap_bit_position bitmap_bucket_number
      bitmap_construct_agg bitmap_count bitmap_or_agg bool_and bool_or btrim cardinality cbrt ceil ceiling char
      char_length character_length chr collation collect_list collect_set collect_union concat contains corr cos cosh
      cot count_if counter_diff covar_pop covar_samp crc32 csc date_diff date_from_unix_date date_part datepart day
      dayname dayofmonth dayofweek dayofyear days degrees elt endswith equal_null every exp explode explode_outer
      expm1 extract factorial find_in_set first_value flatten floor from_base32 getbit grouping grouping_id hash hex
      histogram_numeric hll_sketch_estimate hmac hour hours hypot ifnull ilike initcap inline inline_outer
      is_valid_utf8 is_valid_variant is_variant_null isnan isnotnull isnull jaro_winkler_similarity java_method
      json_array_length json_object_keys kll_sketch_get_n_bigint kll_sketch_get_n_double kll_sketch_get_n_float
      kll_sketch_get_quantile_bigint kll_sketch_get_quantile_double kll_sketch_get_quantile_float
      kll_sketch_get_rank_bigint kll_sketch_get_rank_double kll_sketch_get_rank_float kll_sketch_merge_bigint
      kll_sketch_merge_double kll_sketch_merge_float kll_sketch_to_string_bigint kll_sketch_to_string_double
      kll_sketch_to_string_float kurtosis last_day last_value lcase left length like ln log10 log1p log2 lower
      make_time make_timestamp_ltz make_valid_utf8 map_entries map_from_entries map_keys map_values max md5 median
      min minute month monthname months negative nullif nullifzero nvl nvl2 octet_length parse_json parse_url pmod
      posexplode posexplode_outer position positive printf product quarter quote radians reflect regexp regexp_count
      regexp_like regexp_substr regr_avgx regr_avgy regr_count regr_intercept regr_r2 regr_slope regr_sxx regr_sxy
      regr_syy replace reverse right rint rlike schema_of_variant schema_of_variant_agg sec second sha sha1 sign
      signum sin sinh size skewness some soundex split_part sqrt st_geogfromwkb st_srid stack startswith std stddev
      stddev_pop stddev_samp substr sum tan tanh theta_difference theta_intersection theta_intersection_agg
      theta_sketch_estimate time_bucket time_diff time_from_micros time_from_millis time_from_seconds time_to_micros
      time_to_millis time_to_seconds time_trunc timestamp_micros timestamp_millis timestamp_nanos timestamp_seconds
      to_base32 to_binary to_char to_number to_time to_timestamp_ltz to_timestamp_ntz to_unix_timestamp to_varchar
      to_variant_object try_add try_avg try_divide try_element_at try_make_timestamp_ltz try_mod try_multiply
      try_parse_json try_parse_url try_reflect try_subtract try_sum try_to_binary try_to_number try_to_time
      try_to_timestamp try_url_decode try_validate_utf8 tuple_difference_double tuple_difference_integer
      tuple_difference_theta_double tuple_difference_theta_integer tuple_sketch_estimate_double
      tuple_sketch_estimate_integer tuple_sketch_theta_double tuple_sketch_theta_integer typeof ucase unbase64 unhex
      unix_date unix_micros unix_millis unix_nanos unix_seconds unwrap_udt upper url_decode url_encode validate_utf8
      var_pop var_samp variance vector_avg vector_cosine_similarity vector_inner_product vector_l2_distance
      vector_norm vector_normalize vector_sum weekday weekofyear width_bucket window_time xpath xpath_boolean
      xpath_double xpath_float xpath_int xpath_long xpath_number xpath_short xpath_string xxhash64 year years
      zeroifnull
    ].freeze

    UNIFORM.each do |fn|
      define_method(fn) { |*cols| Column.invoke(fn, *cols.map { |c| _col(c) }) }
    end

    # No-argument functions.
    NO_ARG = %w[
      cume_dist curdate current_catalog current_database current_date current_path current_schema current_timestamp
      current_timezone current_user dense_rank e input_file_block_length input_file_block_start input_file_name
      localtimestamp monotonically_increasing_id now percent_rank pi rank row_number session_user spark_partition_id
      user version
    ].freeze

    NO_ARG.each do |fn|
      define_method(fn) { Column.invoke(fn) }
    end

    # ---- Internal helpers --------------------------------------------------

    # ColumnOrName coercion: String/Symbol -> column reference, Column -> itself,
    # everything else -> literal.
    # @api private
    def _col(value)
      case value
      when Column then value
      when String, Symbol then col(value.to_s)
      else lit(value)
      end
    end

    # @api private
    def _lit_or_col(value)
      value.is_a?(Column) ? value : lit(value)
    end

    # Invoke `name` over ColumnOrName arguments.
    # @api private
    def _invoke(name, *cols) = Column.invoke(name, *cols.map { |c| _col(c) })

    # A ColumnOrName argument, or the literal `default` when omitted (nil).
    # @api private
    def _col_or_default(value, default) = value.nil? ? lit(default) : _col(value)

    # Invoke `name` over ColumnOrName `cols` followed by the given (non-nil)
    # optional literal arguments.
    # @api private
    def _optional_literal_tail(name, cols, tail)
      Column.invoke(name, *cols.map { |c| _col(c) }, *tail.compact.map { |v| lit(v) })
    end

    # @api private
    def _tuple_defaults(lg_nom_entries, mode)
      [lit(lg_nom_entries.nil? ? 12 : lg_nom_entries), lit(mode.nil? ? "sum" : mode)]
    end

    # @api private
    def _agg_with_delimiter(name, col, delimiter, is_distinct: false)
      args = [_col(col)]
      args << lit(delimiter) unless delimiter.nil?
      Column.invoke(name, *args, is_distinct: is_distinct)
    end

    # A percentage argument: a list becomes an `array(...)` of literals, like PySpark.
    # @api private
    def _percentage(percentage)
      percentage.is_a?(Array) ? array(*percentage.map { |p| lit(p) }) : lit(percentage)
    end

    # A random seed literal (a 64-bit long, like PySpark's client-side seed) when
    # `seed` is nil, otherwise `seed` as a literal/Column.
    # @api private
    def _seed(seed)
      return lit(seed) unless seed.nil?

      Column.new(Proto::Expression.new(literal: Proto::Expression::Literal.new(long: Random.rand(2**63))))
    end

    # `f(*cols)` and `f([cols])` are equivalent, like PySpark.
    # @api private
    def _unwrap_list(cols) = cols.size == 1 && cols.first.is_a?(Array) ? cols.first : cols

    # @api private
    def _at_least_two(name, cols)
      raise IllegalArgumentError, "#{name} requires at least 2 columns, got #{cols.size}" if cols.size < 2

      _invoke(name, *cols)
    end

    # @api private
    def _check_str_or_col(arg_name, value)
      return if value.is_a?(String) || value.is_a?(Symbol) || value.is_a?(Column)

      raise IllegalArgumentError, "#{arg_name} must be a String or Column, got #{value.class}"
    end

    # @api private
    def _check_str(arg_name, value)
      raise IllegalArgumentError, "#{arg_name} must be a String, got #{value.class}" unless value.is_a?(String)
    end

    # Spark's trim functions take the trim characters first.
    # @api private
    def _trim(name, col, trim) = trim.nil? ? _invoke(name, col) : _invoke(name, trim, col)

    # @api private
    def _aes_decrypt(name, input, key, mode, padding, aad)
      Column.invoke(name, _col(input), _col(key), _col_or_default(mode, "GCM"), _col_or_default(padding, "DEFAULT"),
                    _col_or_default(aad, ""))
    end

    # @api private
    def _make_interval(name, years, months, weeks, days, hours, mins, secs)
      parts = [years, months, weeks, days, hours, mins].map { |v| _col_or_default(v, 0) }
      Column.invoke(name, *parts, _col_or_default(secs, BigDecimal("0")))
    end

    # @api private
    def _make_timestamp(name, parts, date, time, timezone, allow_timezone:)
      if parts.empty?
        raise IllegalArgumentError, "#{name} requires either the date/time parts or date: and time:" if date.nil? || time.nil?

        args = [date, time]
      else
        unless date.nil? && time.nil?
          raise IllegalArgumentError, "#{name}: years|months|days|hours|mins|secs and date|time cannot be set together"
        end

        allowed = allow_timezone ? [6, 7] : [6]
        raise IllegalArgumentError, "#{name} expects #{allowed.join(' or ')} parts, got #{parts.size}" unless allowed.include?(parts.size)
        raise IllegalArgumentError, "#{name}: timezone given twice" if parts.size == 7 && !timezone.nil?

        args = parts.first(6)
        timezone = parts[6] if parts.size == 7
      end
      args << timezone unless timezone.nil?
      _invoke(name, *args)
    end

    # @api private
    def _offset_fn(name, col, offset, default)
      default.nil? ? Column.invoke(name, _col(col), lit(offset)) : Column.invoke(name, _col(col), lit(offset), lit(default))
    end

    # @api private
    def _fold(name, col, initial, merge, finish)
      args = [_col(col), _col(initial), _lambda(merge)]
      args << _lambda(finish) if finish
      Column.invoke(name, *args)
    end

    # An options Hash as a `map(k1, v1, ...)` literal argument, like PySpark.
    # @api private
    def _options_to_col(options) = create_map(*options.flat_map { |k, v| [lit(k.to_s), lit(v.to_s)] })

    # Append an options map argument. A nil or empty Hash adds nothing (an empty
    # map is equivalent to no options).
    # @api private
    def _with_options(name, args, options)
      args += [_options_to_col(options)] unless options.nil? || options.empty?
      Column.invoke(name, *args)
    end

    # @api private
    def _from_text(name, col, schema_col, options) = _with_options(name, [_col(col), schema_col], options)

    # A schema argument: a DataType becomes its JSON, a String/Column a literal.
    # @api private
    def _schema_literal(schema)
      case schema
      when Types::DataType then lit(schema.json)
      when String, Column then lit(schema)
      when Symbol then lit(schema.to_s)
      else raise IllegalArgumentError, "schema must be a DataType, String, or Column, got #{schema.class}"
      end
    end

    # @api private
    def _schema_of(name, arg_name, sample, options)
      _check_str_or_col(arg_name, sample)
      _with_options(name, [lit(sample.is_a?(Symbol) ? sample.to_s : sample)], options)
    end

    # @api private
    def _star?(col)
      col.is_a?(Column) && col.to_expr.expr_type == :unresolved_star
    end

    @lambda_counter = 0

    class << self
      # @api private
      attr_accessor :lambda_counter
    end

    # Build a {Column} wrapping a LambdaFunction from a Ruby block. The block is
    # called with one or more lambda-variable columns and must return a {Column}.
    # @api private
    def _lambda(block)
      raise IllegalArgumentError, "a block (or lambda) is required" if block.nil?

      arity = block.arity.negative? ? 1 : [block.arity, 1].max
      Functions.lambda_counter += 1
      names = (0...arity).map { |i| "x_#{Functions.lambda_counter}_#{i}" }
      vars = names.map do |n|
        Proto::Expression::UnresolvedNamedLambdaVariable.new(name_parts: [n])
      end
      cols = vars.map { |v| Column.new(Proto::Expression.new(unresolved_named_lambda_variable: v)) }
      body = block.call(*cols)
      raise IllegalArgumentError, "the lambda must return a Column, got #{body.class}" unless body.is_a?(Column)

      Column.new(Proto::Expression.new(
                   lambda_function: Proto::Expression::LambdaFunction.new(function: body.to_expr, arguments: vars)
                 ))
    end
  end

  # Short alias for {Functions}: `SparkConnect::F.col("x")`.
  F = Functions
end
