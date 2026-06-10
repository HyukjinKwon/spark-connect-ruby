# frozen_string_literal: true

module SparkConnect
  # The standard Spark SQL function library, mirroring PySpark's
  # `pyspark.sql.functions`. Every function returns a {Column}.
  #
  # Available both as `SparkConnect::Functions` and the shorthand
  # `SparkConnect::F`. All methods are module functions.
  #
  # Following PySpark's convention, a {String} argument denotes a **column name**
  # for most functions (e.g. `F.sum("salary")` aggregates the `salary` column),
  # while functions whose parameters are genuinely literal (regex patterns, date
  # formats, JSON paths, ...) treat their {String} arguments as literal values.
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

    # A column reference by name. `"*"` selects all columns.
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
    def desc(col) = _col(col).desc
    def asc_nulls_first(col) = _col(col).asc_nulls_first
    def asc_nulls_last(col) = _col(col).asc_nulls_last
    def desc_nulls_first(col) = _col(col).desc_nulls_first
    def desc_nulls_last(col) = _col(col).desc_nulls_last

    # Start a CASE WHEN expression. Chain {Column#when} / {Column#otherwise}.
    # @return [Column]
    def when(condition, value)
      Column.invoke("when", condition, value)
    end

    # ---- Aggregate / counting ---------------------------------------------

    # @return [Column] count of rows (or non-null values of a column). `"*"`
    #   counts all rows.
    def count(col)
      col.to_s == "*" ? Column.invoke("count", lit(1)) : Column.invoke("count", _col(col))
    end

    # @return [Column] count of distinct combinations of the given columns.
    def count_distinct(*cols)
      Column.invoke("count", *cols.map { |c| _col(c) }, is_distinct: true)
    end
    alias countDistinct count_distinct

    # @return [Column] approximate distinct count (optionally with relative SD).
    def approx_count_distinct(col, rsd = nil)
      rsd.nil? ? Column.invoke("approx_count_distinct", _col(col)) : Column.invoke("approx_count_distinct", _col(col), lit(rsd))
    end

    # @return [Column] sum of distinct values.
    def sum_distinct(col) = Column.invoke("sum", _col(col), is_distinct: true)

    # ---- Rounding ----------------------------------------------------------

    # @return [Column] HALF_UP rounding to `scale` decimal places.
    def round(col, scale = 0) = Column.invoke("round", _col(col), lit(scale))
    # @return [Column] HALF_EVEN ("banker's") rounding to `scale` places.
    def bround(col, scale = 0) = Column.invoke("bround", _col(col), lit(scale))

    # ---- Conditionals / null handling -------------------------------------

    # @return [Column] first non-null among the given columns.
    def coalesce(*cols) = Column.invoke("coalesce", *cols.map { |c| _col(c) })
    # @return [Column] `value` if `col` is NaN else `col`.
    def nanvl(col1, col2) = Column.invoke("nanvl", _col(col1), _col(col2))

    # ---- Constructors of complex types ------------------------------------

    # @return [Column] a struct from the given columns.
    def struct(*cols) = Column.invoke("struct", *cols.map { |c| _col(c) })
    # @return [Column] an array from the given columns.
    def array(*cols) = Column.invoke("array", *cols.map { |c| _col(c) })
    # @return [Column] a map from alternating key/value columns.
    def create_map(*cols) = Column.invoke("map", *cols.map { |c| _col(c) })
    # @return [Column] a map from two array columns (keys, values).
    def map_from_arrays(keys, values) = Column.invoke("map_from_arrays", _col(keys), _col(values))
    # @return [Column] a named struct from alternating name/value arguments.
    def named_struct(*cols) = Column.invoke("named_struct", *cols.map { |c| _col(c) })

    # ---- String functions with literal arguments --------------------------

    # @return [Column] concatenation of columns separated by literal `sep`.
    def concat_ws(sep, *cols) = Column.invoke("concat_ws", lit(sep), *cols.map { |c| _col(c) })
    # @return [Column] printf-style formatting using literal `fmt`.
    def format_string(fmt, *cols) = Column.invoke("format_string", lit(fmt), *cols.map { |c| _col(c) })
    # @return [Column] number formatted to `d` decimal places.
    def format_number(col, d) = Column.invoke("format_number", _col(col), lit(d))
    # @return [Column] substring of length `len` from 1-based `pos`.
    def substring(col, pos, len) = Column.invoke("substring", _col(col), lit(pos), lit(len))
    # @return [Column] substring before the `count`-th occurrence of `delim`.
    def substring_index(col, delim, count) = Column.invoke("substring_index", _col(col), lit(delim), lit(count))
    # @return [Column] 1-based position of literal `substr` within `col` (0 if absent).
    def instr(col, substr) = Column.invoke("instr", _col(col), lit(substr))
    # @return [Column] 1-based position of `substr` in `col` at/after `pos`.
    def locate(substr, col, pos = 1) = Column.invoke("locate", lit(substr), _col(col), lit(pos))
    # @return [Column] left-padded string.
    def lpad(col, len, pad) = Column.invoke("lpad", _col(col), lit(len), lit(pad))
    # @return [Column] right-padded string.
    def rpad(col, len, pad) = Column.invoke("rpad", _col(col), lit(len), lit(pad))
    # @return [Column] the string repeated `n` times.
    def repeat(col, n) = Column.invoke("repeat", _col(col), lit(n))
    # @return [Column] split `col` by the literal regex `pattern`.
    def split(col, pattern, limit = -1) = Column.invoke("split", _col(col), lit(pattern), lit(limit))
    # @return [Column] characters of `col` matching `matching` replaced per `replace`.
    def translate(col, matching, replace) = Column.invoke("translate", _col(col), lit(matching), lit(replace))
    # @return [Column] the `idx`-th group of `pattern` matched in `col`.
    def regexp_extract(col, pattern, idx = 0) = Column.invoke("regexp_extract", _col(col), lit(pattern), lit(idx))
    # @return [Column] all matches of group `idx` of `pattern`.
    def regexp_extract_all(col, pattern, idx = 1) = Column.invoke("regexp_extract_all", _col(col), lit(pattern), lit(idx))
    # @return [Column] `col` with `pattern` replaced by `replacement`.
    def regexp_replace(col, pattern, replacement) = Column.invoke("regexp_replace", _col(col), lit(pattern), lit(replacement))
    # @return [Column] whether `col` matches `pattern`.
    def regexp_like(col, pattern) = Column.invoke("regexp_like", _col(col), lit(pattern))
    def regexp_count(col, pattern) = Column.invoke("regexp_count", _col(col), lit(pattern))
    def regexp_substr(col, pattern) = Column.invoke("regexp_substr", _col(col), lit(pattern))
    # @return [Column] overlay `replace` into `col` at `pos` for `len` chars.
    def overlay(col, replace, pos, len = -1) = Column.invoke("overlay", _col(col), _col(replace), lit(pos), lit(len))
    # @return [Column] SHA-2 hash with the given bit length (224/256/384/512).
    def sha2(col, num_bits) = Column.invoke("sha2", _col(col), lit(num_bits))
    # @return [Column] convert a number string from `from_base` to `to_base`.
    def conv(col, from_base, to_base) = Column.invoke("conv", _col(col), lit(from_base), lit(to_base))
    # @return [Column] left shift / right shift by literal bit counts.
    def shiftleft(col, num_bits) = Column.invoke("shiftleft", _col(col), lit(num_bits))
    def shiftright(col, num_bits) = Column.invoke("shiftright", _col(col), lit(num_bits))
    def shiftrightunsigned(col, num_bits) = Column.invoke("shiftrightunsigned", _col(col), lit(num_bits))

    # ---- Date / time functions with literal arguments ---------------------

    def date_format(col, fmt) = Column.invoke("date_format", _col(col), lit(fmt))
    def to_date(col, fmt = nil) = fmt ? Column.invoke("to_date", _col(col), lit(fmt)) : Column.invoke("to_date", _col(col))
    def to_timestamp(col, fmt = nil) = fmt ? Column.invoke("to_timestamp", _col(col), lit(fmt)) : Column.invoke("to_timestamp", _col(col))
    def date_add(col, days) = Column.invoke("date_add", _col(col), lit(days))
    def date_sub(col, days) = Column.invoke("date_sub", _col(col), lit(days))
    def datediff(end_col, start_col) = Column.invoke("datediff", _col(end_col), _col(start_col))
    def add_months(col, months) = Column.invoke("add_months", _col(col), lit(months))
    def months_between(d1, d2, round_off = true) = Column.invoke("months_between", _col(d1), _col(d2), lit(round_off))
    def next_day(col, day_of_week) = Column.invoke("next_day", _col(col), lit(day_of_week))
    def trunc(col, fmt) = Column.invoke("trunc", _col(col), lit(fmt))
    def date_trunc(fmt, col) = Column.invoke("date_trunc", lit(fmt), _col(col))
    def from_unixtime(col, fmt = "yyyy-MM-dd HH:mm:ss") = Column.invoke("from_unixtime", _col(col), lit(fmt))

    def unix_timestamp(col = nil, fmt = "yyyy-MM-dd HH:mm:ss")
      col.nil? ? Column.invoke("unix_timestamp") : Column.invoke("unix_timestamp", _col(col), lit(fmt))
    end

    def from_utc_timestamp(col, tz) = Column.invoke("from_utc_timestamp", _col(col), lit(tz))
    def to_utc_timestamp(col, tz) = Column.invoke("to_utc_timestamp", _col(col), lit(tz))
    def make_date(year, month, day) = Column.invoke("make_date", _col(year), _col(month), _col(day))

    # ---- JSON / CSV --------------------------------------------------------

    def get_json_object(col, path) = Column.invoke("get_json_object", _col(col), lit(path))
    def json_tuple(col, *fields) = Column.invoke("json_tuple", _col(col), *fields.map { |f| lit(f) })

    # @param schema [Types::DataType, String]
    def from_json(col, schema, options = {})
      schema_col = schema.is_a?(Types::DataType) ? lit(schema.json) : lit(schema.to_s)
      args = [_col(col), schema_col] + options.flat_map { |k, v| [lit(k.to_s), lit(v.to_s)] }
      Column.invoke("from_json", *args)
    end

    def to_json(col, options = {})
      args = [_col(col)] + options.flat_map { |k, v| [lit(k.to_s), lit(v.to_s)] }
      Column.invoke("to_json", *args)
    end

    def schema_of_json(json, options = {})
      Column.invoke("schema_of_json", _lit_or_col(json), *options.flat_map { |k, v| [lit(k.to_s), lit(v.to_s)] })
    end

    # ---- Array / map functions with value arguments -----------------------

    def array_contains(col, value) = Column.invoke("array_contains", _col(col), lit(value))
    def array_position(col, value) = Column.invoke("array_position", _col(col), lit(value))
    def array_remove(col, element) = Column.invoke("array_remove", _col(col), lit(element))
    def array_repeat(col, count) = Column.invoke("array_repeat", _col(col), lit(count))
    def array_append(col, value) = Column.invoke("array_append", _col(col), lit(value))
    def array_prepend(col, value) = Column.invoke("array_prepend", _col(col), lit(value))
    def array_insert(col, pos, value) = Column.invoke("array_insert", _col(col), lit(pos), lit(value))

    def array_join(col, delimiter, null_replacement = nil)
      if null_replacement.nil?
        Column.invoke("array_join", _col(col),
                      lit(delimiter))
      else
        Column.invoke("array_join", _col(col), lit(delimiter), lit(null_replacement))
      end
    end

    def element_at(col, extraction) = Column.invoke("element_at", _col(col), lit(extraction))
    def slice(col, start, length) = Column.invoke("slice", _col(col), _lit_or_col(start), _lit_or_col(length))

    def sequence(start, stop, step = nil)
      step.nil? ? Column.invoke("sequence", _col(start), _col(stop)) : Column.invoke("sequence", _col(start), _col(stop), _col(step))
    end

    def map_contains_key(col, key) = Column.invoke("map_contains_key", _col(col), lit(key))

    # ---- Window / analytic functions --------------------------------------

    def lag(col, offset = 1, default = nil) = Column.invoke("lag", _col(col), lit(offset), lit(default))
    def lead(col, offset = 1, default = nil) = Column.invoke("lead", _col(col), lit(offset), lit(default))
    def ntile(n) = Column.invoke("ntile", lit(n))
    def nth_value(col, offset, ignore_nulls = false) = Column.invoke("nth_value", _col(col), lit(offset), lit(ignore_nulls))

    # ---- Sorting helpers ---------------------------------------------------

    def sort_array(col, asc = true) = Column.invoke("sort_array", _col(col), lit(asc))

    # ---- Randomness --------------------------------------------------------

    def rand(seed = nil) = seed.nil? ? Column.invoke("rand") : Column.invoke("rand", lit(seed))
    def randn(seed = nil) = seed.nil? ? Column.invoke("randn") : Column.invoke("randn", lit(seed))

    # ---- Higher-order (lambda) functions -----------------------------------

    # Transform each element of an array. The block receives a {Column} (and
    # optionally the index) and returns a {Column}.
    # @yieldparam element [Column]
    # @return [Column]
    def transform(col, &block) = Column.invoke("transform", _col(col), _lambda(block))
    def exists(col, &block) = Column.invoke("exists", _col(col), _lambda(block))
    def forall(col, &block) = Column.invoke("forall", _col(col), _lambda(block))
    def filter(col, &block) = Column.invoke("filter", _col(col), _lambda(block))
    def zip_with(left, right, &block) = Column.invoke("zip_with", _col(left), _col(right), _lambda(block))
    def transform_keys(col, &block) = Column.invoke("transform_keys", _col(col), _lambda(block))
    def transform_values(col, &block) = Column.invoke("transform_values", _col(col), _lambda(block))
    def map_filter(col, &block) = Column.invoke("map_filter", _col(col), _lambda(block))
    def map_zip_with(c1, c2, &block) = Column.invoke("map_zip_with", _col(c1), _col(c2), _lambda(block))

    # Aggregate (fold) an array. `merge` combines accumulator and element;
    # optional `finish` post-processes the result.
    # @return [Column]
    def aggregate(col, initial, merge, finish = nil)
      args = [_col(col), _col(initial), _lambda(merge)]
      args << _lambda(finish) if finish
      Column.invoke("aggregate", *args)
    end

    # ---- DataFrame-level helper -------------------------------------------

    # Mark a DataFrame for broadcast (map-side) join.
    # @param df [DataFrame]
    # @return [DataFrame]
    def broadcast(df) = df.hint("broadcast")

    # UDFs require a server-side execution environment (Python/Scala) and are not
    # supported by the pure-Ruby client.
    def udf(*)
      raise NotImplementedError, "User-defined functions are not supported by the Ruby Spark Connect client"
    end

    # ---- Generated uniform functions --------------------------------------
    # Functions whose arguments are all ColumnOrName (a String denotes a column
    # name). Defined programmatically to keep the surface complete and compact.

    UNIFORM = %w[
      sum avg mean max min first last stddev stddev_samp stddev_pop variance var_samp var_pop
      skewness kurtosis collect_list collect_set first_value last_value max_by min_by corr
      covar_pop covar_samp median mode any_value every some bit_and bit_or bit_xor bool_and bool_or
      product count_if grouping
      abs acos acosh asin asinh atan atanh atan2 bin cbrt ceil ceiling cos cosh cot csc degrees
      exp expm1 factorial floor hypot ln log log2 log10 log1p negative negate positive pow power
      radians rint sec signum sin sinh sqrt tan tanh hex unhex pmod isnan isnull positive
      upper lower ltrim rtrim trim length char_length character_length octet_length bit_length
      reverse ascii base64 unbase64 initcap soundex crc32 md5 sha1 sha ucase lcase
      size cardinality array_distinct array_max array_min array_compact flatten explode explode_outer
      posexplode posexplode_outer inline inline_outer map_keys map_values map_entries map_from_entries
      array_sort shuffle arrays_zip map_concat concat greatest least hash xxhash64
      array_union array_intersect array_except arrays_overlap
      year quarter month dayofmonth day dayofweek dayofyear hour minute second weekofyear last_day
      weekday unix_date unix_micros unix_millis unix_seconds timestamp_seconds timestamp_millis
      timestamp_micros date_from_unix_date
      bitwise_not bit_count typeof
    ].uniq.freeze

    UNIFORM.each do |fn|
      define_method(fn) { |*cols| Column.invoke(fn, *cols.map { |c| _col(c) }) }
    end

    # No-argument functions.
    NO_ARG = %w[
      current_date current_timestamp now current_timezone current_user current_catalog
      current_database current_schema monotonically_increasing_id spark_partition_id
      input_file_name input_file_block_start input_file_block_length version uuid
      row_number rank dense_rank percent_rank cume_dist
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

    @lambda_counter = 0

    class << self
      # @api private
      attr_accessor :lambda_counter
    end

    # Build a {Column} wrapping a LambdaFunction from a Ruby block. The block is
    # called with one or more lambda-variable columns and must return a {Column}.
    # @api private
    def _lambda(block)
      arity = block.arity.negative? ? 1 : [block.arity, 1].max
      Functions.lambda_counter += 1
      names = (0...arity).map { |i| "x_#{Functions.lambda_counter}_#{i}" }
      vars = names.map do |n|
        Proto::Expression::UnresolvedNamedLambdaVariable.new(name_parts: [n])
      end
      cols = vars.map { |v| Column.new(Proto::Expression.new(unresolved_named_lambda_variable: v)) }
      body = block.call(*cols)
      Column.new(Proto::Expression.new(
                   lambda_function: Proto::Expression::LambdaFunction.new(function: body.to_expr, arguments: vars)
                 ))
    end
  end

  # Short alias for {Functions}: `SparkConnect::F.col("x")`.
  F = Functions
end
