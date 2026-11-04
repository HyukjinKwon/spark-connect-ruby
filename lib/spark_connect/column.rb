# frozen_string_literal: true

require "bigdecimal"
require "date"

module SparkConnect
  # A column expression: a lazily-evaluated reference to a column or a
  # computation over columns. Columns are immutable; operators and methods
  # return new {Column}s.
  #
  # A {Column} wraps a protobuf `Expression`. Build them with
  # {SparkConnect::Functions.col}, {SparkConnect::Functions.lit}, by indexing a
  # DataFrame (`df["id"]`), or by combining other columns with operators.
  #
  # @example
  #   F = SparkConnect::F
  #   (F.col("age") + 1).alias("next_age")
  #   F.col("name").like("a%") & (F.col("age") >= 18)
  class Column
    Proto = SparkConnect::Proto

    # @return [Spark::Connect::Expression] the wrapped protobuf expression.
    attr_reader :expr

    # @param expr [Spark::Connect::Expression]
    def initialize(expr)
      @expr = expr
    end

    # @return [Spark::Connect::Expression]
    def to_expr
      @expr
    end

    class << self
      # Wrap an existing protobuf expression.
      # @return [Column]
      def from_expr(expr)
        new(expr)
      end

      # An unresolved attribute reference by (possibly dotted) name. The special
      # name `"*"` expands to all columns and `"t.*"` to all columns of `t`.
      #
      # @param name [String]
      # @param plan_id [Integer, nil] resolve against the relation with this plan
      #   id (set by {DataFrame#[]} so self-joins stay unambiguous).
      # @param is_metadata_column [Boolean, nil] reference a metadata column.
      # @return [Column]
      def from_name(name, plan_id: nil, is_metadata_column: nil)
        name = name.to_s
        if name == "*" || name.end_with?(".*")
          star = Proto::Expression::UnresolvedStar.new
          star.unparsed_target = name unless name == "*"
          star.plan_id = plan_id if plan_id
          new(Proto::Expression.new(unresolved_star: star))
        else
          attr = Proto::Expression::UnresolvedAttribute.new(unparsed_identifier: name)
          attr.plan_id = plan_id if plan_id
          attr.is_metadata_column = is_metadata_column unless is_metadata_column.nil?
          new(Proto::Expression.new(unresolved_attribute: attr))
        end
      end

      # Build a literal column from a Ruby value.
      #
      # @param value [Object] nil, Boolean, Integer, Float, String, Symbol,
      #   Time, Date, BigDecimal, Array, Hash, or an existing {Column}.
      # @return [Column]
      def lit(value)
        return value if value.is_a?(Column)

        new(Proto::Expression.new(literal: to_literal(value)))
      end

      # Build an `UnresolvedFunction` call column.
      #
      # @param name [String] the Spark function name.
      # @param args [Array<Column, Object>] arguments (non-columns become literals).
      # @param is_distinct [Boolean]
      # @return [Column]
      def invoke(name, *args, is_distinct: false)
        new(Proto::Expression.new(
              unresolved_function: Proto::Expression::UnresolvedFunction.new(
                function_name: name.to_s,
                arguments: args.map { |a| to_col(a).to_expr },
                is_distinct: is_distinct
              )
            ))
      end

      # Coerce a value into a {Column} (literals are wrapped).
      # @return [Column]
      def to_col(value)
        value.is_a?(Column) ? value : lit(value)
      end

      # Encode a Ruby value as a protobuf `Expression.Literal`.
      #
      # @param value [Object]
      # @return [Spark::Connect::Expression::Literal]
      def to_literal(value)
        l = Proto::Expression::Literal
        case value
        when nil
          l.new(null: Types.null.to_proto)
        when true, false
          l.new(boolean: value)
        when Integer
          if value.between?(-2_147_483_648, 2_147_483_647)
            l.new(integer: value)
          else
            l.new(long: value)
          end
        when Float
          l.new(double: value)
        when BigDecimal
          # Like PySpark, tag decimal literals as DecimalType(10, 0); the server
          # widens that to fit the value (max of the declared and the value's own
          # precision/scale), so nothing is rounded.
          l.new(decimal: l::Decimal.new(value: decimal_string(value), precision: 10, scale: 0))
        when Rational
          l.new(double: value.to_f)
        when String
          if value.encoding == Encoding::ASCII_8BIT
            l.new(binary: value)
          else
            l.new(string: value)
          end
        when Symbol
          l.new(string: value.to_s)
        when Time
          l.new(timestamp: (value.to_r * 1_000_000).to_i)
        when DateTime
          l.new(timestamp: (value.to_time.to_r * 1_000_000).to_i)
        when Date
          l.new(date: (value - Date.new(1970, 1, 1)).to_i)
        when Array
          elem_type = infer_array_element_type(value)
          l.new(array: l::Array.new(
            element_type: elem_type.to_proto,
            elements: value.map { |v| to_literal(v) }
          ))
        when Hash
          key_type = value.empty? ? Types.string : infer_type(value.keys.first)
          val_type = value.empty? ? Types.string : infer_type(value.values.first)
          l.new(map: l::Map.new(
            key_type: key_type.to_proto,
            value_type: val_type.to_proto,
            keys: value.keys.map { |k| to_literal(k) },
            values: value.values.map { |v| to_literal(v) }
          ))
        else
          raise IllegalArgumentError, "Unsupported literal value of type #{value.class}: #{value.inspect}"
        end
      end

      # Convert a protobuf `Expression.Literal` into the corresponding Ruby
      # value (the inverse of {to_literal}; used for observed metrics).
      #
      # @param literal [Spark::Connect::Expression::Literal]
      # @return [Object]
      def literal_value(literal)
        kind = literal.literal_type
        case kind
        when nil, :null then nil
        when :decimal then BigDecimal(literal.decimal.value)
        when :date then Date.new(1970, 1, 1) + literal.date
        when :timestamp then Time.at(Rational(literal.timestamp, 1_000_000)).utc
        when :timestamp_ntz then Time.at(Rational(literal.timestamp_ntz, 1_000_000)).utc
        when :array then literal.array.elements.map { |e| literal_value(e) }
        when :map then literal.map.keys.map { |k| literal_value(k) }.zip(literal.map.values.map { |v| literal_value(v) }).to_h
        when :struct then literal.struct.elements.map { |e| literal_value(e) }
        when :specialized_array
          arr = literal.specialized_array
          arr.public_send(arr.value_type).values.to_a
        else
          value = literal.public_send(kind)
          value.is_a?(Google::Protobuf::MessageExts) ? value.to_h : value
        end
      end

      # Infer the Spark {Types::DataType} for a Ruby value (used when building
      # array/map literals). Mirrors PySpark's literal type inference.
      #
      # @param value [Object]
      # @return [Types::DataType]
      def infer_type(value)
        case value
        when nil then Types.null
        when true, false then Types.boolean
        when Integer
          value.between?(-2_147_483_648, 2_147_483_647) ? Types.integer : Types.long
        when Float, Rational then Types.double
        when BigDecimal then Types.decimal(38, 18)
        when String then value.encoding == Encoding::ASCII_8BIT ? Types.binary : Types.string
        when Symbol then Types.string
        when Time, DateTime then Types.timestamp
        when Date then Types.date
        when Array then Types.array(value.empty? ? Types.null : infer_type(value.find { |v| !v.nil? }))
        when Hash
          Types.map(value.empty? ? Types.string : infer_type(value.keys.first),
                    value.empty? ? Types.string : infer_type(value.values.first))
        else
          raise IllegalArgumentError, "Cannot infer Spark type for #{value.class}"
        end
      end

      private

      # Render a BigDecimal like Python's `str(Decimal)` for the common cases:
      # integral values without a fractional part ("12"), others in plain
      # notation ("1.5").
      def decimal_string(value)
        value.frac.zero? ? value.to_i.to_s : value.to_s("F")
      end

      def infer_array_element_type(array)
        sample = array.find { |v| !v.nil? }
        sample.nil? ? Types.null : infer_type(sample)
      end
    end

    # ---- Arithmetic --------------------------------------------------------
    def +(other) = bin_op("+", other)
    def -(other) = bin_op("-", other)
    def *(other) = bin_op("*", other)
    def /(other) = bin_op("/", other)
    def %(other) = bin_op("%", other)
    def -@ = Column.invoke("negative", self)
    def +@ = self

    # Raise this column to the power of `other`.
    # @return [Column]
    def **(other) = bin_op("power", other)

    # ---- Comparison --------------------------------------------------------
    def ==(other) = bin_op("==", other)
    def !=(other) = bin_op("!=", other)
    def <(other) = bin_op("<", other)
    def <=(other) = bin_op("<=", other)
    def >(other) = bin_op(">", other)
    def >=(other) = bin_op(">=", other)

    # Null-safe equality (`<=>` in Spark SQL): `null <=> null` is true.
    # @return [Column]
    def eq_null_safe(other) = bin_op("<=>", other)

    # ---- Boolean -----------------------------------------------------------
    def &(other) = bin_op("and", other)
    def |(other) = bin_op("or", other)

    def !
      Column.invoke("not", self)
    end
    alias not !

    # ---- Bitwise -----------------------------------------------------------
    def bitwise_and(other) = bin_op("&", other)
    def bitwise_or(other) = bin_op("|", other)
    def bitwise_xor(other) = bin_op("^", other)
    alias bitwiseAND bitwise_and
    alias bitwiseOR bitwise_or
    alias bitwiseXOR bitwise_xor

    # Null-safe equality alias matching PySpark's `eqNullSafe`.
    # @return [Column]
    def eqNullSafe(other) = eq_null_safe(other)

    # ---- Null / membership predicates -------------------------------------
    def is_null = Column.invoke("isNull", self)
    def is_not_null = Column.invoke("isNotNull", self)
    def is_nan = Column.invoke("isNaN", self)
    alias isNull is_null
    alias isNotNull is_not_null
    alias isNaN is_nan

    # True if the column's value is in `values`, or - given a single-column
    # {DataFrame} - in the rows of that subquery (`IN (SELECT ...)`). A struct
    # column (`F.struct("a", "b")`) compares several columns against a
    # multi-column subquery. IN subqueries need a Spark 4.1+ server.
    #
    # @param values [Array<Object>, DataFrame]
    # @return [Column]
    def isin(*values)
      return in_subquery(values.first) if values.size == 1 && values.first.is_a?(DataFrame)

      values = values.first if values.size == 1 && values.first.is_a?(Array)
      Column.invoke("in", self, *Array(values))
    end
    alias in_list isin

    # True if `lower <= self <= upper`.
    # @return [Column]
    def between(lower, upper)
      (self >= lower) & (self <= upper)
    end

    # ---- String predicates -------------------------------------------------
    def like(pattern) = bin_op("like", pattern)
    def rlike(pattern) = bin_op("rlike", pattern)
    def ilike(pattern) = bin_op("ilike", pattern)
    def contains(other) = bin_op("contains", other)
    def startswith(other) = bin_op("startswith", other)
    def endswith(other) = bin_op("endswith", other)

    # Substring of length `len` starting at 1-based position `start`.
    # @return [Column]
    def substr(start, len)
      Column.invoke("substr", self, start, len)
    end

    # ---- Complex-type access ----------------------------------------------
    # Extract an array element by index, a map value by key, or a struct field.
    # @return [Column]
    def [](key)
      get_item(key)
    end

    def get_item(key)
      Column.new(Proto::Expression.new(
                   unresolved_extract_value: Proto::Expression::UnresolvedExtractValue.new(
                     child: @expr, extraction: Column.lit(key).to_expr
                   )
                 ))
    end

    # Extract a struct field by name.
    # @return [Column]
    def get_field(name)
      get_item(name.is_a?(Column) ? name : name.to_s)
    end
    alias getItem get_item
    alias getField get_field

    # Add or replace the struct field `field_name` with `col`.
    #
    # @param field_name [String]
    # @param col [Column]
    # @return [Column]
    def with_field(field_name, col)
      raise IllegalArgumentError, "field_name must be a String, got #{field_name.class}" unless field_name.is_a?(String)
      raise IllegalArgumentError, "col must be a Column, got #{col.class}" unless col.is_a?(Column)

      Column.new(Proto::Expression.new(update_fields: Proto::Expression::UpdateFields.new(
        struct_expression: @expr, field_name: field_name, value_expression: col.to_expr
      )))
    end
    alias withField with_field

    # Drop one or more (possibly nested, dotted) fields from a struct column.
    #
    # @param field_names [Array<String>]
    # @return [Column]
    def drop_fields(*field_names)
      raise IllegalArgumentError, "drop_fields requires at least one field name" if field_names.empty?

      expr = field_names.reduce(@expr) do |acc, name|
        raise IllegalArgumentError, "field name must be a String, got #{name.class}" unless name.is_a?(String)

        Proto::Expression.new(update_fields: Proto::Expression::UpdateFields.new(struct_expression: acc, field_name: name))
      end
      Column.new(expr)
    end
    alias dropFields drop_fields

    # ---- Aliasing / naming -------------------------------------------------
    # Assign one or more output names. With multiple names the expression must
    # produce a struct/multiple columns (e.g. `inline`).
    #
    # @param names [Array<String>]
    # @param metadata [Hash, nil] optional JSON metadata for a single alias.
    # @return [Column]
    def alias(*names, metadata: nil)
      a = Proto::Expression::Alias.new(expr: @expr, name: names.map(&:to_s))
      a.metadata = JSON.generate(metadata) if metadata
      Column.new(Proto::Expression.new(alias: a))
    end
    alias name alias
    alias as alias

    # ---- Casting -----------------------------------------------------------
    # Cast to another type, given either a {Types::DataType} or a DDL type
    # string (e.g. `"int"`, `"decimal(10,2)"`).
    #
    # @param data_type [Types::DataType, String]
    # @return [Column]
    def cast(data_type)
      c = Proto::Expression::Cast.new(expr: @expr)
      if data_type.is_a?(String)
        c.type_str = data_type
      else
        c.type = data_type.to_proto
      end
      Column.new(Proto::Expression.new(cast: c))
    end
    alias as_type cast
    alias astype cast

    # Like {#cast}, but returns null instead of failing when the value cannot
    # be cast (TRY mode).
    #
    # @param data_type [Types::DataType, String]
    # @return [Column]
    def try_cast(data_type)
      c = cast(data_type).to_expr.cast
      c.eval_mode = :EVAL_MODE_TRY
      Column.new(Proto::Expression.new(cast: c))
    end

    # Mark this column as a reference to the outer query inside a subquery
    # (e.g. a correlated {DataFrame#scalar}). Over Spark Connect the server
    # resolves outer references itself, so this returns an equivalent column,
    # like PySpark.
    # @return [Column]
    def outer
      Column.new(@expr)
    end

    # Apply a function (a block or callable) to this column and return its
    # result, enabling fluent custom transformations.
    #
    # @yieldparam col [Column] self
    # @return [Column]
    def transform(fn = nil, &block)
      fn ||= block
      raise IllegalArgumentError, "transform requires a block or callable" unless fn

      fn.call(self)
    end

    # ---- Sort ordering -----------------------------------------------------
    def asc = sort_order(:SORT_DIRECTION_ASCENDING, :SORT_NULLS_FIRST)
    def desc = sort_order(:SORT_DIRECTION_DESCENDING, :SORT_NULLS_LAST)
    def asc_nulls_first = sort_order(:SORT_DIRECTION_ASCENDING, :SORT_NULLS_FIRST)
    def asc_nulls_last = sort_order(:SORT_DIRECTION_ASCENDING, :SORT_NULLS_LAST)
    def desc_nulls_first = sort_order(:SORT_DIRECTION_DESCENDING, :SORT_NULLS_FIRST)
    def desc_nulls_last = sort_order(:SORT_DIRECTION_DESCENDING, :SORT_NULLS_LAST)

    # ---- CASE WHEN ---------------------------------------------------------
    # Add a branch to a CASE expression started by {Functions.when}.
    #
    # @return [Column]
    def when(condition, value)
      unless @expr.expr_type == :unresolved_function && @expr.unresolved_function.function_name == "when"
        raise IllegalArgumentError, "when() can only be applied on a Column previously generated by when()"
      end

      args = @expr.unresolved_function.arguments.to_a + [Column.to_col(condition).to_expr, Column.to_col(value).to_expr]
      Column.new(Proto::Expression.new(
                   unresolved_function: Proto::Expression::UnresolvedFunction.new(function_name: "when", arguments: args)
                 ))
    end

    # Provide the default (ELSE) value for a CASE expression.
    # @return [Column]
    def otherwise(value)
      unless @expr.expr_type == :unresolved_function && @expr.unresolved_function.function_name == "when"
        raise IllegalArgumentError, "otherwise() can only be applied on a Column previously generated by when()"
      end

      args = @expr.unresolved_function.arguments.to_a + [Column.to_col(value).to_expr]
      Column.new(Proto::Expression.new(
                   unresolved_function: Proto::Expression::UnresolvedFunction.new(function_name: "when", arguments: args)
                 ))
    end

    # ---- Windowing ---------------------------------------------------------
    # Define a windowed aggregation / analytic computation over this column.
    #
    # @param window [WindowSpec]
    # @return [Column]
    def over(window)
      w = Proto::Expression::Window.new(
        window_function: @expr,
        partition_spec: window.partition_spec,
        order_spec: window.order_spec
      )
      w.frame_spec = window.frame_spec if window.frame_spec
      Column.new(Proto::Expression.new(window: w))
    end

    def to_s
      "Column<#{@expr.expr_type}>"
    end
    alias inspect to_s

    private

    def in_subquery(df)
      values =
        if @expr.expr_type == :unresolved_function && @expr.unresolved_function.function_name == "struct"
          @expr.unresolved_function.arguments.to_a
        else
          [@expr]
        end
      df.subquery_expression(:SUBQUERY_TYPE_IN, in_subquery_values: values)
    end

    def bin_op(name, other)
      Column.invoke(name, self, Column.to_col(other))
    end

    def sort_order(direction, null_ordering)
      Column.new(Proto::Expression.new(
                   sort_order: Proto::Expression::SortOrder.new(
                     child: @expr, direction: direction, null_ordering: null_ordering
                   )
                 ))
    end
  end
end
