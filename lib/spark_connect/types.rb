# frozen_string_literal: true

require "json"

module SparkConnect
  # The Spark SQL type system.
  #
  # Every Spark data type is represented by an instance of a {DataType}
  # subclass. Types convert to and from the protobuf `DataType` message via
  # {DataType#to_proto} and {Types.from_proto}, and render a Spark-compatible
  # `simpleString` (e.g. `"array<int>"`) and `typeName` (e.g. `"integer"`).
  #
  # @example
  #   SparkConnect::Types::IntegerType.new.simple_string      #=> "int"
  #   SparkConnect::Types.array(SparkConnect::Types::StringType.new).simple_string
  #   #=> "array<string>"
  module Types
    Proto = SparkConnect::Proto

    # Abstract base class for all Spark data types.
    class DataType
      # @return [String] the Spark `simpleString` representation, e.g. `"int"`,
      #   `"array<string>"`, `"struct<a:int>"`.
      def simple_string
        type_name
      end

      # @return [String] the short type name used by Spark's JSON schema, e.g.
      #   `"integer"`, `"long"`, `"string"`.
      def type_name
        n = self.class.name.split("::").last.sub(/Type$/, "")
        n.gsub(/([a-z\d])([A-Z])/, '\1_\2').downcase
      end

      # @return [String, Hash] the Spark JSON schema fragment for this type.
      def json_value
        type_name
      end

      # @return [String] the JSON schema string for this type.
      def json
        JSON.generate(json_value)
      end

      # @return [Spark::Connect::DataType] the protobuf representation.
      def to_proto
        raise NotImplementedError, "#{self.class}#to_proto is not implemented"
      end

      def ==(other)
        other.is_a?(self.class) && other.class == self.class
      end
      alias eql? ==

      def hash
        self.class.hash
      end

      def to_s
        simple_string
      end

      def inspect
        "#<#{self.class.name} #{simple_string}>"
      end
    end

    # Helper that wraps a kind message into a `DataType` proto.
    def self.wrap(**kwargs)
      Proto::DataType.new(**kwargs)
    end

    class NullType < DataType
      def simple_string = "void"
      def type_name = "void"
      def to_proto = Types.wrap(null: Proto::DataType::NULL.new)
    end

    class BooleanType < DataType
      def type_name = "boolean"
      def to_proto = Types.wrap(boolean: Proto::DataType::Boolean.new)
    end

    class ByteType < DataType
      def simple_string = "tinyint"
      def type_name = "byte"
      def to_proto = Types.wrap(byte: Proto::DataType::Byte.new)
    end

    class ShortType < DataType
      def simple_string = "smallint"
      def type_name = "short"
      def to_proto = Types.wrap(short: Proto::DataType::Short.new)
    end

    class IntegerType < DataType
      def simple_string = "int"
      def type_name = "integer"
      def to_proto = Types.wrap(integer: Proto::DataType::Integer.new)
    end

    class LongType < DataType
      def simple_string = "bigint"
      def type_name = "long"
      def to_proto = Types.wrap(long: Proto::DataType::Long.new)
    end

    class FloatType < DataType
      def type_name = "float"
      def to_proto = Types.wrap(float: Proto::DataType::Float.new)
    end

    class DoubleType < DataType
      def type_name = "double"
      def to_proto = Types.wrap(double: Proto::DataType::Double.new)
    end

    class StringType < DataType
      # @return [String] the collation name (default `"UTF8_BINARY"`).
      attr_reader :collation

      def initialize(collation = "UTF8_BINARY")
        super()
        @collation = collation
      end

      def type_name = "string"
      def to_proto = Types.wrap(string: Proto::DataType::String.new(collation: @collation))
    end

    class BinaryType < DataType
      def type_name = "binary"
      def to_proto = Types.wrap(binary: Proto::DataType::Binary.new)
    end

    class DateType < DataType
      def type_name = "date"
      def to_proto = Types.wrap(date: Proto::DataType::Date.new)
    end

    class TimestampType < DataType
      def type_name = "timestamp"
      def to_proto = Types.wrap(timestamp: Proto::DataType::Timestamp.new)
    end

    class TimestampNTZType < DataType
      def simple_string = "timestamp_ntz"
      def type_name = "timestamp_ntz"
      def to_proto = Types.wrap(timestamp_ntz: Proto::DataType::TimestampNTZ.new)
    end

    class VariantType < DataType
      def type_name = "variant"
      def to_proto = Types.wrap(variant: Proto::DataType::Variant.new)
    end

    class DecimalType < DataType
      # @return [Integer] total number of digits (max 38).
      attr_reader :precision
      # @return [Integer] number of digits to the right of the decimal point.
      attr_reader :scale

      def initialize(precision = 10, scale = 0)
        super()
        @precision = precision
        @scale = scale
      end

      def simple_string = "decimal(#{precision},#{scale})"
      def type_name = "decimal"
      def json_value = "decimal(#{precision},#{scale})"
      def to_proto = Types.wrap(decimal: Proto::DataType::Decimal.new(precision: precision, scale: scale))

      def ==(other)
        other.is_a?(DecimalType) && other.precision == precision && other.scale == scale
      end
    end

    class CharType < DataType
      attr_reader :length

      def initialize(length)
        super()
        @length = length
      end

      def simple_string = "char(#{length})"
      def type_name = "char"
      def json_value = "char(#{length})"
      def to_proto = Types.wrap(char: Proto::DataType::Char.new(length: length))

      def ==(other) = other.is_a?(CharType) && other.length == length
    end

    class VarcharType < DataType
      attr_reader :length

      def initialize(length)
        super()
        @length = length
      end

      def simple_string = "varchar(#{length})"
      def type_name = "varchar"
      def json_value = "varchar(#{length})"
      def to_proto = Types.wrap(var_char: Proto::DataType::VarChar.new(length: length))

      def ==(other) = other.is_a?(VarcharType) && other.length == length
    end

    class DayTimeIntervalType < DataType
      DAY = 0
      HOUR = 1
      MINUTE = 2
      SECOND = 3
      attr_reader :start_field, :end_field

      def initialize(start_field = DAY, end_field = SECOND)
        super()
        @start_field = start_field
        @end_field = end_field
      end

      def simple_string = "interval day to second"
      def type_name = "interval"

      def to_proto
        Types.wrap(day_time_interval: Proto::DataType::DayTimeInterval.new(start_field: start_field, end_field: end_field))
      end
    end

    class YearMonthIntervalType < DataType
      YEAR = 0
      MONTH = 1
      attr_reader :start_field, :end_field

      def initialize(start_field = YEAR, end_field = MONTH)
        super()
        @start_field = start_field
        @end_field = end_field
      end

      def simple_string = "interval year to month"
      def type_name = "interval"

      def to_proto
        Types.wrap(year_month_interval: Proto::DataType::YearMonthInterval.new(start_field: start_field, end_field: end_field))
      end
    end

    class CalendarIntervalType < DataType
      def simple_string = "interval"
      def type_name = "calendar_interval"
      def to_proto = Types.wrap(calendar_interval: Proto::DataType::CalendarInterval.new)
    end

    # An array type. `element_type` is the type of every element; `contains_null`
    # indicates whether the array may contain `null` values.
    class ArrayType < DataType
      attr_reader :element_type, :contains_null

      def initialize(element_type, contains_null: true)
        super()
        @element_type = element_type
        @contains_null = contains_null
      end

      def simple_string = "array<#{element_type.simple_string}>"
      def type_name = "array"

      def json_value
        { "type" => "array", "elementType" => element_type.json_value, "containsNull" => contains_null }
      end

      def to_proto
        Types.wrap(array: Proto::DataType::Array.new(element_type: element_type.to_proto, contains_null: contains_null))
      end

      def ==(other)
        other.is_a?(ArrayType) && other.element_type == element_type && other.contains_null == contains_null
      end
    end

    # A map type with key and value element types.
    class MapType < DataType
      attr_reader :key_type, :value_type, :value_contains_null

      def initialize(key_type, value_type, value_contains_null: true)
        super()
        @key_type = key_type
        @value_type = value_type
        @value_contains_null = value_contains_null
      end

      def simple_string = "map<#{key_type.simple_string},#{value_type.simple_string}>"
      def type_name = "map"

      def json_value
        {
          "type" => "map",
          "keyType" => key_type.json_value,
          "valueType" => value_type.json_value,
          "valueContainsNull" => value_contains_null,
        }
      end

      def to_proto
        Types.wrap(map: Proto::DataType::Map.new(
          key_type: key_type.to_proto,
          value_type: value_type.to_proto,
          value_contains_null: value_contains_null
        ))
      end

      def ==(other)
        other.is_a?(MapType) && other.key_type == key_type &&
          other.value_type == value_type && other.value_contains_null == value_contains_null
      end
    end

    # A single field within a {StructType}.
    class StructField
      attr_reader :name, :data_type, :nullable, :metadata

      def initialize(name, data_type, nullable: true, metadata: nil)
        @name = name.to_s
        @data_type = data_type
        @nullable = nullable
        @metadata = metadata
      end

      def simple_string = "#{name}:#{data_type.simple_string}"

      def json_value
        h = { "name" => name, "type" => data_type.json_value, "nullable" => nullable }
        h["metadata"] = metadata if metadata
        h
      end

      def to_proto
        Proto::DataType::StructField.new(
          name: name,
          data_type: data_type.to_proto,
          nullable: nullable,
          metadata: metadata ? JSON.generate(metadata) : nil
        )
      end

      def ==(other)
        other.is_a?(StructField) && other.name == name &&
          other.data_type == data_type && other.nullable == nullable
      end
    end

    # A struct (row) type: an ordered collection of {StructField}s. This is the
    # type of every {DataFrame}'s schema.
    class StructType < DataType
      include Enumerable

      attr_reader :fields

      def initialize(fields = [])
        super()
        @fields = fields
      end

      # Append a field and return self (chainable builder).
      #
      # @param name [String, StructField]
      # @return [StructType]
      def add(name, data_type = nil, nullable: true, metadata: nil)
        @fields << if name.is_a?(StructField)
                     name
                   else
                     StructField.new(name, data_type, nullable: nullable, metadata: metadata)
                   end
        self
      end

      def each(&) = fields.each(&)
      def [](key) = key.is_a?(Integer) ? fields[key] : fields.find { |f| f.name == key.to_s }
      def names = fields.map(&:name)
      def length = fields.length
      alias size length

      def simple_string = "struct<#{fields.map(&:simple_string).join(',')}>"
      def type_name = "struct"

      def json_value
        { "type" => "struct", "fields" => fields.map(&:json_value) }
      end

      def to_proto
        Types.wrap(struct: Proto::DataType::Struct.new(fields: fields.map(&:to_proto)))
      end

      # A human-readable, indented tree (used by {DataFrame#print_schema}).
      #
      # @return [String]
      def tree_string
        lines = ["root"]
        fields.each { |f| append_tree(lines, f, " |") }
        "#{lines.join("\n")}\n"
      end

      def ==(other) = other.is_a?(StructType) && other.fields == fields

      private

      def append_tree(lines, field, prefix)
        dt = field.data_type
        lines << "#{prefix}-- #{field.name}: #{dt.type_name} (nullable = #{field.nullable})"
        case dt
        when StructType
          dt.fields.each { |f| append_tree(lines, f, "#{prefix}    |") }
        when ArrayType
          lines << "#{prefix}    |-- element: #{dt.element_type.type_name} (containsNull = #{dt.contains_null})"
          dt.element_type.fields.each { |f| append_tree(lines, f, "#{prefix}    |    |") } if dt.element_type.is_a?(StructType)
        when MapType
          lines << "#{prefix}    |-- key: #{dt.key_type.type_name}"
          lines << "#{prefix}    |-- value: #{dt.value_type.type_name} (valueContainsNull = #{dt.value_contains_null})"
        end
      end
    end

    # ---- Convenience constructors -----------------------------------------

    module_function

    def null = NullType.new
    def boolean = BooleanType.new
    def byte = ByteType.new
    def short = ShortType.new
    def integer = IntegerType.new
    def long = LongType.new
    def float = FloatType.new
    def double = DoubleType.new
    def string = StringType.new
    def binary = BinaryType.new
    def date = DateType.new
    def timestamp = TimestampType.new
    def timestamp_ntz = TimestampNTZType.new
    def variant = VariantType.new
    def decimal(precision = 10, scale = 0) = DecimalType.new(precision, scale)
    def array(element_type, contains_null: true) = ArrayType.new(element_type, contains_null: contains_null)
    def map(key_type, value_type, value_contains_null: true) = MapType.new(key_type, value_type, value_contains_null: value_contains_null)
    def struct(*fields) = StructType.new(fields.flatten)
    def field(name, data_type, nullable: true, metadata: nil) = StructField.new(name, data_type, nullable: nullable, metadata: metadata)

    # Convert a protobuf `DataType` message into a {DataType} instance.
    #
    # @param proto [Spark::Connect::DataType]
    # @return [DataType]
    def from_proto(proto)
      kind = proto.kind
      sub = proto.public_send(kind)
      case kind
      when :null then NullType.new
      when :boolean then BooleanType.new
      when :byte then ByteType.new
      when :short then ShortType.new
      when :integer then IntegerType.new
      when :long then LongType.new
      when :float then FloatType.new
      when :double then DoubleType.new
      when :string then StringType.new(sub.collation.empty? ? "UTF8_BINARY" : sub.collation)
      when :binary then BinaryType.new
      when :date then DateType.new
      when :timestamp then TimestampType.new
      when :timestamp_ntz then TimestampNTZType.new
      when :variant then VariantType.new
      when :calendar_interval then CalendarIntervalType.new
      when :day_time_interval then DayTimeIntervalType.new(sub.start_field || 0, sub.end_field || 3)
      when :year_month_interval then YearMonthIntervalType.new(sub.start_field || 0, sub.end_field || 1)
      when :decimal then DecimalType.new(sub.precision || 10, sub.scale || 0)
      when :char then CharType.new(sub.length)
      when :var_char then VarcharType.new(sub.length)
      when :array then ArrayType.new(from_proto(sub.element_type), contains_null: sub.contains_null)
      when :map then MapType.new(from_proto(sub.key_type), from_proto(sub.value_type), value_contains_null: sub.value_contains_null)
      when :struct
        StructType.new(sub.fields.map do |f|
          StructField.new(f.name, from_proto(f.data_type), nullable: f.nullable,
                                                           metadata: (f.metadata && !f.metadata.empty? ? JSON.parse(f.metadata) : nil))
        end)
      else
        raise IllegalArgumentError, "Unsupported proto DataType kind: #{kind}"
      end
    end
  end
end
