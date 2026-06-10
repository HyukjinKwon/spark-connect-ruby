# frozen_string_literal: true

module SparkConnect
  # An ordered collection of named fields representing a single row of a
  # {DataFrame}, returned by {DataFrame#collect}, {DataFrame#take}, etc.
  #
  # Fields are accessible positionally (`row[0]`), by name (`row["id"]` or
  # `row.id`), and the whole row converts cleanly to a Hash or Array.
  #
  # @example
  #   row = SparkConnect::Row.new({ "id" => 1, "name" => "alice" })
  #   row[0]        #=> 1
  #   row["name"]   #=> "alice"
  #   row.name      #=> "alice"
  #   row.to_h      #=> {"id"=>1, "name"=>"alice"}
  class Row
    include Enumerable

    # @return [Array<String>] the field names, in order.
    attr_reader :fields

    # @return [Array] the field values, in order.
    attr_reader :values

    # @overload initialize(hash)
    #   @param hash [Hash] an ordered mapping of field name to value.
    # @overload initialize(values, fields:)
    #   @param values [Array] positional values
    #   @param fields [Array<String>] field names
    def initialize(data = {}, fields: nil)
      if fields
        @fields = fields.map(&:to_s)
        @values = data
      else
        @fields = data.keys.map(&:to_s)
        @values = data.values
      end
    end

    # Look up a value by zero-based index or by field name.
    #
    # @param key [Integer, String, Symbol]
    # @return [Object, nil]
    def [](key)
      case key
      when Integer then @values[key]
      else
        idx = @fields.index(key.to_s)
        idx && @values[idx]
      end
    end

    # @return [Hash] an ordered Hash of field name to value.
    def to_h
      @fields.zip(@values).to_h
    end
    alias as_dict to_h

    # @return [Array] the row's values, in order.
    def to_a
      @values.dup
    end

    # Iterate over the values in order.
    def each(&) = @values.each(&)

    # @return [Integer] number of fields.
    def length = @values.length
    alias size length

    # @return [Object] the value for `name`, raising if the field is absent.
    def field(name)
      idx = @fields.index(name.to_s)
      raise IllegalArgumentError, "No such field: #{name}" unless idx

      @values[idx]
    end

    def ==(other)
      other.is_a?(Row) && other.fields == fields && other.values == values
    end
    alias eql? ==

    def hash = [fields, values].hash

    # Allows `row.field_name` access for field names that are valid method names.
    def method_missing(name, *args)
      key = name.to_s
      if args.empty? && @fields.include?(key)
        self[key]
      else
        super
      end
    end

    def respond_to_missing?(name, include_private = false)
      @fields.include?(name.to_s) || super
    end

    def to_s
      "Row(#{@fields.zip(@values).map { |k, v| "#{k}=#{v.inspect}" }.join(', ')})"
    end
    alias inspect to_s
  end
end
