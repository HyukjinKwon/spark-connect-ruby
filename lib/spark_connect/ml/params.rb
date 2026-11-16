# frozen_string_literal: true

require "securerandom"

module SparkConnect
  module ML
    # Marks a param that has no default value (Spark leaves it unset).
    NO_DEFAULT = Object.new.tap do |o|
      def o.inspect = "NO_DEFAULT"
      o.freeze
    end

    # A declared ML parameter. Params are declared on the operator classes with
    # {Params.param}; the same {Param} object is shared by every instance of the
    # class, and is what {Params#param} / ParamGridBuilder use as a key.
    class Param
      # @return [Class] the class that declares the param.
      attr_reader :owner
      # @return [Symbol] the Ruby (snake_case) name, e.g. `:reg_param`.
      attr_reader :name
      # @return [String] the camelCase name the Spark server uses, e.g. `"regParam"`.
      attr_reader :spark_name
      # @return [Symbol] the value type (see {ParamTypes}).
      attr_reader :type
      # @return [String, nil] the Spark doc string.
      attr_reader :doc

      # @api private
      def initialize(owner, name, type, spark_name:, default:, doc:)
        @owner = owner
        @name = name
        @type = type
        @spark_name = spark_name
        @default = default
        @doc = doc
      end

      # @return [Boolean] whether Spark defines a default for this param.
      def default?
        !@default.equal?(NO_DEFAULT)
      end

      # @return [Object, nil] the default value (nil when there is none).
      def default
        default? ? @default : nil
      end

      # @return [String]
      def to_s = name.to_s

      # @return [String]
      def inspect = "#<SparkConnect::ML::Param #{owner&.name}##{name}>"
    end

    # Value coercion and validation per {Param} type.
    module ParamTypes
      INT_RANGE = (-(2**31)..((2**31) - 1))

      TYPES = %i[any int long double boolean string string_array int_array double_array
                 double_array_array string_array_array vector matrix].freeze

      module_function

      # Validate and normalise `value` for a param of `type`.
      #
      # @param param [Param]
      # @param value [Object]
      # @return [Object] the normalised value
      # @raise [IllegalArgumentError] when the value does not fit the param type
      def coerce(param, value)
        type = param.type
        return value if type == :any
        return value if value.nil? && type != :any # nil is rejected below for typed params

        case type
        when :int, :long then coerce_int(param, value)
        when :double then coerce_double(param, value)
        when :boolean then coerce_boolean(param, value)
        when :string then coerce_string(param, value)
        when :string_array then array_of(param, value) { |v| coerce_string(param, v) }
        when :int_array then array_of(param, value) { |v| coerce_int(param, v) }
        when :double_array then array_of(param, value) { |v| coerce_double(param, v) }
        when :double_array_array
          array_of(param, value) { |row| array_of(param, row) { |v| coerce_double(param, v) } }
        when :string_array_array
          array_of(param, value) { |row| array_of(param, row) { |v| coerce_string(param, v) } }
        when :vector then coerce_vector(param, value)
        when :matrix then coerce_matrix(param, value)
        else
          raise IllegalArgumentError, "unknown param type #{type.inspect} for #{param.name}"
        end
      end

      def coerce_int(param, value)
        raise IllegalArgumentError, "#{param.name} expects an Integer, got #{value.inspect}" unless value.is_a?(Integer)

        value
      end

      def coerce_double(param, value)
        raise IllegalArgumentError, "#{param.name} expects a Numeric, got #{value.inspect}" unless value.is_a?(Numeric)

        value.to_f
      end

      def coerce_boolean(param, value)
        raise IllegalArgumentError, "#{param.name} expects true or false, got #{value.inspect}" unless [true, false].include?(value)

        value
      end

      def coerce_string(param, value)
        raise IllegalArgumentError, "#{param.name} expects a String, got #{value.inspect}" unless value.is_a?(String) || value.is_a?(Symbol)

        value.to_s
      end

      def array_of(param, value, &)
        raise IllegalArgumentError, "#{param.name} expects an Array, got #{value.inspect}" unless value.is_a?(Array)

        value.map(&)
      end

      def coerce_vector(param, value)
        case value
        when Linalg::Vector then value
        when Array then Linalg::Vectors.dense(value.map { |v| coerce_double(param, v) })
        else raise IllegalArgumentError, "#{param.name} expects a Vector, got #{value.inspect}"
        end
      end

      def coerce_matrix(param, value)
        return value if value.is_a?(Linalg::Matrix)

        raise IllegalArgumentError, "#{param.name} expects a Matrix, got #{value.inspect}"
      end
    end

    # Behaviour shared by every ML operator (estimators, transformers, models,
    # evaluators, pipelines): declared params, values, defaults, copies and a
    # unique `uid`, like PySpark's `pyspark.ml.param.Params`.
    class Params
      class << self
        # @!visibility private
        def own_params
          @own_params ||= {}
        end

        # All params visible on this class, including inherited ones.
        #
        # @return [Hash{Symbol => Param}]
        def params
          inherited = superclass.respond_to?(:params) ? superclass.params : {}
          inherited.merge(own_params)
        end

        # Declare a param. Defines the getter `name` (returns the value or the
        # default) and the chainable setter `set_name(value)`.
        #
        # @param name [Symbol] the Ruby name, snake_case, e.g. `:max_iter`.
        # @param type [Symbol] one of {ParamTypes::TYPES}.
        # @param default [Object] the Spark default; omit for params without one.
        # @param spark_name [String, nil] the Spark name; defaults to camelCase of `name`.
        # @param doc [String, nil] the doc string.
        # @return [Param]
        def param(name, type = :any, default: NO_DEFAULT, spark_name: nil, doc: nil)
          name = name.to_sym
          raise ArgumentError, "unknown param type #{type.inspect}" unless ParamTypes::TYPES.include?(type)

          spark = spark_name || ML.camelize(name)
          declared = Param.new(self, name, type, spark_name: spark, default: default, doc: doc)
          own_params[name] = declared
          define_method(name) { get(declared) }
          define_method(:"set_#{name}") { |value| set(declared, value) }
          declared
        end

        # The Spark operator (fully qualified Scala class) this Ruby class wraps.
        # @param name [String, nil] set when declaring; omit to read.
        # @return [String, nil]
        def operator(name = nil)
          return @operator = name if name

          @operator || (superclass.respond_to?(:operator) ? superclass.operator : nil)
        end

        # @return [String] a fresh uid, e.g. `"LogisticRegression_1a2b3c4d5e6f"`.
        def random_uid
          "#{name.to_s.split('::').last || 'Params'}_#{SecureRandom.hex(6)}"
        end
      end

      # @return [String] the unique id of this instance.
      attr_reader :uid

      # @param uid [String, nil] an explicit uid, otherwise a random one.
      # @param values [Hash] initial param values, e.g. `max_iter: 10`.
      def initialize(uid: nil, **values)
        @uid = uid || self.class.random_uid
        @param_map = {}
        set_params(**values)
      end

      # @return [Array<Param>] every param of this instance, sorted by name.
      def params
        self.class.params.values.sort_by(&:name)
      end

      # Look up a declared param.
      #
      # @param name [Symbol, String, Param]
      # @return [Param]
      # @raise [IllegalArgumentError] when the param does not exist on this class
      def param(name)
        return name if name.is_a?(Param) && self.class.params.key?(name.name)

        self.class.params.fetch(name.to_sym) do
          raise IllegalArgumentError, "#{self.class.name.split('::').last} does not have a param named #{name}."
        end
      end

      # Set one param. Returns `self`.
      #
      # @param name [Symbol, String, Param]
      # @param value [Object]
      # @return [self]
      def set(name, value)
        declared = param(name)
        raise IllegalArgumentError, "#{declared.name} must not be nil" if value.nil?

        @param_map[declared] = ParamTypes.coerce(declared, value)
        self
      end

      # Set several params from keyword arguments, like PySpark's `setParams`.
      # @return [self]
      def set_params(**values)
        values.each { |k, v| set(k, v) }
        self
      end

      # The value of a param: the explicitly set value, else the default.
      #
      # @param name [Symbol, String, Param]
      # @return [Object]
      # @raise [IllegalArgumentError] when neither is available
      def get(name)
        declared = param(name)
        return @param_map[declared] if @param_map.key?(declared)
        return declared.default if declared.default?

        raise IllegalArgumentError, "Failed to find a default value for #{declared.name}"
      end

      # @return [Boolean] whether the param was set explicitly.
      def is_set?(name)
        @param_map.key?(param(name))
      end

      # @return [Boolean] whether the param has a value, set or default.
      def is_defined?(name)
        declared = param(name)
        @param_map.key?(declared) || declared.default?
      end

      # Remove an explicit value, so the default applies again.
      # @return [self]
      def clear(name)
        @param_map.delete(param(name))
        self
      end

      # The explicitly set params only (what gets sent to the server).
      #
      # @return [Hash{Param => Object}]
      def explicit_param_map
        @param_map.dup
      end

      # Every param with a value: defaults, overridden by explicit values, then by `extra`.
      #
      # @param extra [Hash{Param, Symbol, String => Object}]
      # @return [Hash{Param => Object}]
      def extract_param_map(extra = {})
        map = {}
        params.each do |declared|
          map[declared] = declared.default if declared.default?
        end
        map.merge!(@param_map)
        extra.each { |k, v| map[param(k)] = ParamTypes.coerce(param(k), v) if self.class.params.key?(key_name(k)) }
        map
      end

      # A human-readable description of every param, one line each.
      #
      # @return [String]
      def explain_params
        params.map do |declared|
          parts = ["#{declared.name}: #{declared.doc}".rstrip]
          if is_set?(declared.name)
            parts << "(default: #{declared.default.inspect}, current: #{@param_map[declared].inspect})" if declared.default?
            parts << "(current: #{@param_map[declared].inspect})" unless declared.default?
          elsif declared.default?
            parts << "(default: #{declared.default.inspect})"
          end
          parts.join(" ")
        end.join("\n")
      end

      # A copy of this instance with the same uid, explicit values and `extra`
      # applied on top (PySpark's `copy(extra)`).
      #
      # @param extra [Hash]
      # @return [Params]
      def copy(extra = {})
        that = dup
        that.instance_variable_set(:@param_map, @param_map.dup)
        extra.each do |k, v|
          that.set(k, v) if self.class.params.key?(key_name(k))
        end
        that
      end

      # Copy the explicit values of `other` that this class also declares.
      # @api private
      # @return [self]
      def copy_values_from(other)
        other.explicit_param_map.each do |declared, value|
          @param_map[param(declared.name)] = value if self.class.params.key?(declared.name)
        end
        self
      end

      # @api private
      def key_name(key)
        key.is_a?(Param) ? key.name : key.to_sym
      end

      # @return [String]
      def inspect = "#<#{self.class.name} uid=#{uid}>"
    end

    # Converts a Ruby name to the camelCase name Spark uses (`max_iter` -> `maxIter`).
    # @param name [String, Symbol]
    # @return [String]
    def self.camelize(name)
      first, *rest = name.to_s.split("_")
      [first, *rest.map(&:capitalize)].join
    end
  end
end
