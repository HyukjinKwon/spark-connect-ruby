# frozen_string_literal: true

module SparkConnect
  module ML
    # Encodes Ruby values as `Expression.Literal` protos (ML params, fetch
    # arguments) and decodes the literal results of `MlCommandResult`, the same
    # way PySpark's `pyspark.ml.connect.serialize` does.
    module Serialize
      Proto = SparkConnect::Proto

      INT32_MIN = -(2**31)
      INT32_MAX = (2**31) - 1

      module_function

      # @return [Proto::DataType] a `udt` data type for the given JVM class.
      def udt_type(jvm_class)
        Proto::DataType.new(udt: Proto::DataType::UDT.new(type: "udt", jvm_class: jvm_class))
      end

      # @return [Proto::Expression::Literal] a typed null.
      def null_literal
        Proto::Expression::Literal.new(null: Proto::DataType.new(null: Proto::DataType::NULL.new))
      end

      # @return [Proto::Expression::Literal] an int array (specialized array).
      def int_list(values)
        Proto::Expression::Literal.new(
          specialized_array: Proto::Expression::Literal::SpecializedArray.new(
            ints: Proto::Ints.new(values: values.map(&:to_i))
          )
        )
      end

      # @return [Proto::Expression::Literal] a double array (specialized array).
      def double_list(values)
        Proto::Expression::Literal.new(
          specialized_array: Proto::Expression::Literal::SpecializedArray.new(
            doubles: Proto::Doubles.new(values: values.map(&:to_f))
          )
        )
      end

      # @param value [Linalg::Vector, Linalg::Matrix, Object]
      # @return [Proto::Expression::Literal]
      def literal(value)
        case value
        when Linalg::Vector then vector_literal(value)
        when Linalg::Matrix then matrix_literal(value)
        when nil then null_literal
        when true, false then Proto::Expression::Literal.new(boolean: value)
        when Integer then integer_literal(value)
        when Float then Proto::Expression::Literal.new(double: value)
        when String, Symbol then Proto::Expression::Literal.new(string: value.to_s)
        when Array then array_literal(value)
        else raise IllegalArgumentError, "cannot send #{value.inspect} as an ML parameter"
        end
      end

      # @api private
      def integer_literal(value)
        if value.between?(INT32_MIN, INT32_MAX)
          Proto::Expression::Literal.new(integer: value)
        else
          Proto::Expression::Literal.new(long: value)
        end
      end

      # Arrays of numbers and strings are typed `array` literals, which is the
      # form the server reads for ML params. (Specialized arrays are used only
      # inside vector and matrix structs, where the server expects them.)
      # @api private
      def array_literal(values)
        if !values.empty? && values.all?(Numeric)
          element_type = values.all?(Integer) ? Proto::DataType.new(integer: Proto::DataType::Integer.new) : Proto::DataType.new(double: Proto::DataType::Double.new)
          elements = values.map { |v| element_type.integer ? Proto::Expression::Literal.new(integer: v) : Proto::Expression::Literal.new(double: v.to_f) }
          return Proto::Expression::Literal.new(array: Proto::Expression::Literal::Array.new(element_type: element_type,
                                                                                             elements: elements))
        end

        element_type, elements = typed_elements(values)
        Proto::Expression::Literal.new(
          array: Proto::Expression::Literal::Array.new(element_type: element_type, elements: elements)
        )
      end

      # @api private
      def typed_elements(values)
        if values.all?(String) || values.empty?
          [Proto::DataType.new(string: Proto::DataType::String.new(collation: "UTF8_BINARY")),
           values.map { |v| Proto::Expression::Literal.new(string: v) },]
        elsif values.all?(Array)
          inner_type, = typed_elements(values.first)
          [Proto::DataType.new(array: Proto::DataType::Array.new(element_type: inner_type, contains_null: true)),
           values.map { |v| array_literal(v) },]
        else
          raise IllegalArgumentError, "unsupported ML array elements #{values.inspect}"
        end
      end

      # @api private
      def vector_literal(vector)
        builder = Proto::Expression::Literal::Struct.new(struct_type: udt_type(Linalg::VECTOR_UDT))
        if vector.is_a?(Linalg::SparseVector)
          builder.elements.push(
            Proto::Expression::Literal.new(byte: 0),
            Proto::Expression::Literal.new(integer: vector.size),
            int_list(vector.indices),
            double_list(vector.values)
          )
        else
          builder.elements.push(
            Proto::Expression::Literal.new(byte: 1),
            null_literal,
            null_literal,
            double_list(vector.values)
          )
        end
        Proto::Expression::Literal.new(struct: builder)
      end

      # @api private
      def matrix_literal(matrix)
        builder = Proto::Expression::Literal::Struct.new(struct_type: udt_type(Linalg::MATRIX_UDT))
        if matrix.is_a?(Linalg::SparseMatrix)
          builder.elements.push(
            Proto::Expression::Literal.new(byte: 0),
            Proto::Expression::Literal.new(integer: matrix.num_rows),
            Proto::Expression::Literal.new(integer: matrix.num_cols),
            int_list(matrix.col_ptrs),
            int_list(matrix.row_indices),
            double_list(matrix.values),
            Proto::Expression::Literal.new(boolean: matrix.is_transposed)
          )
        else
          builder.elements.push(
            Proto::Expression::Literal.new(byte: 1),
            Proto::Expression::Literal.new(integer: matrix.num_rows),
            Proto::Expression::Literal.new(integer: matrix.num_cols),
            null_literal,
            null_literal,
            double_list(matrix.values),
            Proto::Expression::Literal.new(boolean: matrix.is_transposed)
          )
        end
        Proto::Expression::Literal.new(struct: builder)
      end

      # Build the `MlParams` message for a param map (keys are {Param}s).
      #
      # @param values [Hash{Param => Object}]
      # @return [Proto::MlParams]
      def ml_params(values)
        Proto::MlParams.new(params: values.to_h { |param, value| [param.spark_name, literal(value)] })
      end

      # Decode a literal returned by the server.
      #
      # @param literal [Proto::Expression::Literal]
      # @return [Object]
      def decode(literal)
        case literal.literal_type
        when :struct then decode_struct(literal.struct)
        when :specialized_array then decode_specialized(literal.specialized_array)
        when :array then literal.array.elements.map { |e| decode(e) }
        when :null, nil then nil
        else literal.public_send(literal.literal_type)
        end
      end

      # @api private
      def decode_struct(struct)
        jvm_class = struct.struct_type.udt.jvm_class
        case jvm_class
        when Linalg::VECTOR_UDT then vector_from_elements(struct.elements)
        when Linalg::MATRIX_UDT then matrix_from_elements(struct.elements)
        else raise IllegalArgumentError, "unsupported struct #{jvm_class} in ML result"
        end
      end

      # @api private
      def vector_from_elements(elements)
        if elements[0].byte.zero?
          Linalg::SparseVector.new(elements[1].integer, decode(elements[2]), decode(elements[3]))
        else
          Linalg::DenseVector.new(decode(elements[3]))
        end
      end

      # @api private
      def matrix_from_elements(elements)
        if elements[0].byte.zero?
          Linalg::SparseMatrix.new(elements[1].integer, elements[2].integer, decode(elements[3]), decode(elements[4]),
                                   decode(elements[5]), is_transposed: elements[6].boolean)
        else
          Linalg::DenseMatrix.new(elements[1].integer, elements[2].integer, decode(elements[5]),
                                  is_transposed: elements[6].boolean)
        end
      end

      # @api private
      def decode_specialized(array)
        %i[ints longs floats doubles bools strings].each do |field|
          values = array.public_send(field)&.values.to_a
          return values unless values.empty?
        end
        []
      end
    end
  end
end
