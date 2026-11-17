# frozen_string_literal: true

module SparkConnect
  module ML
    # Local linear algebra values, mirroring `pyspark.ml.linalg`: dense and sparse
    # vectors and matrices. They are accepted as ML params and literals, and are
    # returned from model attributes and from DataFrames whose columns are
    # `VectorUDT` / `MatrixUDT`.
    module Linalg
      # Base class for vectors.
      class Vector
        include Enumerable

        # @return [Integer] the number of elements.
        attr_reader :size

        # @return [Array<Float>] the dense elements.
        def to_a = Array.new(size) { |i| self[i] }
        alias to_array to_a

        # @param other [Vector]
        # @return [Boolean]
        def ==(other)
          other.is_a?(Vector) && other.size == size && other.to_a == to_a
        end
        alias eql? ==

        # @return [Integer]
        def hash = [self.class, size, to_a].hash

        # @param other [Vector, Array<Numeric>]
        # @return [Float] the dot product.
        def dot(other)
          values = other.is_a?(Vector) ? other.to_a : other
          raise IllegalArgumentError, "Dimension mismatch: #{size} vs #{values.size}" unless values.size == size

          to_a.each_with_index.sum { |v, i| v * values[i] }
        end

        # @return [Float] the Euclidean norm.
        def norm = Math.sqrt(to_a.sum { |v| v * v })

        # @return [Array<Float>] the elements, one per entry (Enumerable support).
        def each(&) = to_a.each(&)
      end

      # A vector stored as a plain array of values.
      class DenseVector < Vector
        # @param values [Array<Numeric>]
        def initialize(values)
          super()
          @values = values.map(&:to_f).freeze
          @size = @values.size
        end

        # @return [Array<Float>] the underlying values.
        attr_reader :values

        # @param index [Integer]
        # @return [Float]
        def [](index) = @values[index]

        # @return [String]
        def to_s = "[#{@values.map { |v| Linalg.format_double(v) }.join(',')}]"

        # @return [String]
        def inspect = "DenseVector(#{self})"
      end

      # A vector stored as sorted indices and their non-zero values.
      class SparseVector < Vector
        # @param size [Integer]
        # @param indices [Array<Integer>, Hash{Integer => Numeric}] indices with values, or a
        #   hash of index to value.
        # @param values [Array<Numeric>, nil] the values when `indices` is an array.
        def initialize(size, indices, values = nil)
          super()
          pairs = if indices.is_a?(Hash)
                    indices.to_a
                  else
                    raise IllegalArgumentError, "indices and values must have the same length" unless indices.size == values.size

                    indices.zip(values)
                  end
          pairs = pairs.map { |i, v| [Integer(i), v.to_f] }.sort_by(&:first)
          pairs.map(&:first).each do |i|
            raise IllegalArgumentError, "index #{i} out of range for size #{size}" if i.negative? || i >= size
          end
          @size = size
          @indices = pairs.map(&:first).freeze
          @values = pairs.map(&:last).freeze
        end

        # @return [Array<Integer>] the non-zero indices.
        attr_reader :indices

        # @return [Array<Float>] the non-zero values.
        attr_reader :values

        # @param index [Integer]
        # @return [Float]
        def [](index)
          pos = @indices.bsearch_index { |i| i >= index }
          pos && @indices[pos] == index ? @values[pos] : 0.0
        end

        # @return [String]
        def to_s = "(#{@size},[#{@indices.join(',')}],[#{@values.map { |v| Linalg.format_double(v) }.join(',')}])"

        # @return [String]
        def inspect = "SparseVector#{self}"
      end

      # Column-major (or transposed row-major) dense matrix.
      class Matrix
        # @return [Integer]
        attr_reader :num_rows, :num_cols

        # @return [Boolean]
        attr_reader :is_transposed

        # @return [Array<Array<Float>>] the matrix as rows.
        def to_a
          Array.new(num_rows) { |r| Array.new(num_cols) { |c| self[r, c] } }
        end
        alias to_array to_a

        # @param other [Matrix]
        # @return [Boolean]
        def ==(other)
          other.is_a?(Matrix) && other.num_rows == num_rows && other.num_cols == num_cols && other.to_a == to_a
        end
        alias eql? ==

        # @return [Integer]
        def hash = [self.class, num_rows, num_cols, to_a].hash
      end

      # A dense matrix. Values are column-major unless `is_transposed` is true.
      class DenseMatrix < Matrix
        # @param num_rows [Integer]
        # @param num_cols [Integer]
        # @param values [Array<Numeric>] `num_rows * num_cols` values (column-major by default).
        # @param is_transposed [Boolean]
        def initialize(num_rows, num_cols, values, is_transposed: false)
          super()
          values = values.map(&:to_f)
          raise IllegalArgumentError, "expected #{num_rows * num_cols} values, got #{values.size}" unless values.size == num_rows * num_cols

          @num_rows = num_rows
          @num_cols = num_cols
          @values = values.freeze
          @is_transposed = is_transposed
        end

        # @return [Array<Float>] the raw values as stored.
        attr_reader :values

        # @return [Float]
        def [](row, col)
          idx = is_transposed ? (row * num_cols) + col : (col * num_rows) + row
          @values[idx]
        end

        # @return [String]
        def inspect = "DenseMatrix(#{num_rows}, #{num_cols}, [#{@values.map { |v| Linalg.format_double(v) }.join(', ')}], #{is_transposed})"
      end

      # A compressed-sparse-column matrix.
      class SparseMatrix < Matrix
        # @param num_rows [Integer]
        # @param num_cols [Integer]
        # @param col_ptrs [Array<Integer>] `num_cols + 1` column pointers.
        # @param row_indices [Array<Integer>]
        # @param values [Array<Numeric>]
        # @param is_transposed [Boolean]
        def initialize(num_rows, num_cols, col_ptrs, row_indices, values, is_transposed: false)
          super()
          raise IllegalArgumentError, "colPtrs must have #{num_cols + 1} entries" unless col_ptrs.size == num_cols + 1
          raise IllegalArgumentError, "rowIndices and values must have the same length" unless row_indices.size == values.size

          @num_rows = num_rows
          @num_cols = num_cols
          @col_ptrs = col_ptrs.map(&:to_i).freeze
          @row_indices = row_indices.map(&:to_i).freeze
          @values = values.map(&:to_f).freeze
          @is_transposed = is_transposed
        end

        # @return [Array<Integer>]
        attr_reader :col_ptrs, :row_indices, :values

        # @return [Float]
        def [](row, col)
          row, col = col, row if is_transposed
          start = @col_ptrs[col]
          stop = @col_ptrs[col + 1]
          pos = (start...stop).find { |k| @row_indices[k] == row }
          pos ? @values[pos] : 0.0
        end

        # @return [String]
        def inspect = "SparseMatrix(#{num_rows}, #{num_cols}, #{col_ptrs}, #{row_indices}, #{values}, #{is_transposed})"
      end

      # Factory methods for vectors, like `pyspark.ml.linalg.Vectors`.
      module Vectors
        module_function

        # @param values [Array<Numeric>, *Numeric]
        # @return [DenseVector]
        def dense(*values)
          DenseVector.new(values.size == 1 && values.first.is_a?(Array) ? values.first : values)
        end

        # @param size [Integer]
        # @param indices [Array<Integer>, Hash{Integer => Numeric}]
        # @param values [Array<Numeric>, nil]
        # @return [SparseVector]
        def sparse(size, indices, values = nil)
          SparseVector.new(size, indices, values)
        end
      end

      # Factory methods for matrices, like `pyspark.ml.linalg.Matrices`.
      module Matrices
        module_function

        # @return [DenseMatrix]
        def dense(num_rows, num_cols, values, is_transposed: false)
          DenseMatrix.new(num_rows, num_cols, values, is_transposed: is_transposed)
        end

        # @return [SparseMatrix]
        def sparse(num_rows, num_cols, col_ptrs, row_indices, values, is_transposed: false)
          SparseMatrix.new(num_rows, num_cols, col_ptrs, row_indices, values, is_transposed: is_transposed)
        end
      end

      VECTOR_UDT = "org.apache.spark.ml.linalg.VectorUDT"
      MATRIX_UDT = "org.apache.spark.ml.linalg.MatrixUDT"

      module_function

      # Render a double the way Spark's `toString` does (whole numbers keep `.0`).
      # @api private
      def format_double(value)
        value == value.floor && value.abs < 1e15 ? format("%.1f", value) : value.to_s
      end

      # Build a vector from the Arrow struct value of a `VectorUDT` column.
      #
      # @param value [Hash] `{"type" => 0|1, "size" => ..., "indices" => [...], "values" => [...]}`
      # @return [Vector, nil]
      def vector_from_struct(value)
        return nil if value.nil?

        case fetch_key(value, "type")
        when 0 then SparseVector.new(fetch_key(value, "size"), fetch_key(value, "indices") || [], fetch_key(value, "values") || [])
        when 1 then DenseVector.new(fetch_key(value, "values") || [])
        else raise IllegalArgumentError, "unknown VectorUDT type #{fetch_key(value, 'type').inspect}"
        end
      end

      # Build a matrix from the Arrow struct value of a `MatrixUDT` column.
      #
      # @param value [Hash]
      # @return [Matrix, nil]
      def matrix_from_struct(value)
        return nil if value.nil?

        rows = fetch_key(value, "numRows")
        cols = fetch_key(value, "numCols")
        transposed = fetch_key(value, "isTransposed") ? true : false
        case fetch_key(value, "type")
        when 0
          SparseMatrix.new(rows, cols, fetch_key(value, "colPtrs"), fetch_key(value, "rowIndices"),
                           fetch_key(value, "values"), is_transposed: transposed)
        when 1
          DenseMatrix.new(rows, cols, fetch_key(value, "values"), is_transposed: transposed)
        else raise IllegalArgumentError, "unknown MatrixUDT type #{fetch_key(value, 'type').inspect}"
        end
      end

      # @api private
      def fetch_key(hash, key)
        return hash[key] if hash.key?(key)

        hash[key.to_sym]
      end

      # Replace the `VectorUDT`/`MatrixUDT` values of decoded rows with
      # {Vector}/{Matrix} objects. Rows are returned unchanged when the result
      # has no such column.
      #
      # @param rows [Array<Row>]
      # @param schema [Spark::Connect::DataType, nil] the result schema proto.
      # @return [Array<Row>]
      def convert_rows(rows, schema)
        return rows if schema.nil? || rows.empty?
        return rows unless schema.kind == :struct

        fields = Types.from_proto(schema).fields
        udt_positions = fields.each_index.select { |i| fields[i].data_type.is_a?(Types::UserDefinedType) && fields[i].data_type.jvm_class }
        return rows if udt_positions.empty?

        rows.each do |row|
          udt_positions.each do |i|
            row.values[i] = convert_udt(row.values[i], fields[i].data_type)
          end
        end
        rows
      end

      # Convert a value that a `VectorUDT`/`MatrixUDT` column holds into the
      # local value class, given the column's Spark data type.
      #
      # @param value [Object] the decoded column value.
      # @param data_type [Types::DataType]
      # @return [Object]
      def convert_udt(value, data_type)
        return value unless data_type.is_a?(Types::UserDefinedType) && !value.nil?

        case data_type.jvm_class
        when VECTOR_UDT then vector_from_struct(value)
        when MATRIX_UDT then matrix_from_struct(value)
        else value
        end
      end
    end

    # Module-level shorthand for {Linalg::Vectors} and {Linalg::Matrices}.
    Vectors = Linalg::Vectors
    Matrices = Linalg::Matrices
  end
end
