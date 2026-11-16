# frozen_string_literal: true

module SparkConnect
  module ML
    # The `pyspark.ml.functions` helpers over Connect: converting between array
    # columns and `VectorUDT` columns. They call Spark's internal functions, so
    # the call is sent with `is_internal` set, like PySpark's connect client.
    module Functions
      module_function

      # Convert a vector column into an array column.
      #
      # @param col [Column, String] a `VectorUDT` column.
      # @param dtype [String] the element type of the result: "float64" or "float32" (Spark 4.0+).
      # @return [Column]
      def vector_to_array(col, dtype = "float64")
        Functions.internal_call("vector_to_array", Functions.column_of(col), Column.to_col(dtype))
      end

      # Convert an array column of numbers into a dense vector column.
      #
      # @param col [Column, String]
      # @return [Column]
      def array_to_vector(col)
        Functions.internal_call("array_to_vector", Functions.column_of(col))
      end

      # @api private
      # @return [Column] a column from a name or an existing column.
      def column_of(col)
        col.is_a?(Column) ? col : Column.from_name(col.to_s)
      end

      # @api private
      # @return [Column] an unresolved call to Spark's internal function `name`.
      def internal_call(name, *args)
        Column.new(Proto::Expression.new(
                     unresolved_function: Proto::Expression::UnresolvedFunction.new(
                       function_name: name,
                       arguments: args.map(&:to_expr),
                       is_internal: true
                     )
                   ))
      end
    end
  end
end
