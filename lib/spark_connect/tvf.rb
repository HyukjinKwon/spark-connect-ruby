# frozen_string_literal: true

module SparkConnect
  # Built-in table-valued functions, returned by {SparkSession#tvf}. Mirrors
  # PySpark's `spark.tvf` (`TableValuedFunction`). Arguments are {Column}s or
  # column names; wrap literals with {Functions.lit}.
  #
  # @example
  #   spark.tvf.explode(F.array(F.lit(1), F.lit(2))).show
  #   spark.tvf.sql_keywords.limit(5).collect
  class TableValuedFunction
    Proto = SparkConnect::Proto

    # @param session [SparkSession]
    def initialize(session)
      @session = session
    end

    # Same as {SparkSession#range}.
    # @return [DataFrame]
    def range(start, end_ = nil, step = 1, num_partitions = nil)
      @session.range(start, end_, step, num_partitions)
    end

    %w[explode explode_outer inline inline_outer posexplode posexplode_outer variant_explode
       variant_explode_outer].each do |name|
      define_method(name) { |input| call(name, input) }
    end

    # @!method explode(collection)
    #   One row per element of an array (or entry of a map). @return [DataFrame]
    # @!method explode_outer(collection)
    #   Like {#explode}, but a null/empty input yields one row of nulls. @return [DataFrame]
    # @!method inline(input)
    #   One row per struct of an array of structs. @return [DataFrame]
    # @!method inline_outer(input)
    #   Like {#inline}, keeping null/empty inputs. @return [DataFrame]
    # @!method posexplode(collection)
    #   Like {#explode} with the element position. @return [DataFrame]
    # @!method posexplode_outer(collection)
    #   Like {#posexplode}, keeping null/empty inputs. @return [DataFrame]
    # @!method variant_explode(input)
    #   One row per element/field of a VARIANT array/object. @return [DataFrame]
    # @!method variant_explode_outer(input)
    #   Like {#variant_explode}, keeping null/empty inputs. @return [DataFrame]

    # The given fields of a JSON string, one column each.
    # @return [DataFrame]
    def json_tuple(input, *fields)
      raise IllegalArgumentError, "json_tuple requires at least one field" if fields.empty?

      call("json_tuple", input, *fields)
    end

    # Separate `fields` into `n` rows.
    # @return [DataFrame]
    def stack(n, *fields) = call("stack", n, *fields)

    # @return [DataFrame] the collations supported by the server.
    def collations = call("collations")

    # @return [DataFrame] the SQL keywords of the server.
    def sql_keywords = call("sql_keywords")

    # @return [DataFrame] the Python worker logs (when enabled on the server).
    def python_worker_logs = call("python_worker_logs")

    private

    def call(name, *args)
      tvf = Proto::UnresolvedTableValuedFunction.new(
        function_name: name, arguments: args.map { |a| Functions._col(a).to_expr }
      )
      relation = PlanBuilder.relation(@session, unresolved_table_valued_function: tvf)
      DataFrame.new(@session, PlanBuilder.with_subquery_references(@session, relation))
    end
  end
end
