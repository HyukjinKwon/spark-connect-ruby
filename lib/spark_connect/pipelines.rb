# frozen_string_literal: true

module SparkConnect
  # A timestamped event emitted by the server during a pipeline run.
  PipelineEvent = Struct.new(:timestamp, :message)

  # A Spark Declarative Pipeline (SDP) dataflow graph.
  #
  # A pipeline is built by registering **outputs** (tables, materialized views,
  # temporary views, or sinks) and the **flows** that populate them, then
  # started with {#start_run}. Each flow is defined by a {DataFrame} (an
  # unresolved relation), so you compose flows with the same API you use for
  # ordinary queries.
  #
  # Create one with {SparkSession#pipeline}.
  #
  # @example
  #   pipe = spark.pipeline(storage: "/tmp/pipeline_storage")
  #   pipe.create_materialized_view("bronze", spark.read.json("/data/raw"))
  #   pipe.create_table("silver", pipe.read("bronze").filter(F.col("ok")))
  #   events = pipe.start_run
  #
  # @note `foreach`/`foreachBatch` flows and Python query-function evaluation are
  #   not supported (they require UDFs); define each flow with a relation instead.
  class Pipeline
    Proto = SparkConnect::Proto
    PC = Proto::PipelineCommand

    OUTPUT_TYPES = {
      table: :TABLE,
      materialized_view: :MATERIALIZED_VIEW,
      temporary_view: :TEMPORARY_VIEW,
      sink: :SINK,
    }.freeze

    # @return [String] the server-assigned dataflow graph id.
    attr_reader :graph_id

    # @param session [SparkSession]
    # @param default_catalog [String, nil]
    # @param default_database [String, nil]
    # @param sql_conf [Hash{String=>String}]
    def initialize(session, default_catalog: nil, default_database: nil, sql_conf: {})
      @session = session
      cmd = PC::CreateDataflowGraph.new(sql_conf: stringify(sql_conf))
      cmd.default_catalog = default_catalog if default_catalog
      cmd.default_database = default_database if default_database
      result = dispatch(PC.new(create_dataflow_graph: cmd))
      @graph_id = result.pipeline_command_result.create_dataflow_graph_result.dataflow_graph_id
    end

    # Reference a dataset defined in this pipeline as a {DataFrame} (so later
    # flows can read from earlier outputs).
    #
    # @param name [String]
    # @return [DataFrame]
    def read(name)
      @session.read.table(name)
    end

    # Define a published table and the flow that populates it.
    #
    # @param name [String]
    # @param df [DataFrame, nil] the query that populates the table (a flow).
    # @return [String] the resolved output identifier.
    def create_table(name, df = nil, comment: nil, format: nil, partition_cols: [],
                     clustering_columns: [], table_properties: {}, schema: nil)
      define_table_output(name, :table, df, comment: comment, format: format,
                                            partition_cols: partition_cols, clustering_columns: clustering_columns,
                                            table_properties: table_properties, schema: schema)
    end

    # Define a streaming table that flows (e.g. {#create_auto_cdc_flow} or
    # {#define_flow}) can target, without a defining query. Mirrors PySpark's
    # `create_streaming_table`.
    # @return [String] the resolved output identifier.
    def create_streaming_table(name, comment: nil, format: nil, partition_cols: [], clustering_columns: [],
                               table_properties: {}, schema: nil)
      define_table_output(name, :table, nil, comment: comment, format: format, partition_cols: partition_cols,
                                             clustering_columns: clustering_columns, table_properties: table_properties,
                                             schema: schema)
    end

    # Define an Auto CDC flow that applies the Change Data Capture events of
    # `source` to the streaming table `target` (Spark 4.2+). Mirrors PySpark's
    # `create_auto_cdc_flow`.
    #
    # @example
    #   pipe.create_streaming_table("customers")
    #   pipe.create_auto_cdc_flow(target: "customers", source: "customers_cdc", keys: ["id"],
    #                             sequence_by: "ts", apply_as_deletes: "op = 'DELETE'", stored_as_scd_type: 2)
    #
    # @param target [String] the target streaming table.
    # @param source [String] the CDC source to stream from.
    # @param keys [Array<String, Column>] the columns identifying a row (unqualified).
    # @param sequence_by [String, Column] the ordering expression (a SQL String or Column).
    # @param apply_as_deletes [String, Column, nil] the condition marking a delete event.
    # @param column_list [Array<String, Column>, nil] columns to keep in the target.
    # @param except_column_list [Array<String, Column>, nil] columns to drop from the target.
    # @param stored_as_scd_type [Integer, String, nil] 1 or 2 (server default when nil).
    # @param name [String, nil] the flow name (defaults to `target`).
    # @param track_history_column_list [Array<String, Column>, nil] SCD2 only: columns whose change opens a new history record.
    # @param track_history_except_column_list [Array<String, Column>, nil] SCD2 only: columns excluded from history tracking.
    # @param sql_conf [Hash{String=>String}] SQL configs applied to this flow.
    # @return [String] the resolved flow name.
    def create_auto_cdc_flow(target:, source:, keys:, sequence_by:, apply_as_deletes: nil, column_list: nil,
                             except_column_list: nil, stored_as_scd_type: nil, name: nil,
                             track_history_column_list: nil, track_history_except_column_list: nil, sql_conf: {})
      %i[target source].zip([target, source]).each do |arg, value|
        raise IllegalArgumentError, "#{arg} must be a String, got #{value.class}" unless value.is_a?(String)
      end
      raise IllegalArgumentError, "name must be a String, got #{name.class}" unless name.nil? || name.is_a?(String)

      key_cols = cdc_columns("keys", keys)
      columns = cdc_columns("column_list", column_list)
      except = cdc_columns("except_column_list", except_column_list)
      track = cdc_columns("track_history_column_list", track_history_column_list)
      track_except = cdc_columns("track_history_except_column_list", track_history_except_column_list)
      reject_together("column_list", columns, "except_column_list", except)
      reject_together("track_history_column_list", track, "track_history_except_column_list", track_except)
      unless stored_as_scd_type.nil? || %w[1 2].include?(stored_as_scd_type.to_s)
        raise IllegalArgumentError, "stored_as_scd_type must be 1 or 2, got #{stored_as_scd_type.inspect}"
      end
      if stored_as_scd_type.to_s != "2" && (track&.any? || track_except&.any?)
        raise IllegalArgumentError,
              "track_history_column_list/track_history_except_column_list require stored_as_scd_type 2"
      end

      details = PC::DefineFlow::AutoCdcFlowDetails.new(
        source: source, keys: key_cols.map(&:to_expr), sequence_by: cdc_expression("sequence_by", sequence_by).to_expr,
        column_list: (columns || []).map(&:to_expr), except_column_list: (except || []).map(&:to_expr),
        track_history_column_list: (track || []).map(&:to_expr),
        track_history_except_column_list: (track_except || []).map(&:to_expr)
      )
      details.apply_as_deletes = cdc_expression("apply_as_deletes", apply_as_deletes).to_expr unless apply_as_deletes.nil?
      details.stored_as_scd_type = :"SCD_TYPE_#{stored_as_scd_type}" unless stored_as_scd_type.nil?
      flow_name = (name || target).to_s
      flow = PC::DefineFlow.new(
        dataflow_graph_id: @graph_id, flow_name: flow_name, target_dataset_name: target,
        sql_conf: stringify(sql_conf), auto_cdc_flow_details: details
      )
      result = dispatch(PC.new(define_flow: flow))
      identifier_string(result.pipeline_command_result&.define_flow_result&.resolved_identifier) || flow_name
    end

    # Define a materialized view and the flow that populates it.
    # @return [String]
    def create_materialized_view(name, df = nil, comment: nil, format: nil, partition_cols: [],
                                 clustering_columns: [], table_properties: {}, schema: nil)
      define_table_output(name, :materialized_view, df, comment: comment, format: format,
                                                        partition_cols: partition_cols, clustering_columns: clustering_columns,
                                                        table_properties: table_properties, schema: schema)
    end

    # Define a (non-published) temporary view and its flow.
    # @return [String]
    def create_temporary_view(name, df = nil, comment: nil)
      define_table_output(name, :temporary_view, df, comment: comment)
    end

    # Define a streaming sink.
    #
    # @param name [String]
    # @param df [DataFrame] the flow feeding the sink.
    # @param format [String, nil]
    # @param options [Hash{String=>String}]
    # @return [String]
    def create_sink(name, df, format: nil, options: {})
      sink = PC::DefineOutput::SinkDetails.new(options: stringify(options))
      sink.format = format if format
      define_output(name, :sink, sink_details: sink)
      define_flow(name, df, target: name)
      name
    end

    # Define a flow that writes the contents of `df` into `target`.
    #
    # @param name [String] the flow name.
    # @param df [DataFrame]
    # @param target [String] the dataset the flow writes to (defaults to `name`).
    # @param once [Boolean] define as a one-time (batch) flow.
    # @param sql_conf [Hash{String=>String}]
    # @return [String] the resolved flow name.
    def define_flow(name, df, target: nil, once: false, sql_conf: {})
      flow = PC::DefineFlow.new(
        dataflow_graph_id: @graph_id, flow_name: name.to_s, target_dataset_name: (target || name).to_s,
        sql_conf: stringify(sql_conf),
        relation_flow_details: PC::DefineFlow::WriteRelationFlowDetails.new(relation: df.relation)
      )
      # `once` is optional: only set it when true, since the server rejects the
      # option being present at all for non-one-time flows (e.g. MV flows).
      flow.once = true if once
      result = dispatch(PC.new(define_flow: flow))
      identifier_string(result.pipeline_command_result&.define_flow_result&.resolved_identifier) || name.to_s
    end

    # Register datasets and flows from a SQL definition file.
    #
    # @param sql_text [String] the SQL source.
    # @param sql_file_path [String, nil]
    # @return [void]
    def define_sql(sql_text, sql_file_path: nil)
      el = PC::DefineSqlGraphElements.new(dataflow_graph_id: @graph_id, sql_text: sql_text.to_s)
      el.sql_file_path = sql_file_path if sql_file_path
      dispatch(PC.new(define_sql_graph_elements: el))
      nil
    end

    # Resolve the graph and run a pipeline update. Blocks until the update
    # completes, returning the events emitted during the run.
    #
    # @param full_refresh [Array<String>] datasets to reset and recompute.
    # @param full_refresh_all [Boolean] reset and recompute everything.
    # @param refresh [Array<String>] datasets to update.
    # @param dry [Boolean] validate the graph without executing flows.
    # @param storage [String, nil] checkpoint/metadata storage location.
    # @yieldparam event [PipelineEvent] each event, as it is collected
    # @return [Array<PipelineEvent>]
    def start_run(full_refresh: [], full_refresh_all: false, refresh: [], dry: false, storage: nil, &block)
      run = PC::StartRun.new(
        dataflow_graph_id: @graph_id,
        full_refresh_selection: Array(full_refresh).map(&:to_s),
        full_refresh_all: full_refresh_all,
        refresh_selection: Array(refresh).map(&:to_s),
        dry: dry
      )
      run.storage = storage if storage
      result = dispatch(PC.new(start_run: run))
      events = result.pipeline_events.map { |e| PipelineEvent.new(e.timestamp, e.message) }
      events.each(&block) if block
      events
    end

    # Drop this dataflow graph and stop any attached flows.
    # @return [void]
    def drop
      dispatch(PC.new(drop_dataflow_graph: PC::DropDataflowGraph.new(dataflow_graph_id: @graph_id)))
      nil
    end

    private

    def define_table_output(name, type, df, comment: nil, format: nil, partition_cols: [],
                            clustering_columns: [], table_properties: {}, schema: nil)
      details = nil
      if type != :temporary_view || format || schema
        details = PC::DefineOutput::TableDetails.new(
          table_properties: stringify(table_properties),
          partition_cols: Array(partition_cols).map(&:to_s),
          clustering_columns: Array(clustering_columns).map(&:to_s)
        )
        details.format = format if format
        apply_schema(details, schema) if schema
      end
      resolved = define_output(name, type, table_details: details, comment: comment)
      define_flow(name, df, target: name) if df
      resolved
    end

    def define_output(name, type, table_details: nil, sink_details: nil, comment: nil)
      output = PC::DefineOutput.new(
        dataflow_graph_id: @graph_id, output_name: name.to_s, output_type: OUTPUT_TYPES.fetch(type)
      )
      output.comment = comment if comment
      output.table_details = table_details if table_details
      output.sink_details = sink_details if sink_details
      result = dispatch(PC.new(define_output: output))
      identifier_string(result.pipeline_command_result&.define_output_result&.resolved_identifier) || name.to_s
    end

    def cdc_columns(arg, cols)
      return nil if cols.nil?
      raise IllegalArgumentError, "#{arg} must be an Array of Strings or Columns, got #{cols.class}" unless cols.is_a?(Array)

      cols.map do |c|
        case c
        when Column then c
        when String then Functions.col(c)
        else raise IllegalArgumentError, "#{arg} must contain only Strings or Columns, got #{c.class}"
        end
      end
    end

    def cdc_expression(arg, value)
      case value
      when Column then value
      when String then Functions.expr(value)
      else raise IllegalArgumentError, "#{arg} must be a SQL String or Column, got #{value.class}"
      end
    end

    # An empty list is the same as an omitted one, so only non-empty lists clash.
    def reject_together(include_arg, include_cols, except_arg, except_cols)
      return unless include_cols&.any? && except_cols&.any?

      raise IllegalArgumentError, "#{include_arg} and #{except_arg} cannot be set together"
    end

    def apply_schema(details, schema)
      if schema.is_a?(Types::DataType)
        details.schema_data_type = schema.to_proto
      else
        details.schema_string = schema.to_s
      end
    end

    def dispatch(pipeline_command)
      @session.client.execute_command(Proto::Command.new(pipeline_command: pipeline_command))
    end

    def identifier_string(resolved)
      return nil unless resolved

      parts = [resolved.catalog_name, *resolved.namespace, resolved.table_name].reject { |p| p.nil? || p.empty? }
      parts.join(".")
    end

    def stringify(hash)
      hash.to_h { |k, v| [k.to_s, v.to_s] }
    end
  end
end
