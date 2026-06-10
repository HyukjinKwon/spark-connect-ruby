# frozen_string_literal: true

require "json"

module SparkConnect
  # Loads a streaming {DataFrame} from a streaming source. Returned by
  # {SparkSession#read_stream}. Mirrors PySpark's `DataStreamReader`.
  #
  # @example
  #   df = spark.read_stream.format("rate").option("rowsPerSecond", 5).load
  class DataStreamReader
    Proto = SparkConnect::Proto

    # @param session [SparkSession]
    def initialize(session)
      @session = session
      @format = nil
      @schema = nil
      @options = {}
    end

    # @return [self] set the streaming source format (`"rate"`, `"kafka"`, ...).
    def format(source)
      @format = source.to_s
      self
    end

    # @return [self] set the input schema (a {Types::StructType} or DDL string).
    def schema(schema)
      @schema = schema.is_a?(Types::StructType) ? schema.simple_string : schema.to_s
      self
    end

    # @return [self] set a single source option.
    def option(key, value)
      @options[key.to_s] = value.to_s
      self
    end

    # @return [self] set multiple source options.
    def options(opts)
      opts.each { |k, v| @options[k.to_s] = v.to_s }
      self
    end

    # Load a streaming DataFrame from the configured source.
    #
    # @param path [String, nil]
    # @return [DataFrame]
    def load(path = nil)
      ds = Proto::Read::DataSource.new(options: @options, paths: path ? [path.to_s] : [])
      ds.format = @format if @format
      ds.schema = @schema if @schema
      stream_relation(data_source: ds)
    end

    # Load a streaming DataFrame from a registered table.
    # @return [DataFrame]
    def table(name)
      stream_relation(named_table: Proto::Read::NamedTable.new(unparsed_identifier: name.to_s, options: @options))
    end

    # @return [DataFrame] convenience for `format(...).load(path)`.
    def csv(path) = format("csv").load(path)
    def json(path) = format("json").load(path)
    def parquet(path) = format("parquet").load(path)
    def orc(path) = format("orc").load(path)
    def text(path) = format("text").load(path)

    private

    def stream_relation(**read_kw)
      read = Proto::Read.new(is_streaming: true, **read_kw)
      DataFrame.new(@session, PlanBuilder.relation(@session, read: read))
    end
  end

  # Writes a streaming {DataFrame} to a streaming sink and starts the query.
  # Returned by {DataFrame#write_stream}. Mirrors PySpark's `DataStreamWriter`.
  #
  # `foreach`/`foreach_batch` are intentionally unsupported: they require
  # user-defined functions, whose Spark Connect protobuf definitions are not yet
  # finalized.
  #
  # @example
  #   query = df.write_stream
  #             .format("console")
  #             .output_mode("append")
  #             .trigger(processing_time: "1 second")
  #             .start
  #   query.stop
  class DataStreamWriter
    Proto = SparkConnect::Proto
    WSO = Proto::WriteStreamOperationStart

    # @param df [DataFrame]
    def initialize(df)
      @df = df
      @format = nil
      @options = {}
      @partitioning = []
      @output_mode = nil
      @query_name = nil
      @trigger = nil
      @path = nil
      @table = nil
    end

    # @return [self] set the sink format (`"console"`, `"memory"`, `"kafka"`, ...).
    def format(source)
      @format = source.to_s
      self
    end

    # @return [self] set the output mode (`"append"`, `"complete"`, `"update"`).
    def output_mode(mode)
      @output_mode = mode.to_s
      self
    end

    # @return [self] set a single sink option.
    def option(key, value)
      @options[key.to_s] = value.to_s
      self
    end

    # @return [self] set multiple sink options.
    def options(opts)
      opts.each { |k, v| @options[k.to_s] = v.to_s }
      self
    end

    # @return [self] partition the output by these columns.
    def partition_by(*cols)
      @partitioning = cols.flatten.map(&:to_s)
      self
    end

    # @return [self] name the streaming query (required by the memory sink).
    def query_name(name)
      @query_name = name.to_s
      self
    end

    # Configure the query trigger. Provide exactly one of:
    #
    # @param processing_time [String, nil] e.g. `"10 seconds"` (micro-batch interval).
    # @param once [Boolean, nil] process all available data once and stop.
    # @param available_now [Boolean, nil] process all available data in (possibly) multiple batches, then stop.
    # @param continuous [String, nil] continuous-processing checkpoint interval.
    # @return [self]
    def trigger(processing_time: nil, once: nil, available_now: nil, continuous: nil)
      @trigger =
        if processing_time then [:processing_time_interval, processing_time.to_s]
        elsif once then [:once, true]
        elsif available_now then [:available_now, true]
        elsif continuous then [:continuous_checkpoint_interval, continuous.to_s]
        end
      self
    end

    # Start the streaming query to a file/data path.
    #
    # @param path [String, nil]
    # @return [StreamingQuery]
    def start(path = nil)
      @path = path if path
      run
    end

    # Start the streaming query, writing into the given table.
    #
    # @param name [String]
    # @return [StreamingQuery]
    def to_table(name)
      @table = name.to_s
      run
    end
    alias toTable to_table

    private

    def run
      op = WSO.new(
        input: @df.relation, format: @format || "", options: @options,
        partitioning_column_names: @partitioning
      )
      op.output_mode = @output_mode if @output_mode
      op.query_name = @query_name if @query_name
      op.public_send("#{@trigger[0]}=", @trigger[1]) if @trigger
      if @path then op.path = @path
      elsif @table then op.table_name = @table
      end
      result = @df.session.client.execute_command(Proto::Command.new(write_stream_operation_start: op))
      wsr = result.write_stream_result
      raise SparkConnectError, "Server did not return a streaming query handle" unless wsr

      StreamingQuery.new(@df.session, wsr.query_id, wsr.name)
    end
  end

  # A handle to a running streaming query. Returned by {DataStreamWriter#start}.
  # Mirrors PySpark's `StreamingQuery`.
  class StreamingQuery
    Proto = SparkConnect::Proto
    Cmd = Proto::StreamingQueryCommand

    # @return [String] the stable query id (survives restarts from a checkpoint).
    attr_reader :id
    # @return [String] the run id (unique per start).
    attr_reader :run_id
    # @return [String, nil] the query name, if one was set.
    attr_reader :name

    # @param session [SparkSession]
    # @param instance_id [Spark::Connect::StreamingQueryInstanceId]
    # @param name [String]
    def initialize(session, instance_id, name)
      @session = session
      @instance_id = instance_id
      @id = instance_id.id
      @run_id = instance_id.run_id
      @name = name.nil? || name.empty? ? nil : name
    end

    # @return [Hash] the current status (`message`, `is_data_available`,
    #   `is_trigger_active`, `is_active`).
    def status
      s = command(status: true).status
      {
        "message" => s.status_message,
        "isDataAvailable" => s.is_data_available,
        "isTriggerActive" => s.is_trigger_active,
        "isActive" => s.is_active,
      }
    end

    # @return [Boolean] whether the query is still running.
    def active?
      status["isActive"]
    end

    # @return [Array<Hash>] parsed JSON progress objects for recent micro-batches.
    def recent_progress
      command(recent_progress: true).recent_progress.recent_progress_json.map { |j| JSON.parse(j) }
    end

    # @return [Hash, nil] the most recent progress object, if any.
    def last_progress
      command(last_progress: true).recent_progress.recent_progress_json.map { |j| JSON.parse(j) }.last
    end

    # Block until the query terminates, or until `timeout_ms` elapses.
    #
    # @param timeout_ms [Integer, nil]
    # @return [Boolean] whether the query has terminated.
    def await_termination(timeout_ms = nil)
      ac = Cmd::AwaitTerminationCommand.new
      ac.timeout_ms = timeout_ms if timeout_ms
      command(await_termination: ac).await_termination.terminated
    end

    # Process all available data, then return (useful for tests with bounded sources).
    # @return [void]
    def process_all_available
      command(process_all_available: true)
      nil
    end

    # Stop the query. @return [void]
    def stop
      command(stop: true)
      nil
    end

    # @return [String, nil] the query's exception message, if it has failed.
    def exception
      result = command(exception: true).exception
      result.exception_message && result.exception_message.empty? ? nil : result.exception_message
    end

    # @return [String] the query's execution plan.
    def explain(extended: false)
      command(explain: Cmd::ExplainCommand.new(extended: extended)).explain.result
    end

    def to_s
      "#<SparkConnect::StreamingQuery id=#{@id} name=#{@name.inspect}>"
    end
    alias inspect to_s

    private

    def command(**kw)
      cmd = Cmd.new(query_id: @instance_id, **kw)
      @session.client.execute_command(Proto::Command.new(streaming_query_command: cmd)).streaming_query_result
    end
  end

  # Manages the streaming queries of a session. Returned by {SparkSession#streams}.
  # Mirrors PySpark's `StreamingQueryManager`.
  class StreamingQueryManager
    Proto = SparkConnect::Proto
    MCmd = Proto::StreamingQueryManagerCommand

    # @param session [SparkSession]
    def initialize(session)
      @session = session
    end

    # @return [Array<StreamingQuery>] the currently active queries.
    def active
      command(active: true).active.active_queries.map { |q| StreamingQuery.new(@session, q.id, q.name) }
    end

    # Look up an active query by its id.
    #
    # @param id [String]
    # @return [StreamingQuery, nil]
    def get(id)
      result = command(get_query: id.to_s)
      return nil unless result.result_type == :query

      StreamingQuery.new(@session, result.query.id, result.query.name)
    end

    # Block until any query terminates, or until `timeout_ms` elapses.
    #
    # @param timeout_ms [Integer, nil]
    # @return [Boolean]
    def await_any_termination(timeout_ms = nil)
      ac = MCmd::AwaitAnyTerminationCommand.new
      ac.timeout_ms = timeout_ms if timeout_ms
      command(await_any_termination: ac).await_any_termination.terminated
    end

    # Forget the cached termination state of all queries (so a subsequent
    # {#await_any_termination} blocks again). @return [void]
    def reset_terminated
      command(reset_terminated: true)
      nil
    end

    private

    def command(**kw)
      cmd = MCmd.new(**kw)
      @session.client.execute_command(Proto::Command.new(streaming_query_manager_command: cmd)).streaming_manager_result
    end
  end
end
