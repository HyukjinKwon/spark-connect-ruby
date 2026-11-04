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

    SOURCE_NAME_PATTERN = /\A[a-zA-Z0-9_]+\z/

    # @param session [SparkSession]
    def initialize(session)
      @session = session
      @format = nil
      @schema = nil
      @options = {}
      @source_name = nil
    end

    # @return [self] set the streaming source format (`"rate"`, `"kafka"`, ...).
    def format(source)
      @format = source.to_s
      self
    end

    # @return [self] set the input schema (a {Types::StructType} or DDL string).
    def schema(schema)
      # Like PySpark, a StructType is sent as its JSON form.
      @schema = schema.is_a?(Types::StructType) ? schema.json : schema.to_s
      self
    end

    # @return [self] set a single source option (a `nil` value is ignored).
    def option(key, value)
      @options[key.to_s] = value.to_s unless value.nil?
      self
    end

    # @return [self] set multiple source options.
    def options(opts)
      opts.each { |k, v| option(k, v) }
      self
    end

    # Name this streaming source (Spark 4.2+). The name identifies the source
    # in checkpoint metadata, so sources can be added, removed, or reordered
    # without breaking checkpoint compatibility.
    #
    # @param source_name [String] letters, digits, and underscores only.
    # @return [self]
    def name(source_name)
      raise IllegalArgumentError, "source name must be a String, got #{source_name.class}" unless source_name.is_a?(String)
      unless SOURCE_NAME_PATTERN.match?(source_name)
        raise IllegalArgumentError,
              "Invalid streaming source name #{source_name.inspect}: only ASCII letters, digits, and underscores are allowed"
      end

      @source_name = source_name
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
      ds.source_name = @source_name if @source_name
      stream_relation(data_source: ds)
    end

    # The row-level changes (Change Data Capture) of a table as a streaming
    # DataFrame (Spark 4.2+). Configure the start with options such as
    # `startingVersion` / `startingTimestamp`.
    #
    # @param table_name [String]
    # @return [DataFrame]
    def changes(table_name)
      raise AnalysisError.new("User specified schema not supported with `changes`.", error_class: "_LEGACY_ERROR_TEMP_1189") if @schema

      rc = Proto::RelationChanges.new(unparsed_identifier: table_name.to_s, options: @options, is_streaming: true)
      DataFrame.new(@session, PlanBuilder.relation(@session, relation_changes: rc))
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
    # @return [DataFrame] a streaming XML source (set `option("rowTag", ...)`).
    def xml(path) = format("xml").load(path)

    private

    def stream_relation(**read_kw)
      read = Proto::Read.new(is_streaming: true, **read_kw)
      DataFrame.new(@session, PlanBuilder.relation(@session, read: read))
    end
  end

  # Writes a streaming {DataFrame} to a streaming sink and starts the query.
  # Returned by {DataFrame#write_stream}. Mirrors PySpark's `DataStreamWriter`.
  #
  # `foreach`/`foreach_batch` are intentionally unsupported: they run a
  # user-defined function on the server, which this client cannot ship.
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
      @clustering = []
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

    # @return [self] set a single sink option (a `nil` value is ignored).
    def option(key, value)
      @options[key.to_s] = value.to_s unless value.nil?
      self
    end

    # @return [self] set multiple sink options.
    def options(opts)
      opts.each { |k, v| option(k, v) }
      self
    end

    # @return [self] partition the output by these columns.
    def partition_by(*cols)
      @partitioning = cols.flatten.map(&:to_s)
      self
    end
    alias partitionBy partition_by

    # @return [self] cluster the output table by these columns.
    def cluster_by(*cols)
      @clustering = cols.flatten.map(&:to_s)
      self
    end
    alias clusterBy cluster_by

    # Not supported: `foreach` runs a user-defined function on the server.
    # @raise [NotImplementedError]
    def foreach(*_args)
      raise NotImplementedError, "foreach sinks require user-defined functions, which the Ruby client does not support"
    end

    # Not supported: `foreach_batch` runs a user-defined function on the server.
    # @raise [NotImplementedError]
    def foreach_batch(*_args)
      raise NotImplementedError, "foreachBatch sinks require user-defined functions, which the Ruby client does not support"
    end
    alias foreachBatch foreach_batch

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
    # @param real_time [String, nil] real-time mode with the given batch
    #   duration, e.g. `"5 minutes"` (Spark 4.2+; supported sources/sinks only).
    # @return [self]
    def trigger(processing_time: nil, once: nil, available_now: nil, continuous: nil, real_time: nil)
      given = { processing_time: processing_time, once: once, available_now: available_now,
                continuous: continuous, real_time: real_time, }.compact
      raise IllegalArgumentError, "trigger requires exactly one of #{given.keys.inspect}" if given.size > 1
      raise IllegalArgumentError, "trigger requires one of processing_time, once, available_now, continuous, real_time" if given.empty?

      key, value = given.first
      @trigger =
        case key
        when :processing_time then [:processing_time_interval, non_empty!(key, value)]
        when :once then [:once, true!(key, value)]
        when :available_now then [:available_now, true!(key, value)]
        when :continuous then [:continuous_checkpoint_interval, non_empty!(key, value)]
        when :real_time then [:real_time_batch_duration, non_empty!(key, value)]
        end
      self
    end

    # Start the streaming query to a file/data path. The keyword arguments are
    # shortcuts for {#format}, {#output_mode}, {#partition_by}, {#query_name},
    # and {#options}.
    #
    # @param path [String, nil]
    # @return [StreamingQuery]
    def start(path = nil, format: nil, output_mode: nil, partition_by: nil, query_name: nil, **options)
      apply_start_options(format, output_mode, partition_by, query_name, options)
      @path = path if path
      run
    end

    # Start the streaming query, writing into the given table.
    #
    # @param name [String]
    # @return [StreamingQuery]
    def to_table(name, format: nil, output_mode: nil, partition_by: nil, query_name: nil, **options)
      apply_start_options(format, output_mode, partition_by, query_name, options)
      @table = name.to_s
      run
    end
    alias toTable to_table

    private

    def apply_start_options(format, output_mode, partition_by, query_name, options)
      self.format(format) if format
      self.output_mode(output_mode) if output_mode
      self.partition_by(partition_by) if partition_by
      self.query_name(query_name) if query_name
      self.options(options) unless options.empty?
    end

    def non_empty!(key, value)
      raise IllegalArgumentError, "trigger #{key} must be a non-empty String" unless value.is_a?(String) && !value.strip.empty?

      value.strip
    end

    def true!(key, value)
      raise IllegalArgumentError, "trigger #{key} must be true" unless value == true

      true
    end

    def run
      op = WSO.new(
        input: @df.relation, format: @format || "", options: @options,
        partitioning_column_names: @partitioning, clustering_column_names: @clustering
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

      if wsr.query_started_event_json && !wsr.query_started_event_json.empty?
        @df.session.streams.post_event(StreamingQueryListener.event_from_json(:started, wsr.query_started_event_json))
      end
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
    alias runId run_id
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
    alias is_active active?
    alias isActive active?

    # @return [Array<Hash>] parsed JSON progress objects for recent micro-batches.
    def recent_progress
      command(recent_progress: true).recent_progress.recent_progress_json.map { |j| JSON.parse(j) }
    end
    alias recentProgress recent_progress

    # @return [Hash, nil] the most recent progress object, if any.
    def last_progress
      command(last_progress: true).recent_progress.recent_progress_json.map { |j| JSON.parse(j) }.last
    end
    alias lastProgress last_progress

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
    alias processAllAvailable process_all_available

    # Stop the query. @return [void]
    def stop
      command(stop: true)
      nil
    end

    # @return [String, nil] the query's exception message, if it has failed.
    def exception
      result = command(exception: true).exception
      # The server omits the result (or its message) when the query has not failed.
      return nil if result.nil? || !result.has_exception_message? || result.exception_message.empty?

      result.exception_message
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
      @listener_bus = StreamingQueryListenerBus.new(session)
    end

    # Register a {StreamingQueryListener} (or any object responding to some of
    # `on_query_started`, `on_query_progress`, `on_query_idle`,
    # `on_query_terminated`, or their camelCase PySpark names). Events are
    # delivered on a background thread, like PySpark's client-side listener bus.
    #
    # @param listener [Object]
    # @return [void]
    def add_listener(listener)
      @listener_bus.add(listener)
      nil
    end
    alias addListener add_listener

    # Unregister a listener added with {#add_listener}. @return [void]
    def remove_listener(listener)
      @listener_bus.remove(listener)
      nil
    end
    alias removeListener remove_listener

    # @return [Array<Object>] the registered listeners.
    def listeners
      @listener_bus.listeners
    end

    # Remove all listeners and stop the listener thread. @return [void]
    def close
      @listener_bus.close
      nil
    end

    # @api private - deliver a client-side event (e.g. QueryStartedEvent).
    def post_event(event)
      @listener_bus.post(event)
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

  # Base class for streaming query listeners registered with
  # {StreamingQueryManager#add_listener}. Override the callbacks you need; the
  # camelCase PySpark names (`onQueryStarted`, ...) are honoured too.
  class StreamingQueryListener
    # @param event [QueryStartedEvent]
    def on_query_started(event); end

    # @param event [QueryProgressEvent]
    def on_query_progress(event); end

    # @param event [QueryIdleEvent]
    def on_query_idle(event); end

    # @param event [QueryTerminatedEvent]
    def on_query_terminated(event); end

    # A query started. `job_tags` is an Array of Strings.
    QueryStartedEvent = Struct.new(:id, :run_id, :name, :timestamp, :job_tags, keyword_init: true)
    # A micro-batch made progress. `progress` is the parsed progress Hash.
    QueryProgressEvent = Struct.new(:progress, keyword_init: true)
    # A query is idle, waiting for new data.
    QueryIdleEvent = Struct.new(:id, :run_id, :timestamp, keyword_init: true)
    # A query terminated, possibly with an exception message and error class.
    QueryTerminatedEvent = Struct.new(:id, :run_id, :exception, :error_class_on_exception, keyword_init: true)

    EVENT_TYPES = {
      QUERY_PROGRESS_EVENT: :progress, QUERY_TERMINATED_EVENT: :terminated, QUERY_IDLE_EVENT: :idle,
    }.freeze

    CALLBACKS = {
      QueryStartedEvent => %i[on_query_started onQueryStarted],
      QueryProgressEvent => %i[on_query_progress onQueryProgress],
      QueryIdleEvent => %i[on_query_idle onQueryIdle],
      QueryTerminatedEvent => %i[on_query_terminated onQueryTerminated],
    }.freeze

    class << self
      # @api private - build an event from its server-side JSON.
      def event_from_json(kind, json)
        j = JSON.parse(json)
        case kind
        when :started
          QueryStartedEvent.new(id: j["id"], run_id: j["runId"], name: j["name"], timestamp: j["timestamp"],
                                job_tags: Array(j["jobTags"]))
        when :progress then QueryProgressEvent.new(progress: j["progress"])
        when :idle then QueryIdleEvent.new(id: j["id"], run_id: j["runId"], timestamp: j["timestamp"])
        when :terminated
          QueryTerminatedEvent.new(id: j["id"], run_id: j["runId"], exception: j["exception"],
                                   error_class_on_exception: j["errorClassOnException"])
        end
      end

      # @api private - invoke the listener callback for `event`, if defined.
      # A camelCase callback (PySpark's name) wins over the inherited no-op
      # snake_case one of this base class.
      def dispatch(listener, event)
        snake, camel = CALLBACKS.fetch(event.class)
        if listener.respond_to?(camel) &&
           (!listener.respond_to?(snake) || listener.method(snake).owner == StreamingQueryListener)
          listener.public_send(camel, event)
        elsif listener.respond_to?(snake)
          listener.public_send(snake, event)
        end
      end
    end
  end

  # The client-side listener bus behind {StreamingQueryManager#add_listener}:
  # registers one server-side listener for the session and dispatches the
  # events it streams back to every registered listener on a background thread.
  # @api private
  class StreamingQueryListenerBus
    Proto = SparkConnect::Proto

    def initialize(session)
      @session = session
      @listeners = []
      @lock = Mutex.new
      @thread = nil
    end

    def listeners
      @lock.synchronize { @listeners.dup }
    end

    def add(listener)
      @lock.synchronize do
        return if @listeners.include?(listener)

        @listeners << listener
        start_server_listener if @listeners.size == 1
      end
    end

    def remove(listener)
      thread = nil
      @lock.synchronize do
        return unless @listeners.include?(listener)

        if @listeners.size == 1
          bus_command(remove_listener_bus_listener: true)
          thread = @thread
          @thread = nil
        end
        @listeners.delete(listener)
      end
      # Join outside the lock: the handler thread takes it to dispatch events.
      thread&.join(30)
    end

    def close
      listeners.each { |l| remove(l) }
    end

    def post(event)
      listeners.each do |listener|
        StreamingQueryListener.dispatch(listener, event)
      rescue StandardError => e
        warn("spark-connect: streaming listener #{listener.class} raised #{e.class}: #{e.message}")
      end
    end

    private

    def bus_command(**kw)
      cmd = Proto::Command.new(streaming_query_listener_bus_command: Proto::StreamingQueryListenerBusCommand.new(**kw))
      @session.client.execute_command(cmd)
    end

    # Must be called with @lock held.
    def start_server_listener
      ready = Queue.new
      cmd = Proto::Command.new(
        streaming_query_listener_bus_command: Proto::StreamingQueryListenerBusCommand.new(add_listener_bus_listener: true)
      )
      @thread = Thread.new do
        @session.client.stream_command(cmd) do |resp|
          result = resp.streaming_query_listener_events_result
          next unless result

          ready << :added if result.listener_bus_listener_added
          result.events.each do |e|
            kind = StreamingQueryListener::EVENT_TYPES[e.event_type]
            post(StreamingQueryListener.event_from_json(kind, e.event_json)) if kind
          end
        end
      rescue StandardError => e
        ready << e
        warn("spark-connect: streaming listener bus stopped: #{e.class}: #{e.message}; re-add the listeners")
        @lock.synchronize { @listeners.clear }
      ensure
        ready << :finished
      end
      status = ready.pop
      return if status == :added

      @thread = nil
      @listeners.clear
      raise status if status.is_a?(Exception)

      raise SparkConnectError, "The server did not register the streaming query listener"
    end
  end
end
