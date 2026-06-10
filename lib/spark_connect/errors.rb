# frozen_string_literal: true

module SparkConnect
  # Base class for every error raised by spark-connect. Rescue this to catch
  # any library-specific failure.
  class Error < StandardError; end

  # Raised when a connection string (`sc://...`) or builder configuration is
  # malformed.
  class ConnectionError < Error; end

  # Raised for invalid arguments passed to the public API before any request is
  # sent to the server (mirrors PySpark's analysis-time argument validation).
  class IllegalArgumentError < Error; end

  # Raised when a feature is recognised but not implemented by this client.
  class NotImplementedError < Error; end

  # Wraps an error returned by the Spark Connect server (a gRPC failure carrying
  # a Spark error payload). The original gRPC exception is available via
  # {#cause}, and Spark's error class / SQL state are surfaced when present.
  class SparkConnectError < Error
    # @return [String, nil] Spark's canonical error class, e.g.
    #   `"TABLE_OR_VIEW_NOT_FOUND"`, when the server provided one.
    attr_reader :error_class

    # @return [String, nil] the ANSI SQL state, when present.
    attr_reader :sql_state

    # @return [String, nil] the gRPC status code name, e.g. `"UNAVAILABLE"`.
    attr_reader :grpc_code

    # @return [Array<String>] the server-side stack trace lines, when present.
    attr_reader :stack_trace

    def initialize(message, error_class: nil, sql_state: nil, grpc_code: nil, stack_trace: [])
      super(message)
      @error_class = error_class
      @sql_state = sql_state
      @grpc_code = grpc_code
      @stack_trace = stack_trace
    end
  end

  # Raised when an analysis-time error is reported by the server (e.g. an
  # unresolved column or an invalid plan).
  class AnalysisError < SparkConnectError; end

  # Raised when SQL parsing fails on the server.
  class ParseError < AnalysisError; end

  # Raised on a temporary/transient server or transport condition that the
  # client gave up retrying.
  class RetriesExceededError < SparkConnectError; end

  # Raised when the user (or a signal) interrupts a running operation.
  class OperationInterruptedError < SparkConnectError; end
end
