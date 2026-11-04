# frozen_string_literal: true

require "grpc"
require "uri"

module SparkConnect
  # Parses a Spark Connect connection string (`sc://...`) and builds the gRPC
  # stub, credentials, and per-request metadata.
  #
  # The connection string grammar mirrors the official Spark Connect clients:
  #
  #     sc://host[:port][/;param=value;param=value...]
  #
  # Recognised parameters:
  # * `token`      - bearer token; implies TLS and adds an `authorization` header
  #   (defaults to the `SPARK_CONNECT_AUTHENTICATE_TOKEN` environment variable)
  # * `user_id`    - the Spark user id
  # * `user_agent` - client user agent (default `spark-connect-ruby/<version>`)
  # * `use_ssl`    - `true`/`false`; force TLS on or off
  # * `session_id` - reuse a specific server-side session id (UUID)
  # * `grpc_keepalive_enabled` (default `true`), `grpc_keepalive_time_ms`
  #   (default 60000), `grpc_keepalive_timeout_ms` (default 20000), and
  #   `grpc_keepalive_without_calls` (default `true`) - gRPC/HTTP2 keepalive
  #   PINGs that detect a silently dead connection (Spark 4.3 client defaults)
  #
  # Every other parameter is forwarded verbatim as gRPC request metadata (like
  # the official clients), e.g. `x-api-key=...`.
  #
  # @example
  #   cb = SparkConnect::ChannelBuilder.new("sc://localhost:15002")
  #   cb.host       #=> "localhost"
  #   cb.port       #=> 15002
  class ChannelBuilder
    DEFAULT_PORT = 15_002
    PARAM_PREFIX = "x-"

    PARAM_TOKEN = "token"
    PARAM_USER_ID = "user_id"
    PARAM_USER_AGENT = "user_agent"
    PARAM_USE_SSL = "use_ssl"
    PARAM_SESSION_ID = "session_id"
    PARAM_GRPC_KEEPALIVE_ENABLED = "grpc_keepalive_enabled"
    PARAM_GRPC_KEEPALIVE_TIME_MS = "grpc_keepalive_time_ms"
    PARAM_GRPC_KEEPALIVE_TIMEOUT_MS = "grpc_keepalive_timeout_ms"
    PARAM_GRPC_KEEPALIVE_WITHOUT_CALLS = "grpc_keepalive_without_calls"

    # Parameters consumed by the client itself and never sent as metadata.
    RESERVED_PARAMS = [
      PARAM_TOKEN, PARAM_USER_ID, PARAM_USER_AGENT, PARAM_USE_SSL, PARAM_SESSION_ID,
      PARAM_GRPC_KEEPALIVE_ENABLED, PARAM_GRPC_KEEPALIVE_TIME_MS, PARAM_GRPC_KEEPALIVE_TIMEOUT_MS,
      PARAM_GRPC_KEEPALIVE_WITHOUT_CALLS,
    ].freeze

    GRPC_DEFAULT_KEEPALIVE_ENABLED = true
    GRPC_DEFAULT_KEEPALIVE_TIME_MS = 60 * 1000
    GRPC_DEFAULT_KEEPALIVE_TIMEOUT_MS = 20 * 1000
    GRPC_DEFAULT_KEEPALIVE_WITHOUT_CALLS = true

    # Like the official clients, allow messages (e.g. large Arrow batches) of up
    # to 128 MiB instead of gRPC's 4 MiB receive default.
    GRPC_MAX_MESSAGE_LENGTH_DEFAULT = 128 * 1024 * 1024

    UUID_PATTERN = /\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/

    # @return [String]
    attr_reader :host
    # @return [Integer]
    attr_reader :port
    # @return [Hash{String=>String}] raw connection parameters.
    attr_reader :params
    # @return [String, nil]
    attr_reader :token
    # @return [String, nil]
    attr_reader :user_id
    # @return [String, nil]
    attr_reader :session_id

    # @param url [String] an `sc://` connection string.
    def initialize(url)
      raise ConnectionError, "Connection string must not be nil" if url.nil?
      raise ConnectionError, "Connection string must start with 'sc://', got: #{url.inspect}" unless url.start_with?("sc://")

      body = url.delete_prefix("sc://")
      endpoint, _, param_str = body.partition("/")
      @params = parse_params(param_str)
      parse_endpoint(endpoint)

      @token = @params[PARAM_TOKEN] || ENV.fetch("SPARK_CONNECT_AUTHENTICATE_TOKEN", nil)
      @user_id = @params[PARAM_USER_ID]
      @session_id = @params[PARAM_SESSION_ID]
      validate_session_id!(@session_id)
      @use_ssl = parse_bool(@params[PARAM_USE_SSL]) || !@token.nil?
    end

    # A copy of this builder with `session_id` replaced (or removed when nil).
    # Used to create a fresh ({SparkSession#new_session}) or cloned
    # ({SparkSession#clone_session}) session against the same endpoint.
    #
    # @param session_id [String, nil]
    # @return [ChannelBuilder]
    def with_session_id(session_id)
      copy = dup
      params = @params.dup
      if session_id.nil?
        params.delete(PARAM_SESSION_ID)
      else
        params[PARAM_SESSION_ID] = session_id
      end
      copy.instance_variable_set(:@params, params)
      copy.instance_variable_set(:@session_id, session_id)
      copy
    end

    # @return [Boolean] whether gRPC keepalive PINGs are sent.
    def keepalive_enabled?
      bool_param(PARAM_GRPC_KEEPALIVE_ENABLED, GRPC_DEFAULT_KEEPALIVE_ENABLED)
    end

    # @return [Integer] idle time (ms) before a keepalive PING.
    def keepalive_time_ms
      int_param(PARAM_GRPC_KEEPALIVE_TIME_MS, GRPC_DEFAULT_KEEPALIVE_TIME_MS)
    end

    # @return [Integer] time (ms) to wait for a keepalive PING ack.
    def keepalive_timeout_ms
      int_param(PARAM_GRPC_KEEPALIVE_TIMEOUT_MS, GRPC_DEFAULT_KEEPALIVE_TIMEOUT_MS)
    end

    # @return [Boolean] whether keepalive PINGs are sent without in-flight calls.
    def keepalive_without_calls?
      bool_param(PARAM_GRPC_KEEPALIVE_WITHOUT_CALLS, GRPC_DEFAULT_KEEPALIVE_WITHOUT_CALLS)
    end

    # The gRPC channel arguments derived from the connection string.
    # @return [Hash{String=>Object}]
    def channel_args
      args = {
        "grpc.primary_user_agent" => user_agent,
        "grpc.max_send_message_length" => GRPC_MAX_MESSAGE_LENGTH_DEFAULT,
        "grpc.max_receive_message_length" => GRPC_MAX_MESSAGE_LENGTH_DEFAULT,
      }
      if keepalive_enabled?
        args["grpc.keepalive_time_ms"] = keepalive_time_ms
        args["grpc.keepalive_timeout_ms"] = keepalive_timeout_ms
        args["grpc.keepalive_permit_without_calls"] = keepalive_without_calls? ? 1 : 0
      end
      args
    end

    # @return [Boolean] whether the channel uses TLS.
    def ssl?
      @use_ssl
    end

    # @return [String] the gRPC target, e.g. `"localhost:15002"`.
    def target
      "#{host}:#{port}"
    end

    # @return [String] the effective user agent.
    def user_agent
      @params["user_agent"] || "spark-connect-ruby/#{SparkConnect::VERSION}"
    end

    # Per-request gRPC metadata derived from the connection string: the bearer
    # token plus every parameter the client does not consume itself.
    #
    # @return [Hash{String=>String}]
    def metadata
      md = {}
      md["authorization"] = "Bearer #{@token}" if @token
      @params.each { |k, v| md[k] = v unless RESERVED_PARAMS.include?(k) }
      md
    end

    # Build the gRPC stub for the {Spark::Connect::SparkConnectService}.
    #
    # @param channel_args [Hash] extra gRPC channel arguments (override the
    #   defaults from {#channel_args}).
    # @return [Spark::Connect::SparkConnectService::Stub]
    def build_stub(channel_args: {})
      creds = @use_ssl ? GRPC::Core::ChannelCredentials.new : :this_channel_is_insecure
      Proto::SparkConnectService::Stub.new(target, creds, channel_args: self.channel_args.merge(channel_args))
    end

    private

    Proto = SparkConnect::Proto

    def parse_endpoint(endpoint)
      raise ConnectionError, "Missing host in connection string" if endpoint.nil? || endpoint.empty?

      host, sep, port = endpoint.rpartition(":")
      if sep.empty?
        @host = endpoint
        @port = DEFAULT_PORT
      else
        @host = host
        @port = Integer(port, exception: false) ||
                raise(ConnectionError, "Invalid port in connection string: #{port.inspect}")
      end
    end

    def parse_params(param_str)
      params = {}
      param_str.split(";").each do |kv|
        next if kv.empty?

        key, sep, value = kv.partition("=")
        raise ConnectionError, "Malformed parameter (expected key=value): #{kv.inspect}" if sep.empty?

        params[URI.decode_www_form_component(key)] = URI.decode_www_form_component(value)
      end
      params
    end

    def parse_bool(value)
      return nil if value.nil?

      %w[true 1 yes].include?(value.to_s.downcase)
    end

    def bool_param(key, default)
      value = @params[key]
      value.nil? ? default : parse_bool(value)
    end

    def int_param(key, default)
      value = @params[key]
      return default if value.nil?

      Integer(value, exception: false) || raise(ConnectionError, "Invalid integer for #{key}: #{value.inspect}")
    end

    def validate_session_id!(session_id)
      return if session_id.nil? || UUID_PATTERN.match?(session_id)

      raise ConnectionError, "session_id must be a UUID string, got #{session_id.inspect}"
    end
  end
end
