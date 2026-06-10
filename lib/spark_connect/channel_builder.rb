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
  # * `user_id`    - the Spark user id
  # * `user_agent` - client user agent (default `spark-connect-ruby/<version>`)
  # * `use_ssl`    - `true`/`false`; force TLS on or off
  # * `session_id` - reuse a specific server-side session id (UUID)
  #
  # Any parameter whose name begins with `x-` is forwarded verbatim as gRPC
  # request metadata.
  #
  # @example
  #   cb = SparkConnect::ChannelBuilder.new("sc://localhost:15002")
  #   cb.host       #=> "localhost"
  #   cb.port       #=> 15002
  class ChannelBuilder
    DEFAULT_PORT = 15_002
    PARAM_PREFIX = "x-"

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

      @token = @params["token"]
      @user_id = @params["user_id"]
      @session_id = @params["session_id"]
      @use_ssl = parse_bool(@params["use_ssl"]) || !@token.nil?
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

    # Per-request gRPC metadata derived from the connection string (bearer token
    # plus any `x-*` parameters).
    #
    # @return [Hash{String=>String}]
    def metadata
      md = {}
      md["authorization"] = "Bearer #{@token}" if @token
      @params.each { |k, v| md[k] = v if k.start_with?(PARAM_PREFIX) }
      md
    end

    # Build the gRPC stub for the {Spark::Connect::SparkConnectService}.
    #
    # @param channel_args [Hash] extra gRPC channel arguments.
    # @return [Spark::Connect::SparkConnectService::Stub]
    def build_stub(channel_args: {})
      creds = @use_ssl ? GRPC::Core::ChannelCredentials.new : :this_channel_is_insecure
      args = { "grpc.primary_user_agent" => user_agent }.merge(channel_args)
      Proto::SparkConnectService::Stub.new(target, creds, channel_args: args)
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
  end
end
