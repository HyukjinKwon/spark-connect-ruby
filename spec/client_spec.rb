# frozen_string_literal: true

RSpec.describe SparkConnect::SparkConnectClient do
  # A real (insecure) ChannelBuilder is fine; we never make RPCs because we
  # replace the gRPC stub with a double before exercising the client.
  def build_client(**kw)
    cb = SparkConnect::ChannelBuilder.new("sc://localhost:15002")
    described_class.new(cb, **kw)
  end

  def stub_for(client)
    instance_double(Spark::Connect::SparkConnectService::Stub).tap do |s|
      client.instance_variable_set(:@stub, s)
    end
  end

  describe SparkConnect::SparkConnectClient::ExecuteResult do
    it "exposes the accumulated fields" do
      result = described_class.new(["b1"], :schema, :metrics, [:m], :rel, 3)
      expect(result.arrow_batches).to eq(["b1"])
      expect(result.schema).to eq(:schema)
      expect(result.metrics).to eq(:metrics)
      expect(result.observed_metrics).to eq([:m])
      expect(result.sql_command_result).to eq(:rel)
      expect(result.row_count).to eq(3)
    end
  end

  describe "construction" do
    it "derives session_id, client_type and user_context from the channel builder" do
      cb = SparkConnect::ChannelBuilder.new("sc://localhost:15002/;user_id=u1;session_id=sess-9")
      client = described_class.new(cb)
      expect(client.session_id).to eq("sess-9")
      expect(client.client_type).to eq("spark-connect-ruby/#{SparkConnect::VERSION}")
      expect(client.channel_builder).to equal(cb)
    end

    it "generates a UUID session id when none is provided" do
      client = build_client
      expect(client.session_id).to match(/\A[0-9a-f-]{36}\z/)
    end

    it "honours an explicit session_id override" do
      expect(build_client(session_id: "mine").session_id).to eq("mine")
    end
  end

  describe "#analyze" do
    it "sends an AnalyzePlanRequest carrying the oneof keyword" do
      client = build_client
      stub = stub_for(client)
      version = SparkConnect::Proto::AnalyzePlanRequest::SparkVersion.new
      expect(stub).to receive(:analyze_plan) do |req, **|
        expect(req).to be_a(SparkConnect::Proto::AnalyzePlanRequest)
        expect(req.session_id).to eq(client.session_id)
        expect(req.analyze).to eq(:spark_version)
        :ok
      end
      expect(client.analyze(spark_version: version)).to eq(:ok)
    end
  end

  describe "#config" do
    it "wraps the operation in a ConfigRequest" do
      client = build_client
      stub = stub_for(client)
      op = SparkConnect::Proto::ConfigRequest::Operation.new
      expect(stub).to receive(:config) do |req, **|
        expect(req).to be_a(SparkConnect::Proto::ConfigRequest)
        expect(req.session_id).to eq(client.session_id)
        :cfg
      end
      expect(client.config(op)).to eq(:cfg)
    end
  end

  describe "retry / backoff" do
    it "retries a retryable error then succeeds" do
      client = build_client(max_retries: 3, retry_base_delay: 0.0)
      stub = stub_for(client)
      calls = 0
      allow(stub).to receive(:analyze_plan) do
        calls += 1
        raise GRPC::Unavailable, "down" if calls < 3

        :recovered
      end
      result = client.analyze(spark_version: SparkConnect::Proto::AnalyzePlanRequest::SparkVersion.new)
      expect(result).to eq(:recovered)
      expect(calls).to eq(3)
    end

    it "gives up after max_retries and translates the error" do
      client = build_client(max_retries: 2, retry_base_delay: 0.0)
      stub = stub_for(client)
      allow(stub).to receive(:analyze_plan).and_raise(GRPC::Unavailable.new("still down"))
      expect { client.analyze(spark_version: SparkConnect::Proto::AnalyzePlanRequest::SparkVersion.new) }
        .to raise_error(SparkConnect::SparkConnectError)
    end

    it "does not retry a non-retryable error" do
      client = build_client(max_retries: 5, retry_base_delay: 0.0)
      stub = stub_for(client)
      calls = 0
      allow(stub).to receive(:analyze_plan) do
        calls += 1
        raise GRPC::BadStatus.new(GRPC::Core::StatusCodes::INVALID_ARGUMENT, "[PARSE_SYNTAX_ERROR] nope")
      end
      expect { client.analyze(spark_version: SparkConnect::Proto::AnalyzePlanRequest::SparkVersion.new) }
        .to raise_error(SparkConnect::ParseError)
      expect(calls).to eq(1)
    end

    describe "#backoff" do
      it "grows exponentially and is capped at max_retry_delay" do
        client = build_client(retry_base_delay: 1.0, max_retry_delay: 10.0)
        b0 = client.send(:backoff, 0)
        b1 = client.send(:backoff, 1)
        b_big = client.send(:backoff, 20)
        expect(b0).to be >= 1.0
        expect(b0).to be <= 1.5
        expect(b1).to be >= 2.0
        expect(b1).to be <= 3.0
        # capped base (10) plus up to 50% jitter
        expect(b_big).to be <= 15.0
        expect(b_big).to be >= 10.0
      end
    end
  end

  describe "#translate_error" do
    def translate(client, message, code: GRPC::Core::StatusCodes::INTERNAL)
      client.send(:translate_error, GRPC::BadStatus.new(code, message))
    end

    let(:client) { build_client }

    it "maps parse errors to ParseError" do
      err = translate(client, "[PARSE_SYNTAX_ERROR] bad sql")
      expect(err).to be_a(SparkConnect::ParseError)
      expect(err.error_class).to eq("PARSE_SYNTAX_ERROR")
    end

    it "maps analysis errors to AnalysisError" do
      err = translate(client, "[TABLE_OR_VIEW_NOT_FOUND] missing")
      expect(err).to be_a(SparkConnect::AnalysisError)
      expect(err.error_class).to eq("TABLE_OR_VIEW_NOT_FOUND")
    end

    it "falls back to SparkConnectError for unrecognised messages" do
      err = translate(client, "some random failure")
      expect(err).to be_an_instance_of(SparkConnect::SparkConnectError)
      expect(err.error_class).to be_nil
    end

    it "records the gRPC status code name" do
      err = translate(client, "boom", code: GRPC::Core::StatusCodes::UNAVAILABLE)
      expect(err.grpc_code).to eq("UNAVAILABLE")
    end
  end

  describe "#retryable?" do
    let(:client) { build_client }

    it "is true for UNAVAILABLE" do
      expect(client.send(:retryable?, GRPC::Unavailable.new)).to be(true)
    end

    it "is false for INVALID_ARGUMENT" do
      err = GRPC::BadStatus.new(GRPC::Core::StatusCodes::INVALID_ARGUMENT, "x")
      expect(client.send(:retryable?, err)).to be(false)
    end
  end

  describe "#release_session" do
    it "swallows errors from the stub" do
      client = build_client
      stub = stub_for(client)
      allow(stub).to receive(:release_session).and_raise(GRPC::Unavailable.new("gone"))
      expect(client.release_session).to be_nil
    end
  end
end
