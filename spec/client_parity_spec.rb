# frozen_string_literal: true

# Client/transport features from the Spark 4.1-4.3 clients: GetStatus,
# CloneSession, fresh sessions, per-RPC deadlines, keepalive, metadata, and
# execution progress handlers.
RSpec.describe SparkConnect::SparkConnectClient do
  Proto = SparkConnect::Proto

  def build_client(url = "sc://localhost:15002", **kw)
    described_class.new(SparkConnect::ChannelBuilder.new(url), **kw)
  end

  def stub_for(client)
    instance_double(Spark::Connect::SparkConnectService::Stub).tap { |s| client.instance_variable_set(:@stub, s) }
  end

  describe "#get_operation_statuses" do
    it "sends a GetStatus request for the given operations" do
      client = build_client
      stub = stub_for(client)
      resp = Proto::GetStatusResponse.new(
        operation_statuses: [Proto::GetStatusResponse::OperationStatus.new(operation_id: "op", state: :OPERATION_STATE_RUNNING)]
      )
      expect(stub).to receive(:get_status) do |req, **opts|
        expect(req.session_id).to eq(client.session_id)
        expect(req.operation_status.operation_ids.to_a).to eq(%w[op])
        expect(opts[:deadline]).to be_a(Time)
        resp
      end
      out = client.get_operation_statuses(["op"])
      expect(out.operation_statuses.first.state).to eq(:OPERATION_STATE_RUNNING)
    end

    it "requests every operation of the session when no id is given" do
      client = build_client
      stub = stub_for(client)
      expect(stub).to receive(:get_status) { |req, **| expect(req.operation_status.operation_ids).to be_empty }
                  .and_return(Proto::GetStatusResponse.new)
      client.getOperationStatuses
    end
  end

  describe "#clone_session / #new_session_client" do
    it "clones on the server and binds a client to the new session id" do
      client = build_client
      stub = stub_for(client)
      new_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
      expect(stub).to receive(:clone_session) { |req, **| expect(req.new_session_id).to eq(new_id) }
                  .and_return(Proto::CloneSessionResponse.new(new_session_id: new_id))
      cloned = client.clone_session(new_id)
      expect(cloned.session_id).to eq(new_id)
      expect(cloned.channel_builder.session_id).to eq(new_id)
    end

    it "derives a fresh session id even when the connection string pins one" do
      sid = "11111111-2222-3333-4444-555555555555"
      client = build_client("sc://localhost:15002/;session_id=#{sid}")
      fresh = client.new_session_client
      expect(client.session_id).to eq(sid)
      expect(fresh.session_id).not_to eq(sid)
      expect(fresh.channel_builder.params).not_to have_key("session_id")
    end
  end

  describe "RPC deadlines" do
    it "applies the per-RPC timeout as a deadline and none when disabled" do
      client = build_client(rpc_deadlines: SparkConnect::RpcDeadlines.new(config: 5))
      stub = stub_for(client)
      expect(stub).to receive(:config) do |_req, **opts|
        expect(opts[:deadline]).to be_within(2).of(Time.now + 5)
        Proto::ConfigResponse.new
      end
      client.config(Proto::ConfigRequest::Operation.new(get_all: Proto::ConfigRequest::GetAll.new))
      expect(stub).to receive(:analyze_plan) { |_req, **opts| expect(opts).not_to have_key(:deadline) }
                  .and_return(Proto::AnalyzePlanResponse.new)
      client.analyze(spark_version: Proto::AnalyzePlanRequest::SparkVersion.new)
    end

    it "defaults to the Spark 4.3 deadlines" do
      d = SparkConnect::RpcDeadlines.defaults
      expect([d.analyze_plan, d.config, d.get_status]).to eq([3600, 600, 600])
      expect(SparkConnect::RpcDeadlines.disabled.to_a.compact).to be_empty
    end
  end

  describe "progress handlers" do
    it "notifies handlers on progress updates and once more when done" do
      client = build_client
      stub = stub_for(client)
      calls = []
      client.register_progress_handler(->(**kw) { calls << kw })
      progress = Proto::ExecutePlanResponse::ExecutionProgress.new(
        stages: [Proto::ExecutePlanResponse::ExecutionProgress::StageInfo.new(stage_id: 1, num_tasks: 4,
                                                                              num_completed_tasks: 2, input_bytes_read: 9)],
        num_inflight_tasks: 2
      )
      responses = [Proto::ExecutePlanResponse.new(execution_progress: progress), Proto::ExecutePlanResponse.new]
      allow(stub).to receive(:execute_plan).and_return(responses)
      client.execute_plan(Proto::Relation.new)
      expect(calls.size).to eq(2)
      expect(calls.first[:stages].first.num_bytes_read).to eq(9)
      expect(calls.first[:inflight_tasks]).to eq(2)
      expect(calls.map { |c| c[:done] }).to eq([false, true])
      expect(calls.first[:operation_id]).to match(/\A\h{8}-/)
    end

    it "validates, removes, and clears handlers" do
      client = build_client
      expect { client.register_progress_handler(:nope) }.to raise_error(SparkConnect::IllegalArgumentError)
      h = ->(**_kw) {}
      client.register_progress_handler(h)
      client.remove_progress_handler(h)
      client.clear_progress_handlers
    end
  end

  describe "#close" do
    it "is idempotent and reported by closed?" do
      client = build_client
      expect(client.closed?).to be(false)
      client.close
      client.close
      expect(client.closed?).to be(true)
    end
  end
end

RSpec.describe SparkConnect::ChannelBuilder do
  it "adds Spark's keepalive and 128 MiB message-size channel arguments" do
    args = described_class.new("sc://h").channel_args
    expect(args).to include("grpc.keepalive_time_ms" => 60_000, "grpc.keepalive_timeout_ms" => 20_000,
                            "grpc.keepalive_permit_without_calls" => 1,
                            "grpc.max_receive_message_length" => 128 * 1024 * 1024)
  end

  it "honours the keepalive connection-string parameters" do
    cb = described_class.new("sc://h/;grpc_keepalive_time_ms=1000;grpc_keepalive_without_calls=false")
    expect(cb.channel_args).to include("grpc.keepalive_time_ms" => 1000, "grpc.keepalive_permit_without_calls" => 0)
    expect(described_class.new("sc://h/;grpc_keepalive_enabled=false").channel_args).not_to have_key("grpc.keepalive_time_ms")
    expect { described_class.new("sc://h/;grpc_keepalive_time_ms=x").channel_args }.to raise_error(SparkConnect::ConnectionError)
  end

  it "forwards every non-reserved parameter as metadata" do
    md = described_class.new("sc://h/;token=t;user_id=u;x-a=1;custom=2;grpc_keepalive_enabled=true").metadata
    expect(md).to eq("authorization" => "Bearer t", "x-a" => "1", "custom" => "2")
  end

  it "reads the token from SPARK_CONNECT_AUTHENTICATE_TOKEN" do
    original = ENV.fetch("SPARK_CONNECT_AUTHENTICATE_TOKEN", nil)
    ENV["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = "env-token"
    cb = described_class.new("sc://h")
    expect(cb.token).to eq("env-token")
    expect(cb.ssl?).to be(true)
  ensure
    ENV["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = original
  end

  it "validates session_id as a UUID" do
    expect { described_class.new("sc://h/;session_id=nope") }.to raise_error(SparkConnect::ConnectionError)
  end

  it "with_session_id copies the builder with a replaced or removed session id" do
    sid = "11111111-2222-3333-4444-555555555555"
    cb = described_class.new("sc://h:1/;session_id=#{sid};x-a=1")
    other = cb.with_session_id(nil)
    expect(other.session_id).to be_nil
    expect(other.params).to eq("x-a" => "1")
    expect(cb.session_id).to eq(sid)
  end
end
