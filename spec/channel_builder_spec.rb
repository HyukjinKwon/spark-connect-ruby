# frozen_string_literal: true

RSpec.describe SparkConnect::ChannelBuilder do
  def cb(url)
    described_class.new(url)
  end

  describe "host/port parsing" do
    it "parses host with the default port" do
      b = cb("sc://localhost")
      expect(b.host).to eq("localhost")
      expect(b.port).to eq(15_002)
      expect(b.target).to eq("localhost:15002")
    end

    it "parses an explicit port" do
      b = cb("sc://example.com:1234")
      expect(b.host).to eq("example.com")
      expect(b.port).to eq(1234)
      expect(b.target).to eq("example.com:1234")
    end

    it "applies the default port even when params follow" do
      b = cb("sc://host/;user_id=alice")
      expect(b.host).to eq("host")
      expect(b.port).to eq(15_002)
      expect(b.user_id).to eq("alice")
    end

    it "keeps params with an explicit port" do
      b = cb("sc://host:9999/;user_id=bob")
      expect(b.port).to eq(9999)
      expect(b.user_id).to eq("bob")
    end
  end

  describe "validation errors" do
    it "rejects a nil connection string" do
      expect { described_class.new(nil) }.to raise_error(SparkConnect::ConnectionError, /must not be nil/)
    end

    it "rejects a bad scheme" do
      expect { cb("http://localhost:15002") }
        .to raise_error(SparkConnect::ConnectionError, %r{must start with 'sc://'})
    end

    it "rejects an empty host" do
      expect { cb("sc://") }.to raise_error(SparkConnect::ConnectionError, /Missing host/)
    end

    it "rejects a non-integer port" do
      expect { cb("sc://host:abc") }
        .to raise_error(SparkConnect::ConnectionError, /Invalid port/)
    end

    it "rejects a malformed parameter without =" do
      expect { cb("sc://host/;tokenonly") }
        .to raise_error(SparkConnect::ConnectionError, /Malformed parameter/)
    end
  end

  describe "parameters" do
    it "captures the recognised parameters" do
      b = cb("sc://host/;token=abc;user_id=u1;session_id=sess-1")
      expect(b.token).to eq("abc")
      expect(b.user_id).to eq("u1")
      expect(b.session_id).to eq("sess-1")
      expect(b.params).to include("token" => "abc", "user_id" => "u1", "session_id" => "sess-1")
    end

    it "URL-decodes parameter keys and values" do
      b = cb("sc://host/;user_id=a%20b")
      expect(b.user_id).to eq("a b")
    end

    it "ignores empty segments between semicolons" do
      b = cb("sc://host/;;user_id=u1;")
      expect(b.user_id).to eq("u1")
    end
  end

  describe "TLS" do
    it "is insecure by default" do
      expect(cb("sc://host").ssl?).to be(false)
    end

    it "is forced on by use_ssl=true" do
      expect(cb("sc://host/;use_ssl=true").ssl?).to be(true)
    end

    it "treats 1 and yes as truthy for use_ssl" do
      expect(cb("sc://host/;use_ssl=1").ssl?).to be(true)
      expect(cb("sc://host/;use_ssl=yes").ssl?).to be(true)
    end

    it "is forced off by use_ssl=false" do
      expect(cb("sc://host/;use_ssl=false").ssl?).to be(false)
    end

    it "is implied by a token even without use_ssl" do
      expect(cb("sc://host/;token=abc").ssl?).to be(true)
    end
  end

  describe "user_agent" do
    it "defaults to the gem identifier" do
      expect(cb("sc://host").user_agent).to eq("spark-connect-ruby/#{SparkConnect::VERSION}")
    end

    it "uses an explicit user_agent param" do
      expect(cb("sc://host/;user_agent=my-app").user_agent).to eq("my-app")
    end
  end

  describe "metadata" do
    it "is empty without a token or x-params" do
      expect(cb("sc://host").metadata).to eq({})
    end

    it "adds an Authorization header for the token" do
      expect(cb("sc://host/;token=secret").metadata)
        .to eq("authorization" => "Bearer secret")
    end

    it "passes x-* params through verbatim" do
      md = cb("sc://host/;x-foo=bar;x-baz=qux;user_id=u1").metadata
      expect(md).to eq("x-foo" => "bar", "x-baz" => "qux")
    end

    it "combines token and x-params" do
      md = cb("sc://host/;token=t;x-trace=1").metadata
      expect(md).to eq("authorization" => "Bearer t", "x-trace" => "1")
    end
  end

  describe "#build_stub" do
    it "builds an insecure stub for a non-TLS target" do
      b = cb("sc://localhost:15002")
      stub = b.build_stub
      expect(stub).to be_a(SparkConnect::Proto::SparkConnectService::Stub)
    end
  end
end
