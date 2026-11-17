# frozen_string_literal: true

# Shared support for the integration suite. Integration examples run against a
# real Spark Connect server and are only enabled when SPARK_REMOTE is set (each
# example group is guarded with `if: ENV.fetch("SPARK_REMOTE", nil)`).
#
# A single live session is lazily created on first use and reused across the
# whole run, then released in an `after(:suite)` hook.
module IntegrationHelpers
  # The shared live {SparkConnect::SparkSession}, created on first access.
  # @return [SparkConnect::SparkSession]
  def live_session
    IntegrationHelpers.session
  end

  # Whether the live server's Spark version is at least +target+ (e.g. "4.1").
  # Used to skip examples for features only available on newer servers.
  # @param target [String] a dotted version such as "4.1" or "4.1.0"
  # @return [Boolean]
  def server_spark_version_at_least?(target)
    IntegrationHelpers.spark_version_at_least?(target)
  end

  ML_MEMORY_CONTROL_SKIP = "Spark 4.1 refuses this fit while its ML cache memory control is on (the default)"

  class << self
    # {#server_spark_version_at_least?} for groups that don't include this
    # module, such as the hooks in `RSpec.configure` below.
    def spark_version_at_least?(target)
      current = session.version.split(/[.-]/).map(&:to_i)
      wanted = target.split(".").map(&:to_i)
      (current.first(wanted.size) <=> wanted) >= 0
    end

    # Spark 4.1 refuses to fit FPGrowth, and LDA with the em optimizer, while its
    # ML cache memory control is on, which is the default
    # (spark.connect.session.connectML.mlCache.memoryControl.enabled). Spark 4.0
    # predates the memory control, and Spark 4.2 lifted the restriction.
    def ml_memory_control_rejects_fits?
      spark_version_at_least?("4.1") && !spark_version_at_least?("4.2")
    end

    def session
      @session ||= SparkConnect::SparkSession.builder.remote(ENV.fetch("SPARK_REMOTE")).create
    end

    def stop
      @session&.stop
      @session = nil
    end
  end
end

RSpec.configure do |config|
  config.include IntegrationHelpers, :integration

  # Spark ML over Spark Connect needs a Spark 4.0 server, so the ML specs are
  # skipped on older ones. Skipping at the group level also skips the groups'
  # `before(:all)` setup, which already trains models.
  config.define_derived_metadata(file_path: %r{/spec/integration/ml_[^/]*_spec\.rb\z}) do |metadata|
    metadata[:ml] = true
  end
  config.before(:context, :ml) do
    skip "Spark ML over Spark Connect needs a Spark 4.0+ server" unless IntegrationHelpers.spark_version_at_least?("4.0")
  end

  config.after(:suite) { IntegrationHelpers.stop }
end
