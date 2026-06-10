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
    current = live_session.version.split(/[.-]/).map(&:to_i)
    wanted = target.split(".").map(&:to_i)
    (current.first(wanted.size) <=> wanted) >= 0
  end

  class << self
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
  config.after(:suite) { IntegrationHelpers.stop }
end
