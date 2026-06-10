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
