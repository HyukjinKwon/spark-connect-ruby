source "https://rubygems.org"

# Specify gem dependencies in spark-connect.gemspec
gemspec

# red-arrow must match the major version of the Apache Arrow C++ / GLib
# libraries installed on the machine. For local development we pin to the
# Homebrew-provided version; CI installs the system Arrow packages and lets
# Bundler resolve a compatible gem from the gemspec's (broader) constraint.
# Override locally with SPARK_CONNECT_RED_ARROW_VERSION if your Arrow differs.
unless ENV["CI"]
  gem "red-arrow", "= #{ENV.fetch('SPARK_CONNECT_RED_ARROW_VERSION', '22.0.0')}"
end

group :development, :test do
  gem "rake", "~> 13.0"
  gem "rspec", "~> 3.13"
  gem "rubocop", ">= 1.75", require: false
  gem "rubocop-rspec", ">= 3.0", require: false
  gem "yard", "~> 0.9.36", require: false
  gem "grpc-tools", "~> 1.60", require: false
  gem "simplecov", "~> 0.22", require: false
end
