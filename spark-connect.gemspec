# frozen_string_literal: true

require_relative "lib/spark_connect/version"

Gem::Specification.new do |spec|
  spec.name          = "spark-connect"
  spec.version       = SparkConnect::VERSION
  spec.authors       = ["Hyukjin Kwon"]
  spec.email         = ["gurwls223@apache.org"]

  spec.summary       = "A pure-Ruby client for Apache Spark Connect."
  spec.description   = <<~DESC
    spark-connect is a Ruby client for Apache Spark Connect, the gRPC-based
    decoupled client-server protocol for Apache Spark. It provides a DataFrame
    API closely modeled on PySpark, including SQL, relational operators,
    column expressions, a comprehensive functions library, typed schemas, and
    Apache Arrow-based result decoding.
  DESC
  spec.homepage      = "https://github.com/HyukjinKwon/spark-connect-ruby"
  spec.license       = "Apache-2.0"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata = {
    "homepage_uri" => spec.homepage,
    "source_code_uri" => spec.homepage,
    "documentation_uri" => "https://hyukjinkwon.github.io/spark-connect-ruby/",
    "bug_tracker_uri" => "#{spec.homepage}/issues",
    "changelog_uri" => "#{spec.homepage}/blob/main/CHANGELOG.md",
    "rubygems_mfa_required" => "true",
  }

  spec.files = Dir[
    "lib/**/*.rb",
    "proto/**/*.proto",
    "README.md",
    "CHANGELOG.md",
    "LICENSE",
    "NOTICE"
  ]
  spec.require_paths = ["lib"]

  spec.add_dependency "google-protobuf", ">= 3.25", "< 5.0"
  spec.add_dependency "grpc", "~> 1.60"
  spec.add_dependency "red-arrow", ">= 15.0"
end
