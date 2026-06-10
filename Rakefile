# frozen_string_literal: true

require "bundler/gem_tasks"

begin
  require "rspec/core/rake_task"
  RSpec::Core::RakeTask.new(:spec) do |t|
    # Unit specs only by default; integration specs require a live server.
    t.exclude_pattern = "spec/integration/**/*_spec.rb" unless ENV["SPARK_REMOTE"]
  end
rescue LoadError
  nil
end

begin
  require "rubocop/rake_task"
  RuboCop::RakeTask.new
rescue LoadError
  nil
end

begin
  require "yard"
  YARD::Rake::YardocTask.new(:yard)
rescue LoadError
  nil
end

desc "Regenerate the protobuf/gRPC stubs from the vendored .proto files"
task :proto do
  sh "bin/generate-protos"
end

task default: %i[spec rubocop]
