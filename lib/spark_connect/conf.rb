# frozen_string_literal: true

module SparkConnect
  # Runtime configuration interface, returned by {SparkSession#conf}. Mirrors
  # PySpark's `spark.conf`.
  #
  # @example
  #   spark.conf.set("spark.sql.shuffle.partitions", "8")
  #   spark.conf.get("spark.sql.shuffle.partitions")  #=> "8"
  class RuntimeConfig
    Proto = SparkConnect::Proto
    Op = Proto::ConfigRequest::Operation
    CR = Proto::ConfigRequest

    # @param client [SparkConnectClient]
    def initialize(client)
      @client = client
    end

    # Set a configuration property.
    #
    # @param key [String]
    # @param value [String, Integer, Boolean]
    # @return [void]
    def set(key, value)
      op = Op.new(set: CR::Set.new(pairs: [Proto::KeyValue.new(key: key.to_s, value: value.to_s)]))
      @client.config(op)
      nil
    end

    # Get the value of a configuration property.
    #
    # @param key [String]
    # @param default [String, nil] returned when the key is unset (when given).
    # @return [String, nil]
    def get(key, default = :__unset__)
      op =
        if default == :__unset__
          Op.new(get: CR::Get.new(keys: [key.to_s]))
        else
          Op.new(get_with_default: CR::GetWithDefault.new(
            pairs: [Proto::KeyValue.new(key: key.to_s, value: default)]
          ))
        end
      resp = @client.config(op)
      pair = resp.pairs.first
      pair&.value
    end

    # Unset a configuration property.
    #
    # @param key [String]
    # @return [void]
    def unset(key)
      @client.config(Op.new(unset: CR::Unset.new(keys: [key.to_s])))
      nil
    end

    # All configuration properties (optionally filtered by `prefix`).
    #
    # @param prefix [String, nil]
    # @return [Hash{String=>String}]
    def get_all(prefix = nil)
      ga = CR::GetAll.new
      ga.prefix = prefix if prefix
      resp = @client.config(Op.new(get_all: ga))
      resp.pairs.to_h { |p| [p.key, p.value] }
    end

    # Whether a configuration property is modifiable in the current session.
    #
    # @param key [String]
    # @return [Boolean]
    def modifiable?(key)
      resp = @client.config(Op.new(is_modifiable: CR::IsModifiable.new(keys: [key.to_s])))
      resp.pairs.first&.value == "true"
    end
  end
end
