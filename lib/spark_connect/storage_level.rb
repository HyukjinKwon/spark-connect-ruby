# frozen_string_literal: true

module SparkConnect
  # How a cached {DataFrame} (or table) is stored: in memory and/or on disk,
  # off-heap, deserialized, and with how many replicas. Mirrors PySpark's
  # `StorageLevel`; use the predefined constants (`StorageLevel::MEMORY_AND_DISK`, ...).
  #
  # @example
  #   df.persist(SparkConnect::StorageLevel::DISK_ONLY)
  #   df.storage_level  #=> StorageLevel(true, false, false, false, 1)
  class StorageLevel
    Proto = SparkConnect::Proto

    # @return [Boolean]
    attr_reader :use_disk, :use_memory, :use_off_heap, :deserialized
    # @return [Integer]
    attr_reader :replication

    def initialize(use_disk, use_memory, use_off_heap, deserialized, replication = 1)
      @use_disk = use_disk
      @use_memory = use_memory
      @use_off_heap = use_off_heap
      @deserialized = deserialized
      @replication = replication
    end

    # @return [Spark::Connect::StorageLevel]
    def to_proto
      Proto::StorageLevel.new(use_disk: use_disk, use_memory: use_memory, use_off_heap: use_off_heap,
                              deserialized: deserialized, replication: replication)
    end

    # @param proto [Spark::Connect::StorageLevel]
    # @return [StorageLevel]
    def self.from_proto(proto)
      new(proto.use_disk, proto.use_memory, proto.use_off_heap, proto.deserialized, proto.replication)
    end

    def ==(other)
      other.is_a?(StorageLevel) && to_a == other.to_a
    end
    alias eql? ==

    def hash = to_a.hash

    # @return [Array] `[use_disk, use_memory, use_off_heap, deserialized, replication]`.
    def to_a = [use_disk, use_memory, use_off_heap, deserialized, replication]

    def to_s
      "StorageLevel(#{to_a.join(', ')})"
    end
    alias inspect to_s

    NONE = new(false, false, false, false)
    DISK_ONLY = new(true, false, false, false)
    DISK_ONLY_2 = new(true, false, false, false, 2)
    DISK_ONLY_3 = new(true, false, false, false, 3)
    MEMORY_ONLY = new(false, true, false, false)
    MEMORY_ONLY_2 = new(false, true, false, false, 2)
    MEMORY_AND_DISK = new(true, true, false, false)
    MEMORY_AND_DISK_2 = new(true, true, false, false, 2)
    OFF_HEAP = new(true, true, true, false, 1)
    MEMORY_AND_DISK_DESER = new(true, true, false, true)
  end
end
