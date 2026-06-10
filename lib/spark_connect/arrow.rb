# frozen_string_literal: true

require "arrow"

module SparkConnect
  # Decodes the Apache Arrow IPC stream payloads returned by the server into
  # Ruby {Row}s. Each `ExecutePlanResponse.arrow_batch.data` chunk is a complete,
  # self-contained Arrow IPC stream (schema + record batches); this converter
  # reads each chunk and flattens all record batches into rows.
  #
  # Named `ArrowConverter` (not `Arrow`) so that references to the `red-arrow`
  # top-level {Arrow} constant inside the gem are unambiguous.
  module ArrowConverter
    module_function

    # Decode IPC stream chunks into rows.
    #
    # @param batches [Array<String>] Arrow IPC stream byte chunks.
    # @return [Array<Row>]
    def to_rows(batches)
      rows = []
      field_names = nil
      batches.each do |data|
        next if data.nil? || data.empty?

        reader = ::Arrow::RecordBatchStreamReader.new(::Arrow::BufferInputStream.new(::Arrow::Buffer.new(data)))
        reader.each do |record_batch|
          field_names ||= record_batch.schema.fields.map(&:name)
          record_batch.raw_records.each do |values|
            rows << Row.new(values, fields: field_names)
          end
        end
      end
      rows
    end

    # Decode IPC stream chunks into a single Arrow Table (for advanced/columnar
    # consumers who want zero-copy access to the underlying data).
    #
    # @param batches [Array<String>]
    # @return [Arrow::Table, nil]
    def to_table(batches)
      tables = batches.reject { |b| b.nil? || b.empty? }.map do |data|
        ::Arrow::RecordBatchStreamReader.new(::Arrow::BufferInputStream.new(::Arrow::Buffer.new(data))).read_all
      end
      return nil if tables.empty?

      tables.reduce { |acc, t| acc.concatenate([t]) }
    end

    # Serialize an array of Ruby hashes into a single Arrow IPC stream given a
    # Spark {Types::StructType}. Used by {SparkSession#create_data_frame} to ship
    # local data to the server as a `LocalRelation`.
    #
    # @param rows [Array<Hash, Array, Row>]
    # @param schema [Types::StructType]
    # @return [String] Arrow IPC stream bytes.
    def from_rows(rows, schema)
      arrow_schema = build_arrow_schema(schema)
      raw_rows = rows.map do |row|
        schema.fields.each_with_index.map { |field, idx| extract_value(row, field.name, idx) }
      end
      record_batch = ::Arrow::RecordBatch.new(arrow_schema, raw_rows)
      buffer = ::Arrow::ResizableBuffer.new(1024)
      ::Arrow::BufferOutputStream.open(buffer) do |output|
        ::Arrow::RecordBatchStreamWriter.open(output, arrow_schema) do |writer|
          writer.write_record_batch(record_batch)
        end
      end
      buffer.data.to_s
    end

    def build_arrow_schema(schema)
      fields = schema.fields.map do |f|
        ::Arrow::Field.new(f.name, arrow_field_type(f.data_type))
      end
      ::Arrow::Schema.new(fields)
    end

    def extract_value(row, name, idx)
      case row
      when Row then row[name]
      when Hash
        if row.key?(name) then row[name]
        elsif row.key?(name.to_sym) then row[name.to_sym]
        end
      when Array then row[idx]
      else row
      end
    end

    # Map a Spark type to the corresponding `red-arrow` data type used when
    # building local relations.
    def arrow_field_type(data_type)
      case data_type
      when Types::BooleanType then :boolean
      when Types::ByteType then :int8
      when Types::ShortType then :int16
      when Types::IntegerType then :int32
      when Types::LongType then :int64
      when Types::FloatType then :float
      when Types::DoubleType then :double
      when Types::StringType, Types::CharType, Types::VarcharType then :string
      when Types::BinaryType then :binary
      when Types::DateType then :date32
      when Types::TimestampType, Types::TimestampNTZType then { type: :timestamp, unit: :micro }
      when Types::ArrayType
        { type: :list, field: { name: "element", type: arrow_field_type(data_type.element_type) } }
      when Types::StructType
        { type: :struct, fields: data_type.fields.map { |f| { name: f.name, type: arrow_field_type(f.data_type) } } }
      else :string # rubocop:disable Lint/DuplicateBranch -- string default for unsupported-locally types
      end
    end
  end
end
