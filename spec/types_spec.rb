# frozen_string_literal: true

RSpec.describe SparkConnect::Types do
  T = described_class

  describe "#simple_string" do
    {
      T.null => "void",
      T.boolean => "boolean",
      T.byte => "tinyint",
      T.short => "smallint",
      T.integer => "int",
      T.long => "bigint",
      T.float => "float",
      T.double => "double",
      T.string => "string",
      T.binary => "binary",
      T.date => "date",
      T.timestamp => "timestamp",
      T.timestamp_ntz => "timestamp_ntz",
      T.decimal(12, 3) => "decimal(12,3)",
      T::CharType.new(10) => "char(10)",
      T::VarcharType.new(20) => "varchar(20)",
      T.array(T.integer) => "array<int>",
      T.map(T.string, T.long) => "map<string,bigint>",
    }.each do |type, expected|
      it "renders #{expected.inspect}" do
        expect(type.simple_string).to eq(expected)
      end
    end

    it "renders a struct" do
      st = T.struct(T.field("a", T.integer), T.field("b", T.string))
      expect(st.simple_string).to eq("struct<a:int,b:string>")
    end

    it "nests array/map/struct simple strings" do
      type = T.array(T.map(T.string, T.struct(T.field("x", T.long))))
      expect(type.simple_string).to eq("array<map<string,struct<x:bigint>>>")
    end

    it "aliases to_s to simple_string" do
      expect(T.integer.to_s).to eq("int")
    end
  end

  describe "#type_name" do
    it "uses Spark short names that differ from simple_string" do
      expect(T.byte.type_name).to eq("byte")
      expect(T.short.type_name).to eq("short")
      expect(T.integer.type_name).to eq("integer")
      expect(T.long.type_name).to eq("long")
      expect(T.decimal.type_name).to eq("decimal")
      expect(T.array(T.string).type_name).to eq("array")
      expect(T.map(T.string, T.string).type_name).to eq("map")
      expect(T.struct.type_name).to eq("struct")
    end
  end

  describe "#to_proto / Types.from_proto round-trips" do
    [
      ["null", T.null],
      ["boolean", T.boolean],
      ["byte", T.byte],
      ["short", T.short],
      ["integer", T.integer],
      ["long", T.long],
      ["float", T.float],
      ["double", T.double],
      ["string", T.string],
      ["binary", T.binary],
      ["date", T.date],
      ["timestamp", T.timestamp],
      ["timestamp_ntz", T.timestamp_ntz],
      ["decimal", T.decimal(18, 4)],
      ["char", T::CharType.new(8)],
      ["varchar", T::VarcharType.new(16)],
      ["array", T.array(T.integer, contains_null: false)],
      ["map", T.map(T.string, T.long, value_contains_null: false)],
    ].each do |label, type|
      it "round-trips #{label}" do
        expect(T.from_proto(type.to_proto)).to eq(type)
      end
    end

    it "round-trips a nested struct" do
      st = T.struct(
        T.field("id", T.long, nullable: false),
        T.field("tags", T.array(T.string)),
        T.field("attrs", T.map(T.string, T.integer)),
        T.field("inner", T.struct(T.field("x", T.double)))
      )
      round = T.from_proto(st.to_proto)
      expect(round).to eq(st)
      expect(round.fields.first.nullable).to be(false)
    end

    it "preserves decimal precision and scale through proto" do
      proto = T.decimal(38, 10).to_proto
      back = T.from_proto(proto)
      expect([back.precision, back.scale]).to eq([38, 10])
    end

    it "preserves array contains_null and map value_contains_null" do
      arr = T.from_proto(T.array(T.string, contains_null: false).to_proto)
      map = T.from_proto(T.map(T.string, T.string, value_contains_null: false).to_proto)
      expect(arr.contains_null).to be(false)
      expect(map.value_contains_null).to be(false)
    end

    it "round-trips struct field metadata" do
      st = T.struct(T.field("a", T.integer, metadata: { "comment" => "hi" }))
      back = T.from_proto(st.to_proto)
      expect(back.fields.first.metadata).to eq({ "comment" => "hi" })
    end
  end

  describe "StructType" do
    it "supports #add chaining with name + type" do
      st = T::StructType.new.add("a", T.integer).add("b", T.string, nullable: false)
      expect(st).to be_a(T::StructType)
      expect(st.names).to eq(%w[a b])
      expect(st["b"].nullable).to be(false)
    end

    it "supports #add with a StructField" do
      st = T::StructType.new.add(T.field("z", T.long))
      expect(st.names).to eq(%w[z])
    end

    it "looks up fields by index and by name via #[]" do
      st = T.struct(T.field("a", T.integer), T.field("b", T.string))
      expect(st[0].name).to eq("a")
      expect(st[1].name).to eq("b")
      expect(st["b"].data_type).to eq(T.string)
      expect(st[:a].data_type).to eq(T.integer)
    end

    it "reports length/size and is Enumerable" do
      st = T.struct(T.field("a", T.integer), T.field("b", T.string))
      expect(st.length).to eq(2)
      expect(st.size).to eq(2)
      expect(st.map(&:name)).to eq(%w[a b])
    end

    it "renders tree_string" do
      st = T.struct(
        T.field("id", T.long),
        T.field("name", T.string, nullable: false),
        T.field("tags", T.array(T.string)),
        T.field("m", T.map(T.string, T.integer)),
        T.field("nested", T.struct(T.field("a", T.integer)))
      )
      expected = <<~TREE
        root
         |-- id: long (nullable = true)
         |-- name: string (nullable = false)
         |-- tags: array (nullable = true)
         |    |-- element: string (containsNull = true)
         |-- m: map (nullable = true)
         |    |-- key: string
         |    |-- value: integer (valueContainsNull = true)
         |-- nested: struct (nullable = true)
         |    |-- a: integer (nullable = true)
      TREE
      expect(st.tree_string).to eq(expected)
    end
  end

  describe "#json_value" do
    it "returns the short name for atomic types" do
      expect(T.integer.json_value).to eq("integer")
      expect(T.long.json_value).to eq("long")
    end

    it "returns decimal/char/varchar parameterized strings" do
      expect(T.decimal(12, 2).json_value).to eq("decimal(12,2)")
      expect(T::CharType.new(5).json_value).to eq("char(5)")
      expect(T::VarcharType.new(7).json_value).to eq("varchar(7)")
    end

    it "returns a hash for arrays" do
      expect(T.array(T.integer, contains_null: false).json_value).to eq(
        { "type" => "array", "elementType" => "integer", "containsNull" => false }
      )
    end

    it "returns a hash for maps" do
      expect(T.map(T.string, T.long).json_value).to eq(
        { "type" => "map", "keyType" => "string", "valueType" => "long", "valueContainsNull" => true }
      )
    end

    it "returns a hash for structs with nested fields" do
      st = T.struct(T.field("a", T.array(T.integer)))
      expect(st.json_value).to eq(
        {
          "type" => "struct",
          "fields" => [
            {
              "name" => "a",
              "type" => { "type" => "array", "elementType" => "integer", "containsNull" => true },
              "nullable" => true,
            },
          ],
        }
      )
    end
  end

  describe "equality" do
    it "compares atomic types by class" do
      expect(T.integer == T::IntegerType.new).to be(true)
      expect(T.integer).not_to eq(T.long)
    end

    it "compares decimals by precision and scale" do
      expect(T.decimal(10, 2) == T::DecimalType.new(10, 2)).to be(true)
      expect(T.decimal(10, 2)).not_to eq(T.decimal(10, 3))
    end

    it "compares arrays by element type and contains_null" do
      expect(T.array(T.integer) == T.array(T.integer)).to be(true)
      expect(T.array(T.integer, contains_null: false)).not_to eq(T.array(T.integer))
      expect(T.array(T.integer)).not_to eq(T.array(T.string))
    end

    it "compares maps structurally" do
      expect(T.map(T.string, T.long) == T.map(T.string, T.long)).to be(true)
      expect(T.map(T.string, T.long)).not_to eq(T.map(T.string, T.integer))
    end

    it "compares structs by their fields" do
      a = T.struct(T.field("a", T.integer))
      b = T.struct(T.field("a", T.integer))
      c = T.struct(T.field("a", T.integer, nullable: false))
      expect(a).to eq(b)
      expect(a).not_to eq(c)
    end

    it "hashes equal atomic types identically" do
      expect(T.integer.hash).to eq(T::IntegerType.new.hash)
    end
  end
end
