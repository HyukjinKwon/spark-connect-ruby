---
title: Types & Schemas
nav_order: 8
---

# Types & Schemas

The `SparkConnect::Types` module models the Spark SQL type system. Every type is
an instance of a `SparkConnect::Types::DataType` subclass.

## Constructing types

Convenience constructors live directly on the module:

```ruby
T = SparkConnect::Types

T.long; T.integer; T.short; T.byte
T.double; T.float; T.decimal(10, 2)
T.string; T.binary; T.boolean
T.date; T.timestamp; T.timestamp_ntz
T.array(T.string)
T.map(T.string, T.long)
T.struct(
  T.field("id", T.long, nullable: false),
  T.field("name", T.string),
  T.field("tags", T.array(T.string)),
)
```

## Struct types and schemas

A DataFrame's schema is a `StructType`. You can build one fluently:

```ruby
schema = T.struct
          .add("id", T.long, nullable: false)
          .add("name", T.string)

schema.names                #=> ["id", "name"]
schema["name"].data_type    #=> #<SparkConnect::Types::StringType string>
schema.simple_string        #=> "struct<id:bigint,name:string>"
```

## Rendering

| Method           | Example output |
| ---------------- | -------------- |
| `simple_string`  | `array<string>`, `decimal(10,2)`, `struct<a:int>` |
| `type_name`      | `integer`, `long`, `string` |
| `json`           | Spark's JSON schema representation |
| `tree_string`    | indented schema tree (used by `print_schema`) |

```ruby
df.print_schema
# root
#  |-- id: long (nullable = false)
#  |-- name: string (nullable = true)

df.schema.simple_string
df.dtypes   #=> [["id", "bigint"], ["name", "string"]]
```

## DDL strings

Anywhere a schema is accepted you may pass a DDL string instead of a
`StructType`:

```ruby
spark.read.schema("id BIGINT, name STRING").csv("people.csv")
spark.create_data_frame(rows, "id BIGINT, name STRING")
```

## Ruby <-> Spark value mapping

When building literals and local DataFrames, Ruby values map to Spark types as
follows (and the inverse applies when decoding results):

| Ruby                         | Spark type        |
| ---------------------------- | ----------------- |
| `nil`                        | null              |
| `true` / `false`             | boolean           |
| `Integer` (32-bit range)     | int               |
| `Integer` (larger)           | bigint (long)     |
| `Float`                      | double            |
| `BigDecimal`                 | decimal           |
| `String` (UTF-8)             | string            |
| `String` (ASCII-8BIT)        | binary            |
| `Symbol`                     | string            |
| `Time` / `DateTime`          | timestamp         |
| `Date`                       | date              |
| `Array`                      | array             |
| `Hash`                       | map               |

Results decode back into `SparkConnect::Row` objects, with structs as `Row`/Hash,
arrays as `Array`, maps as `Hash`, decimals as `BigDecimal`, timestamps as
`Time`, and dates as `Date`.
