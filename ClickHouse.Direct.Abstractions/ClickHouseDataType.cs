namespace ClickHouse.Direct.Abstractions;

public enum ClickHouseDataType : byte
{
    // Integers
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    UInt8 = 5,
    UInt16 = 6,
    UInt32 = 7,
    UInt64 = 8,

    // Floating point
    Float32 = 9,
    Float64 = 10,

    // String types
    String = 11,
    FixedString = 12,

    // Date and time
    Date = 13,
    DateTime = 14,
    DateTime64 = 15,

    // UUID
    UUID = 16,

    // Array
    Array = 17,

    // Nullable
    Nullable = 18,

    // LowCardinality
    LowCardinality = 19,

    // Decimal
    Decimal32 = 20,
    Decimal64 = 21,
    Decimal128 = 22,
    Decimal256 = 23,

    // Boolean
    Bool = 24,

    // IPv4/IPv6
    IPv4 = 25,
    IPv6 = 26,

    // Enum
    Enum8 = 27,
    Enum16 = 28,

    // Tuple
    Tuple = 29,

    // Map
    Map = 30,

    // Nested
    Nested = 31
}