using ClickHouse.Direct.Abstractions;
using System.Collections.Frozen;

namespace ClickHouse.Direct.Types;

/// <summary>
/// Central registry of all ClickHouse data types with protocol code mapping.
/// Provides single source of truth for type instances and protocol code resolution.
///
/// <see href="https://github.com/ClickHouse/ClickHouse/blob/master/docs/en/sql-reference/data-types/data-types-binary-encoding.md" />
/// </summary>
public static class ClickHouseTypes
{
    // Signed integer type instances
    public static readonly Int8Type Int8 = Int8Type.Instance;
    public static readonly Int16Type Int16 = Int16Type.Instance;
    public static readonly Int32Type Int32 = Int32Type.Instance;
    public static readonly Int64Type Int64 = Int64Type.Instance;
    
    // Unsigned integer type instances
    public static readonly UInt8Type UInt8 = UInt8Type.Instance;
    public static readonly UInt16Type UInt16 = UInt16Type.Instance;
    public static readonly UInt32Type UInt32 = UInt32Type.Instance;
    public static readonly UInt64Type UInt64 = UInt64Type.Instance;
    
    // String type instances
    public static readonly StringType String = StringType.Instance;
    
    // UUID type instances
    public static readonly UuidType Uuid = UuidType.Instance;
    
    // Floating-point type instances
    public static readonly Float32Type Float32 = Float32Type.Instance;
    public static readonly Float64Type Float64 = Float64Type.Instance;
    
    // Date/Time type instances
    public static readonly DateType Date = DateType.Instance;
    public static readonly Date32Type Date32 = Date32Type.Instance;
    public static readonly DateTimeType DateTime = DateTimeType.Instance;
    public static readonly DateTime64Type DateTime64 = DateTime64Type.Instance;
    
    // Boolean type instance (alias for UInt8)
    public static readonly BoolType Bool = BoolType.Instance;
    
    // IP address type instances
    public static readonly IPv4Type IPv4 = IPv4Type.Instance;
    public static readonly IPv6Type IPv6 = IPv6Type.Instance;
    
    // Decimal type instances
    public static readonly Decimal32Type Decimal32 = Decimal32Type.Instance;
    public static readonly Decimal64Type Decimal64 = Decimal64Type.Instance;
    public static readonly Decimal128Type Decimal128 = Decimal128Type.Instance;

    // Protocol code to type mapping for fast lookup
    private static readonly FrozenDictionary<byte, IClickHouseType> ByProtocolCode = 
        new Dictionary<byte, IClickHouseType>
        {
            // Unsigned integers
            [UInt8.ProtocolCode] = UInt8,     // 0x01
            [UInt16.ProtocolCode] = UInt16,   // 0x02
            [UInt32.ProtocolCode] = UInt32,   // 0x03
            [UInt64.ProtocolCode] = UInt64,   // 0x04
            
            // Signed integers
            [Int8.ProtocolCode] = Int8,       // 0x07
            [Int16.ProtocolCode] = Int16,     // 0x08
            [Int32.ProtocolCode] = Int32,     // 0x09
            [Int64.ProtocolCode] = Int64,     // 0x0A
            
            // Other types
            [String.ProtocolCode] = String,   // 0x15
            [Uuid.ProtocolCode] = Uuid,        // 0x1D
            
            // Floating-point types
            [Float32.ProtocolCode] = Float32,  // 0x43
            [Float64.ProtocolCode] = Float64,  // 0x44
            
            // Date/Time types
            [Date.ProtocolCode] = Date,        // 0x10
            [Date32.ProtocolCode] = Date32,    // 0x1E
            [DateTime.ProtocolCode] = DateTime,// 0x11
            [DateTime64.ProtocolCode] = DateTime64, // 0x19
            
            // IP address types
            [IPv4.ProtocolCode] = IPv4,        // 0x13
            [IPv6.ProtocolCode] = IPv6,        // 0x14
            
            // Decimal types
            [Decimal32.ProtocolCode] = Decimal32, // 0x42
            [Decimal64.ProtocolCode] = Decimal64, // 0x17
            [Decimal128.ProtocolCode] = Decimal128 // 0x18
            
            // Note: Bool uses the same protocol code as UInt8 (0x01)
        }.ToFrozenDictionary();
}