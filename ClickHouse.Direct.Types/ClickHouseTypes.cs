using ClickHouse.Direct.Abstractions;
using System.Collections.Frozen;

namespace ClickHouse.Direct.Types;

/// <summary>
/// Central registry of all ClickHouse data types with protocol code mapping.
/// Provides single source of truth for type instances and protocol code resolution.
/// </summary>
public static class ClickHouseTypes
{
    // Core type instances
    public static readonly Int32Type Int32 = Int32Type.Instance;
    public static readonly StringType String = StringType.Instance;
    public static readonly UuidType UUID = UuidType.Instance;

    // Protocol code to type mapping for fast lookup
    private static readonly FrozenDictionary<byte, IClickHouseType> _byProtocolCode = 
        new Dictionary<byte, IClickHouseType>
        {
            [Int32.ProtocolCode] = Int32,
            [String.ProtocolCode] = String,
            [UUID.ProtocolCode] = UUID
        }.ToFrozenDictionary();

    // Type name to type mapping for string-based lookup
    private static readonly FrozenDictionary<string, IClickHouseType> _byTypeName =
        new Dictionary<string, IClickHouseType>(StringComparer.OrdinalIgnoreCase)
        {
            [Int32.TypeName] = Int32,
            [String.TypeName] = String,
            [UUID.TypeName] = UUID
        }.ToFrozenDictionary();

    /// <summary>
    /// Gets a type instance by its protocol code (as received from ClickHouse server).
    /// </summary>
    /// <param name="protocolCode">The protocol code byte</param>
    /// <returns>The corresponding type instance</returns>
    /// <exception cref="ArgumentException">If the protocol code is not supported</exception>
    public static IClickHouseType GetByProtocolCode(byte protocolCode)
    {
        if (_byProtocolCode.TryGetValue(protocolCode, out var type))
            return type;
        
        throw new ArgumentException($"Unsupported protocol code: 0x{protocolCode:X2}", nameof(protocolCode));
    }

    /// <summary>
    /// Gets a type instance by its name (case-insensitive).
    /// </summary>
    /// <param name="typeName">The type name (e.g., "Int32", "String")</param>
    /// <returns>The corresponding type instance</returns>
    /// <exception cref="ArgumentException">If the type name is not supported</exception>
    public static IClickHouseType GetByTypeName(string typeName)
    {
        if (_byTypeName.TryGetValue(typeName, out var type))
            return type;
        
        throw new ArgumentException($"Unsupported type name: {typeName}", nameof(typeName));
    }

    /// <summary>
    /// Tries to get a type instance by its protocol code.
    /// </summary>
    /// <param name="protocolCode">The protocol code byte</param>
    /// <param name="type">The type instance if found</param>
    /// <returns>True if the protocol code is supported</returns>
    public static bool TryGetByProtocolCode(byte protocolCode, out IClickHouseType? type)
    {
        return _byProtocolCode.TryGetValue(protocolCode, out type);
    }

    /// <summary>
    /// Tries to get a type instance by its name (case-insensitive).
    /// </summary>
    /// <param name="typeName">The type name</param>
    /// <param name="type">The type instance if found</param>
    /// <returns>True if the type name is supported</returns>
    public static bool TryGetByTypeName(string typeName, out IClickHouseType? type)
    {
        return _byTypeName.TryGetValue(typeName, out type);
    }

    /// <summary>
    /// Gets all supported protocol codes.
    /// </summary>
    public static IEnumerable<byte> SupportedProtocolCodes => _byProtocolCode.Keys;

    /// <summary>
    /// Gets all supported type names.
    /// </summary>
    public static IEnumerable<string> SupportedTypeNames => _byTypeName.Keys;
}