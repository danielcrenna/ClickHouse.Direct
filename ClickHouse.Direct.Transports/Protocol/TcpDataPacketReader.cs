using System.Text;

namespace ClickHouse.Direct.Transports.Protocol;

/// <summary>
/// Reads Data packets from the TCP protocol stream
/// </summary>
public static class TcpDataPacketReader
{
    /// <summary>
    /// Reads a Data packet from a BinaryReader
    /// </summary>
    /// <returns>The raw Native format data (after table name and block info)</returns>
    public static byte[] ReadDataPacket(BinaryReader reader)
    {
        // Read table name (can be empty)
        var tableName = ReadString(reader);
        
        // Read block info
        // For TCP protocol, the block info is always present but may be minimal
        var isOverflows = reader.ReadByte();
        
        // bucket_num can be -1 (0xFFFFFFFF) or 0
        var bucketNum = reader.ReadByte();
        
        // These fields are only present if is_overflows is non-zero
        if (isOverflows != 0)
        {
            // Read additional overflow fields
            _ = reader.ReadInt32(); // bucket_num as int32 when overflows
            _ = ReadVarInt(reader); // num_rows_in_bucket
        }
        
        // Now read the Native format block
        // First, check if we have columns and rows
        var numColumns = ReadVarInt(reader);
        var numRows = ReadVarInt(reader);
        
        if (numColumns == 0 || numRows == 0)
        {
            // Empty block
            return Array.Empty<byte>();
        }
        
        // For non-empty blocks, we need to read the entire Native format data
        // This includes column metadata and data
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        
        // Write the Native format header (columns and rows)
        WriteVarInt(writer, numColumns);
        WriteVarInt(writer, numRows);
        
        // Read and write each column
        for (ulong i = 0; i < numColumns; i++)
        {
            // Read column name
            var columnName = ReadString(reader);
            WriteString(writer, columnName);
            
            // Read column type
            var columnType = ReadString(reader);
            WriteString(writer, columnType);
            
            // Read column data - this is type-specific
            // For now, we'll read the raw bytes based on the type
            var columnData = ReadColumnData(reader, columnType, numRows);
            writer.Write(columnData);
        }
        
        writer.Flush();
        return ms.ToArray();
    }
    
    private static byte[] ReadColumnData(BinaryReader reader, string typeName, ulong numRows)
    {
        // This is a simplified implementation
        // The actual format depends on the column type
        
        using var ms = new MemoryStream();
        
        // Handle common types
        if (typeName.StartsWith("Int32"))
        {
            // Int32 is 4 bytes per value
            for (ulong i = 0; i < numRows; i++)
            {
                var value = reader.ReadInt32();
                ms.Write(BitConverter.GetBytes(value), 0, 4);
            }
        }
        else if (typeName.StartsWith("Int64"))
        {
            // Int64 is 8 bytes per value
            for (ulong i = 0; i < numRows; i++)
            {
                var value = reader.ReadInt64();
                ms.Write(BitConverter.GetBytes(value), 0, 8);
            }
        }
        else if (typeName.StartsWith("String"))
        {
            // String is length-prefixed
            for (ulong i = 0; i < numRows; i++)
            {
                var str = ReadString(reader);
                var bytes = Encoding.UTF8.GetBytes(str);
                WriteVarInt(ms, (ulong)bytes.Length);
                ms.Write(bytes, 0, bytes.Length);
            }
        }
        else if (typeName.StartsWith("UInt8") || typeName.StartsWith("Bool"))
        {
            // Single byte per value
            for (ulong i = 0; i < numRows; i++)
            {
                ms.WriteByte(reader.ReadByte());
            }
        }
        else if (typeName.StartsWith("UInt32"))
        {
            // UInt32 is 4 bytes per value
            for (ulong i = 0; i < numRows; i++)
            {
                var value = reader.ReadUInt32();
                ms.Write(BitConverter.GetBytes(value), 0, 4);
            }
        }
        else if (typeName.StartsWith("Float32"))
        {
            // Float32 is 4 bytes per value
            for (ulong i = 0; i < numRows; i++)
            {
                var value = reader.ReadSingle();
                ms.Write(BitConverter.GetBytes(value), 0, 4);
            }
        }
        else if (typeName.StartsWith("Float64"))
        {
            // Float64 is 8 bytes per value
            for (ulong i = 0; i < numRows; i++)
            {
                var value = reader.ReadDouble();
                ms.Write(BitConverter.GetBytes(value), 0, 8);
            }
        }
        else
        {
            // Unknown type - try to read as raw bytes
            // This is a fallback and may not work correctly
            throw new NotSupportedException($"Column type '{typeName}' is not yet supported");
        }
        
        return ms.ToArray();
    }
    
    private static string ReadString(BinaryReader reader)
    {
        var length = (int)ReadVarInt(reader);
        if (length == 0)
            return string.Empty;
        
        var bytes = reader.ReadBytes(length);
        return Encoding.UTF8.GetString(bytes);
    }
    
    private static void WriteString(BinaryWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value ?? "");
        WriteVarInt(writer, (ulong)bytes.Length);
        if (bytes.Length > 0)
        {
            writer.Write(bytes);
        }
    }
    
    private static ulong ReadVarInt(BinaryReader reader)
    {
        ulong value = 0;
        var shift = 0;
        
        while (true)
        {
            var b = reader.ReadByte();
            value |= (ulong)(b & 0x7F) << shift;
            
            if ((b & 0x80) == 0)
                return value;
            
            shift += 7;
            if (shift >= 64)
                throw new InvalidOperationException("VarInt too large");
        }
    }
    
    private static void WriteVarInt(BinaryWriter writer, ulong value)
    {
        while (value >= 0x80)
        {
            writer.Write((byte)(value | 0x80));
            value >>= 7;
        }
        writer.Write((byte)value);
    }
    
    private static void WriteVarInt(Stream stream, ulong value)
    {
        while (value >= 0x80)
        {
            stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        stream.WriteByte((byte)value);
    }
}