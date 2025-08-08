using System.Buffers;
using System.Collections;
using System.Text;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Protocol;

/// <summary>
/// Serializer for ClickHouse Native format.
/// https://clickhouse.com/docs/integrations/data-formats/binary-native
/// https://clickhouse.com/docs/interfaces/formats/Native
/// 
/// Native format structure over HTTP:
/// - Column count (varint)
/// - Row count (varint)  
/// - For each column:
///   - Name length (varint) + Name (UTF8)
///   - Type length (varint) + Type (UTF8)
/// - Column data in column-oriented format
/// </summary>
public sealed class NativeFormatSerializer : IFormatSerializer
{
    public void WriteBlock(Block block, IBufferWriter<byte> writer)
    {
        // Write header
        WriteVarInt(writer, (ulong)block.ColumnCount);
        WriteVarInt(writer, (ulong)block.RowCount);
        
        // Write column metadata and data
        for (var columnIndex = 0; columnIndex < block.ColumnCount; columnIndex++)
        {
            var column = block.Columns[columnIndex];
            
            // Write column name
            WriteString(writer, column.Name);
            
            // Write column type name
            WriteString(writer, GetTypeName(column.Type));
            
            // Write column data
            var columnData = block.GetColumnData(columnIndex);
            WriteColumnDynamic(writer, column.Type, columnData, block.RowCount);
        }
    }

    public Block ReadBlock(int rows, IReadOnlyList<ColumnDescriptor> columns, ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        bytesConsumed = 0;
        
        // Read header
        var columnCount = (int)ReadVarInt(ref sequence, out var bytes);
        bytesConsumed += bytes;
        
        var rowCount = (int)ReadVarInt(ref sequence, out bytes);
        bytesConsumed += bytes;
        
        // Validate header matches expected
        if (columnCount != columns.Count)
            throw new InvalidOperationException($"Column count mismatch: expected {columns.Count}, got {columnCount}");
        if (rowCount != rows)
            throw new InvalidOperationException($"Row count mismatch: expected {rows}, got {rowCount}");
        
        var columnData = new List<IList>(columns.Count);
        
        // Read each column metadata and data
        for (var i = 0; i < columnCount; i++)
        {
            // Read column name
            var name = ReadString(ref sequence, out bytes);
            bytesConsumed += bytes;
            
            // Read column type
            var typeName = ReadString(ref sequence, out bytes);
            bytesConsumed += bytes;
            
            // Validate column matches expected
            if (name != columns[i].Name)
                throw new InvalidOperationException($"Column name mismatch at index {i}: expected '{columns[i].Name}', got '{name}'");
            
            // Read column data
            var columnBytesConsumed = ReadColumnDynamic(ref sequence, columns[i].Type, rowCount, columnData);
            bytesConsumed += columnBytesConsumed;
        }
        
        return Block.CreateFromColumnData(columns, columnData, rowCount);
    }
    
    private static void WriteColumnDynamic(IBufferWriter<byte> writer, IClickHouseType type, IList data, int rowCount)
    {
        dynamic columnWriter = type;
        for (var i = 0; i < rowCount; i++)
        {
            dynamic? value = data[i];
            columnWriter.WriteValue(writer, value);
        }
    }
    
    private static int ReadColumnDynamic(ref ReadOnlySequence<byte> sequence, IClickHouseType type, int rowCount, List<IList> columnData)
    {
        var totalBytesConsumed = 0;
        dynamic columnReader = type;
        
        // Create typed list using the type's CLR type
        var listType = typeof(List<>).MakeGenericType(type.ClrType);
        var data = (IList)Activator.CreateInstance(listType, rowCount)!;
        
        for (var i = 0; i < rowCount; i++)
        {
            var value = columnReader.ReadValue(ref sequence, out int bytesConsumed);
            data.Add(value);
            totalBytesConsumed += bytesConsumed;
        }
        
        columnData.Add(data);
        return totalBytesConsumed;
    }
    
    private static void WriteVarInt(IBufferWriter<byte> writer, ulong value)
    {
        var span = writer.GetSpan(9); // Max varint size
        var bytesWritten = 0;
        
        while (value >= 0x80)
        {
            span[bytesWritten++] = (byte)(value | 0x80);
            value >>= 7;
        }
        span[bytesWritten++] = (byte)value;
        
        writer.Advance(bytesWritten);
    }
    
    private static ulong ReadVarInt(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        bytesConsumed = 0;
        ulong result = 0;
        var shift = 0;
        
        var reader = new SequenceReader<byte>(sequence);
        
        while (reader.TryRead(out var b))
        {
            bytesConsumed++;
            result |= (ulong)(b & 0x7F) << shift;
            
            if ((b & 0x80) == 0)
            {
                sequence = sequence.Slice(bytesConsumed);
                return result;
            }
            
            shift += 7;
            if (shift >= 64)
                throw new InvalidOperationException("VarInt too large");
        }
        
        throw new InvalidOperationException("Unexpected end of varint");
    }
    
    private static void WriteString(IBufferWriter<byte> writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteVarInt(writer, (ulong)bytes.Length);
        writer.Write(bytes);
    }
    
    private static string ReadString(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        var length = (int)ReadVarInt(ref sequence, out bytesConsumed);
        
        if (length == 0)
            return string.Empty;
        
        var stringBytes = sequence.Slice(0, length);
        var result = Encoding.UTF8.GetString(stringBytes.ToArray());
        
        sequence = sequence.Slice(length);
        bytesConsumed += length;
        
        return result;
    }
    
    private static string GetTypeName(IClickHouseType type)
    {
        // Map our type instances to ClickHouse type names
        // This should eventually be a property on the type itself
        return type switch
        {
            _ when type.GetType().Name == "Int32Type" => "Int32",
            _ when type.GetType().Name == "StringType" => "String",
            _ when type.GetType().Name == "UuidType" => "UUID",
            _ => type.GetType().Name.Replace("Type", "")
        };
    }
}