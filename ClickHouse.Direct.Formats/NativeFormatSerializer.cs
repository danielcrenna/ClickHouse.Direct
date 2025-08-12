using System.Buffers;
using System.Collections;
using System.Text;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Formats;

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
            WriteString(writer, column.GetClickHouseTypeName());
            
            // Write column data
            var columnData = block.GetColumnData(columnIndex);
            
            if (column.ArrayDepth > 0)
            {
                WriteNestedArrayColumn(writer, column.Type, columnData, column.ArrayDepth);
            }
            else
            {
                WriteColumnDynamic(writer, column.Type, columnData, block.RowCount);
            }
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
            ReadString(ref sequence, out bytes);
            bytesConsumed += bytes;
            
            // Validate column matches expected
            if (name != columns[i].Name)
                throw new InvalidOperationException($"Column name mismatch at index {i}: expected '{columns[i].Name}', got '{name}'");
            
            // Read column data
            int columnBytesConsumed;
            if (columns[i].ArrayDepth > 0)
            {
                columnBytesConsumed = ReadNestedArrayColumn(ref sequence, columns[i].Type, rowCount, columns[i].ArrayDepth, out var data);
                columnData.Add(data);
            }
            else
            {
                columnBytesConsumed = ReadColumnDynamic(ref sequence, columns[i].Type, rowCount, columnData);
            }
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
    
    private static void WriteArrayNativeColumn(IBufferWriter<byte> writer, IClickHouseType elementType, IList arrays)
    {
        
        // Write offsets array (UInt64 for each row)
        var span = writer.GetSpan(arrays.Count * 8);
        var offset = 0;
        ulong cumulativeOffset = 0;
        
        for (var i = 0; i < arrays.Count; i++)
        {
            var array = (Array)arrays[i]!;
            cumulativeOffset += (ulong)array.Length;
            
            var bytes = BitConverter.GetBytes(cumulativeOffset);
            bytes.CopyTo(span.Slice(offset));
            offset += 8;
        }
        writer.Advance(arrays.Count * 8);
        
        // Write all elements contiguously
        foreach (Array array in arrays)
        {
            foreach (var element in array)
            {
                WriteValueDynamic(writer, elementType, element);
            }
        }
    }
    
    private static int ReadArrayNativeColumn(ref ReadOnlySequence<byte> sequence, IClickHouseType elementType, int rowCount, out IList data)
    {
        
        var bytesConsumed = 0;
        
        // Read offsets array
        var offsets = new ulong[rowCount];
        var reader = new SequenceReader<byte>(sequence);
        
        for (var i = 0; i < rowCount; i++)
        {
            if (!reader.TryReadLittleEndian(out long offset))
                throw new InvalidOperationException("Failed to read array offset");
            offsets[i] = (ulong)offset;
            bytesConsumed += 8;
        }
        sequence = sequence.Slice(bytesConsumed);
        
        // Create list for arrays
        var arrayType = elementType.ClrType.MakeArrayType();
        var listType = typeof(List<>).MakeGenericType(arrayType);
        data = (IList)Activator.CreateInstance(listType, rowCount)!;
        
        // Read arrays based on offsets
        ulong previousOffset = 0;
        for (var i = 0; i < rowCount; i++)
        {
            var currentOffset = offsets[i];
            var elementCount = (int)(currentOffset - previousOffset);
            
            var arrayInstance = Array.CreateInstance(elementType.ClrType, elementCount);
            
            for (var j = 0; j < elementCount; j++)
            {
                var element = ReadValueDynamic(ref sequence, elementType, out var elementBytes);
                arrayInstance.SetValue(element, j);
                bytesConsumed += elementBytes;
            }
            
            data.Add(arrayInstance);
            previousOffset = currentOffset;
        }
        
        return bytesConsumed;
    }
    
    private static void WriteValueDynamic(IBufferWriter<byte> writer, IClickHouseType type, object? value)
    {
        dynamic columnWriter = type;
        dynamic? dynamicValue = value;
        columnWriter.WriteValue(writer, dynamicValue);
    }
    
    private static object? ReadValueDynamic(ref ReadOnlySequence<byte> sequence, IClickHouseType type, out int bytesConsumed)
    {
        dynamic columnReader = type;
        return columnReader.ReadValue(ref sequence, out bytesConsumed);
    }
    
    private static void WriteNestedArrayColumn(IBufferWriter<byte> writer, IClickHouseType elementType, IList arrays, int depth)
    {
        if (depth == 1)
        {
            // Base case: simple array
            WriteArrayNativeColumn(writer, elementType, arrays);
        }
        else
        {
            // Recursive case: array of arrays
            // Write offsets for current level
            var span = writer.GetSpan(arrays.Count * 8);
            var offset = 0;
            ulong cumulativeOffset = 0;
            
            for (var i = 0; i < arrays.Count; i++)
            {
                var array = (Array)arrays[i]!;
                cumulativeOffset += (ulong)array.Length;
                
                var bytes = BitConverter.GetBytes(cumulativeOffset);
                bytes.CopyTo(span.Slice(offset));
                offset += 8;
            }
            writer.Advance(arrays.Count * 8);
            
            // Flatten one level and recurse
            var flattenedArrays = new List<Array>();
            foreach (Array array in arrays)
            {
                foreach (var nestedArray in array)
                {
                    flattenedArrays.Add((Array)nestedArray);
                }
            }
            
            WriteNestedArrayColumn(writer, elementType, flattenedArrays, depth - 1);
        }
    }
    
    private static int ReadNestedArrayColumn(ref ReadOnlySequence<byte> sequence, IClickHouseType elementType, int rowCount, int depth, out IList data)
    {
        if (depth == 1)
        {
            // Base case: simple array
            return ReadArrayNativeColumn(ref sequence, elementType, rowCount, out data);
        }
        else
        {
            // Recursive case: array of arrays
            var bytesConsumed = 0;
            
            // Read offsets for current level
            var offsets = new ulong[rowCount];
            var reader = new SequenceReader<byte>(sequence);
            
            for (var i = 0; i < rowCount; i++)
            {
                if (!reader.TryReadLittleEndian(out long offset))
                    throw new InvalidOperationException("Failed to read array offset");
                offsets[i] = (ulong)offset;
                bytesConsumed += 8;
            }
            sequence = sequence.Slice(bytesConsumed);
            
            // Calculate total elements at next level
            var totalNextLevel = (int)offsets[rowCount - 1];
            
            // Read nested arrays recursively
            var nestedBytesConsumed = ReadNestedArrayColumn(ref sequence, elementType, totalNextLevel, depth - 1, out var nestedData);
            bytesConsumed += nestedBytesConsumed;
            
            // Build the proper nested structure
            var arrayType = elementType.ClrType;
            for (var d = 0; d < depth; d++)
            {
                arrayType = arrayType.MakeArrayType();
            }
            var listType = typeof(List<>).MakeGenericType(arrayType);
            data = (IList)Activator.CreateInstance(listType, rowCount)!;
            
            // Reconstruct arrays based on offsets
            ulong previousOffset = 0;
            var nestedIndex = 0;
            for (var i = 0; i < rowCount; i++)
            {
                var currentOffset = offsets[i];
                var elementCount = (int)(currentOffset - previousOffset);
                
                // Create array at current depth
                var currentArray = Array.CreateInstance(arrayType.GetElementType()!, elementCount);
                
                for (var j = 0; j < elementCount; j++)
                {
                    currentArray.SetValue(nestedData[nestedIndex++], j);
                }
                
                data.Add(currentArray);
                previousOffset = currentOffset;
            }
            
            return bytesConsumed;
        }
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
}