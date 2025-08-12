using System.Buffers;
using System.Collections;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Formats;

/// <summary>
/// https://clickhouse.com/docs/integrations/data-formats/binary-native
/// https://clickhouse.com/docs/interfaces/formats/RowBinary
/// </summary>
public sealed class RowBinaryFormatSerializer : IFormatSerializer
{
    public void WriteBlock(Block block, IBufferWriter<byte> writer)
    {
        for (var row = 0; row < block.RowCount; row++)
        {
            for (var columnIndex = 0; columnIndex < block.ColumnCount; columnIndex++)
            {
                var column = block.Columns[columnIndex];
                var value = block[row, columnIndex];
                
                if (column.IsArray)
                {
                    WriteArrayRowBinary(writer, column.Type, value);
                }
                else
                {
                    WriteValueDynamic(writer, column.Type, value);
                }
            }
        }
    }

    public Block ReadBlock(int rows, IReadOnlyList<ColumnDescriptor> columns, ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        bytesConsumed = 0;
        var columnData = new List<IList>(columns.Count);
        
        foreach (var column in columns)
        {
            var listType = typeof(List<>).MakeGenericType(column.GetClrType());
            var list = (IList)Activator.CreateInstance(listType, rows)!;
            columnData.Add(list);
        }
        
        for (var row = 0; row < rows; row++)
        {
            for (var columnIndex = 0; columnIndex < columns.Count; columnIndex++)
            {
                var column = columns[columnIndex];
                object? value;
                int valueBytesConsumed;
                
                if (column.IsArray)
                {
                    value = ReadArrayRowBinary(ref sequence, column.Type, out valueBytesConsumed);
                }
                else
                {
                    value = ReadValueDynamic(ref sequence, column.Type, out valueBytesConsumed);
                }
                
                columnData[columnIndex].Add(value);
                bytesConsumed += valueBytesConsumed;
            }
        }
        
        return Block.CreateFromColumnData(columns, columnData, rows);
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
    
    private static void WriteArrayRowBinary(IBufferWriter<byte> writer, IClickHouseType elementType, object? value)
    {
        if (value == null)
        {
            WriteVarInt(writer, 0);
            return;
        }
        
        var array = (Array)value;
        WriteVarInt(writer, (ulong)array.Length);
        
        foreach (var element in array)
        {
            WriteValueDynamic(writer, elementType, element);
        }
    }
    
    private static object? ReadArrayRowBinary(ref ReadOnlySequence<byte> sequence, IClickHouseType elementType, out int bytesConsumed)
    {
        bytesConsumed = 0;
        
        var length = (int)ReadVarInt(ref sequence, out var varIntBytes);
        bytesConsumed += varIntBytes;
        
        var arrayInstance = Array.CreateInstance(elementType.ClrType, length);
        
        for (var i = 0; i < length; i++)
        {
            var element = ReadValueDynamic(ref sequence, elementType, out var elementBytes);
            arrayInstance.SetValue(element, i);
            bytesConsumed += elementBytes;
        }
        
        return arrayInstance;
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
    
    private static void WriteVarInt(IBufferWriter<byte> writer, ulong value)
    {
        var span = writer.GetSpan(9);
        var bytesWritten = 0;
        
        while (value >= 0x80)
        {
            span[bytesWritten++] = (byte)(value | 0x80);
            value >>= 7;
        }
        span[bytesWritten++] = (byte)value;
        
        writer.Advance(bytesWritten);
    }
}