using System.Buffers;
using System.Collections;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Protocol;

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
                
                WriteValueDynamic(writer, column.Type, value);
            }
        }
    }

    public Block ReadBlock(int rows, IReadOnlyList<ColumnDescriptor> columns, ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        bytesConsumed = 0;
        var columnData = new List<IList>(columns.Count);
        
        foreach (var column in columns)
        {
            var listType = typeof(List<>).MakeGenericType(column.Type.ClrType);
            var list = (IList)Activator.CreateInstance(listType, rows)!;
            columnData.Add(list);
        }
        
        for (var row = 0; row < rows; row++)
        {
            for (var columnIndex = 0; columnIndex < columns.Count; columnIndex++)
            {
                var column = columns[columnIndex];
                var value = ReadValueDynamic(ref sequence, column.Type, out var valueBytesConsumed);
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
}