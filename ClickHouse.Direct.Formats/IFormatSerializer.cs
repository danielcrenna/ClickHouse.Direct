using System.Buffers;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Formats;

public interface IFormatSerializer
{
    void WriteBlock(Block block, IBufferWriter<byte> writer);
    Block ReadBlock(int rows, IReadOnlyList<ColumnDescriptor> columns, ref ReadOnlySequence<byte> sequence, out int bytesConsumed);
}