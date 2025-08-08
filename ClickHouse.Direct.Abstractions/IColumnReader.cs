using System.Buffers;

namespace ClickHouse.Direct.Abstractions;

public interface IColumnReader<T>
{
    T ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed);
    int ReadValues(ref ReadOnlySequence<byte> sequence, Span<T> destination, out int bytesConsumed);
}