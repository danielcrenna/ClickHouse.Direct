using System.Buffers;

namespace ClickHouse.Direct.Abstractions;

public interface IColumnWriter<T>
{
    void WriteValue(IBufferWriter<byte> writer, T value);
    void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<T> values);
    int GetFixedByteLength();
}