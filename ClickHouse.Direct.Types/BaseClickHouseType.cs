using System.Buffers;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// https://github.com/ClickHouse/ClickHouse/blob/master/docs/en/sql-reference/data-types/data-types-binary-encoding.md
/// </summary>
public abstract class BaseClickHouseType<T> : IClickHouseType, IColumnReader<T>, IColumnWriter<T>
{
    public abstract byte ProtocolCode { get; }
    public Type ClrType => typeof(T);
    public abstract string TypeName { get; }
    public abstract bool IsFixedLength { get; }
    public abstract int FixedByteLength { get; }

    public abstract T ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed);
    public abstract int ReadValues(ref ReadOnlySequence<byte> sequence, Span<T> destination, out int bytesConsumed);
    public abstract void WriteValue(IBufferWriter<byte> writer, T value);
    public abstract void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<T> values);
    public int GetFixedByteLength() => FixedByteLength;
}