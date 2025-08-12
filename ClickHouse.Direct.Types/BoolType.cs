using System.Buffers;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse Bool type handler.
///
/// https://clickhouse.com/docs/sql-reference/data-types/boolean
/// 
/// Bool is a semantic alias for UInt8 with values restricted to 0 (false) or 1 (true).
/// This type delegates all operations to UInt8Type but provides boolean semantics.
/// </summary>
public sealed class BoolType : BaseClickHouseType<bool>
{
    public static readonly BoolType Instance = new();
    private readonly UInt8Type _uint8Type = UInt8Type.Instance;

    public override byte ProtocolCode => 0x01; // Same as UInt8
    public override string TypeName => "Bool";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(byte);

    public override bool ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        var value = _uint8Type.ReadValue(ref sequence, out bytesConsumed);
        return value != 0;
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<bool> destination, out int bytesConsumed)
    {
        if (destination.IsEmpty)
        {
            bytesConsumed = 0;
            return 0;
        }

        // Read as bytes first
        Span<byte> byteBuffer = stackalloc byte[Math.Min(destination.Length, 4096)];
        var remaining = destination.Length;
        var destIndex = 0;
        bytesConsumed = 0;

        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, byteBuffer.Length);
            var currentBytes = byteBuffer[..batchSize];
            var read = _uint8Type.ReadValues(ref sequence, currentBytes, out var consumed);
            bytesConsumed += consumed;

            // Convert bytes to bools
            for (var i = 0; i < read; i++)
            {
                destination[destIndex++] = currentBytes[i] != 0;
            }

            remaining -= read;
            
            if (read < batchSize)
                break;
        }

        return destIndex;
    }

    public override void WriteValue(IBufferWriter<byte> writer, bool value)
    {
        _uint8Type.WriteValue(writer, (byte)(value ? 1 : 0));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<bool> values)
    {
        if (values.IsEmpty)
            return;

        // Convert bools to bytes
        Span<byte> byteBuffer = stackalloc byte[Math.Min(values.Length, 4096)];
        var remaining = values.Length;
        var srcIndex = 0;

        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, byteBuffer.Length);
            var currentBytes = byteBuffer[..batchSize];

            // Convert batch of bools to bytes
            for (var i = 0; i < batchSize; i++)
            {
                currentBytes[i] = (byte)(values[srcIndex++] ? 1 : 0);
            }

            _uint8Type.WriteValues(writer, currentBytes);
            remaining -= batchSize;
        }
    }
}