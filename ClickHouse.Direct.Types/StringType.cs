using System.Buffers;
using System.Text;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse String type handler for arbitrary-length UTF-8 encoded strings.
///
/// https://clickhouse.com/docs/sql-reference/data-types/string
/// 
/// The String data type stores strings of arbitrary length as a sequence of bytes. 
/// ClickHouse does not restrict the character set or enforce UTF-8 validation, though UTF-8 is the standard encoding.
/// 
/// Wire format:
/// - Length prefix: Variable-length integer (LEB128/varint) encoding the byte length
/// - String data: Raw UTF-8 bytes (or any byte sequence)
/// 
/// This implementation provides:
/// - LEB128 varint encoding for length prefix (1-10 bytes depending on string length)
/// - UTF-8 encoding/decoding for .NET string conversion
/// - Optimized single-segment fast path for contiguous memory
/// - Stack allocation for small strings (≤1024 bytes) to avoid heap allocations
/// </summary>
public sealed class StringType : BaseClickHouseType<string>
{
    public static readonly StringType Instance = new();

    public override ClickHouseDataType DataType => ClickHouseDataType.String;
    public override string TypeName => "String";
    public override bool IsFixedLength => false;
    public override int FixedByteLength => -1;

    public override string ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        // Read the length as varint
        var length = ReadVarint(ref sequence, out var varintBytes);
        
        if (length == 0)
        {
            bytesConsumed = varintBytes;
            return string.Empty;
        }

        if (sequence.Length < (long)length)
            throw new InvalidOperationException($"Insufficient data: expected {length} bytes for string content, got {sequence.Length}");

        string result;
        if (sequence.First.Length >= (int)length)
        {
            // Fast path: string data is in a single segment
            result = Encoding.UTF8.GetString(sequence.First.Span[..(int)length]);
        }
        else
        {
            // Slow path: string data spans multiple segments
            var buffer = length <= 1024 
                ? stackalloc byte[(int)length] 
                : new byte[length];
            sequence.Slice(0, (long)length).CopyTo(buffer);
            result = Encoding.UTF8.GetString(buffer);
        }

        sequence = sequence.Slice((long)length);
        bytesConsumed = varintBytes + (int)length;
        return result;
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<string> destination, out int bytesConsumed)
    {
        bytesConsumed = 0;
        var itemsRead = 0;

        for (var i = 0; i < destination.Length && sequence.Length > 0; i++)
        {
            destination[i] = ReadValue(ref sequence, out var consumed);
            bytesConsumed += consumed;
            itemsRead++;
        }

        return itemsRead;
    }

    public override void WriteValue(IBufferWriter<byte> writer, string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            WriteVarint(writer, 0);
            return;
        }

        var utf8Bytes = Encoding.UTF8.GetByteCount(value);
        WriteVarint(writer, (ulong)utf8Bytes);

        var span = writer.GetSpan(utf8Bytes);
        var actualBytes = Encoding.UTF8.GetBytes(value, span);
        writer.Advance(actualBytes);
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<string> values)
    {
        foreach (var value in values)
        {
            WriteValue(writer, value);
        }
    }

    private static ulong ReadVarint(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        ulong result = 0;
        var shift = 0;
        bytesConsumed = 0;

        while (sequence.Length > 0)
        {
            var b = sequence.First.Span[0];
            sequence = sequence.Slice(1);
            bytesConsumed++;

            result |= (ulong)(b & 0x7F) << shift;
            
            if ((b & 0x80) == 0)
                break;
                
            shift += 7;
            if (shift >= 64)
                throw new InvalidOperationException("Varint is too long");
        }

        return result;
    }

    private static void WriteVarint(IBufferWriter<byte> writer, ulong value)
    {
        while (value >= 0x80)
        {
            var span = writer.GetSpan(1);
            span[0] = (byte)(value | 0x80);
            writer.Advance(1);
            value >>= 7;
        }

        var finalSpan = writer.GetSpan(1);
        finalSpan[0] = (byte)value;
        writer.Advance(1);
    }
}