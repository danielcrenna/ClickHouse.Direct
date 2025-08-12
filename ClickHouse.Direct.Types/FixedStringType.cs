using System.Buffers;
using System.Text;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse FixedString(N) type handler.
///
/// https://clickhouse.com/docs/sql-reference/data-types/fixedstring
/// 
/// Stores strings of fixed byte length N. Strings shorter than N bytes are padded with null bytes.
/// Strings longer than N bytes trigger an error.
/// 
/// Unlike String type, FixedString doesn't use length prefixes - it always stores exactly N bytes.
/// This type is efficient for storing data that has a constant or maximum length.
/// </summary>
public sealed class FixedStringType : BaseClickHouseType<string>
{
    public int Length { get; }

    public FixedStringType(int length)
    {
        if (length <= 0)
            throw new ArgumentOutOfRangeException(nameof(length), length, "FixedString length must be positive");
        if (length > 1_000_000) // Reasonable maximum to prevent memory issues
            throw new ArgumentOutOfRangeException(nameof(length), length, "FixedString length is too large");
            
        Length = length;
    }

    public static FixedStringType Create(int length) => new(length);

    public override byte ProtocolCode => 0x16;
    public override string TypeName => $"FixedString({Length})";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => Length;

    public override string ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < Length)
            throw new InvalidOperationException($"Insufficient data to read FixedString({Length}). Expected {Length} bytes, got {sequence.Length}");

        // Read exactly N bytes
        var buffer = Length <= 1024 ? stackalloc byte[Length] : new byte[Length];
        sequence.Slice(0, Length).CopyTo(buffer);
        sequence = sequence.Slice(Length);
        bytesConsumed = Length;

        // Find the actual string length (trim null padding)
        var actualLength = buffer.Length;
        for (var i = buffer.Length - 1; i >= 0; i--)
        {
            if (buffer[i] != 0)
            {
                actualLength = i + 1;
                break;
            }
            if (i == 0)
            {
                actualLength = 0; // All null bytes
            }
        }

        // Convert to string (UTF-8)
        return actualLength == 0 
            ? string.Empty 
            : Encoding.UTF8.GetString(buffer[..actualLength]);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<string> destination, out int bytesConsumed)
    {
        var valuesRead = Math.Min(destination.Length, (int)(sequence.Length / Length));
        bytesConsumed = valuesRead * Length;

        if (valuesRead == 0)
            return valuesRead;

        // Read values
        var buffer = Length <= 1024 ? stackalloc byte[Length] : new byte[Length];
        
        for (var i = 0; i < valuesRead; i++)
        {
            sequence.Slice(0, Length).CopyTo(buffer);
            sequence = sequence.Slice(Length);
            
            // Find actual string length
            var actualLength = buffer.Length;
            for (var j = buffer.Length - 1; j >= 0; j--)
            {
                if (buffer[j] != 0)
                {
                    actualLength = j + 1;
                    break;
                }
                if (j == 0)
                {
                    actualLength = 0;
                }
            }
            
            destination[i] = actualLength == 0 
                ? string.Empty 
                : Encoding.UTF8.GetString(buffer[..actualLength]);
        }

        return valuesRead;
    }

    public override void WriteValue(IBufferWriter<byte> writer, string value)
    {
        var span = writer.GetSpan(Length);
        
        // Clear the buffer (fill with nulls)
        span[..Length].Clear();
        
        if (!string.IsNullOrEmpty(value))
        {
            // Get UTF-8 bytes
            var byteCount = Encoding.UTF8.GetByteCount(value);
            if (byteCount > Length)
                throw new ArgumentException($"String is too long for FixedString({Length}). String requires {byteCount} bytes", nameof(value));
            
            // Write the string bytes
            Encoding.UTF8.GetBytes(value, span);
        }
        
        writer.Advance(Length);
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<string> values)
    {
        if (values.IsEmpty)
            return;

        // Process each value
        for (var i = 0; i < values.Length; i++)
        {
            WriteValue(writer, values[i]);
        }
    }
    
    /// <summary>
    /// Creates a FixedStringType instance from a type name string like "FixedString(10)".
    /// </summary>
    public static FixedStringType? ParseTypeName(string typeName)
    {
        if (!typeName.StartsWith("FixedString(", StringComparison.OrdinalIgnoreCase))
            return null;
            
        var endIndex = typeName.IndexOf(')', 12);
        if (endIndex == -1)
            return null;
            
        var lengthStr = typeName.Substring(12, endIndex - 12);
        if (!int.TryParse(lengthStr, out var length))
            return null;
            
        return new FixedStringType(length);
    }
}