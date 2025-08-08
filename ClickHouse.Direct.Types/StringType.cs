using System.Buffers;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse String type handler for arbitrary-length UTF-8 encoded strings with SIMD-optimized processing.
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
/// - AVX2: SIMD ASCII validation for large strings (32 bytes at once) using TestZ operations
/// - SSE2: SIMD ASCII-to-Unicode conversion (16 bytes to 16 chars) using vector unpacking
/// - Optimized varint encoding/decoding with fast paths for common lengths (1-2 bytes)
/// - Batch processing for small ASCII strings to reduce method call overhead
/// - Single-segment fast path for contiguous memory sequences
/// - Stack allocation for small strings (≤1024 bytes) to avoid heap allocations
/// - Automatic fallback to scalar operations on unsupported hardware
/// </summary>
public sealed class StringType(ISimdCapabilities simdCapabilities) : BaseClickHouseType<string>
{
    public static readonly StringType Instance = new();
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public StringType() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x15;
    public override string TypeName => "String";
    public override bool IsFixedLength => false;
    public override int FixedByteLength => -1;


    public override string ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
        => ReadValueInternal(ref sequence, out bytesConsumed);

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<string> destination, out int bytesConsumed)
    {
        bytesConsumed = 0;
        var itemsRead = 0;

        // Check if we can use SIMD for small fixed-length strings
        // For variable-length strings, we need to process sequentially due to varint prefixes
        // However, we can optimize the varint reading and UTF-8 decoding
        
        for (var i = 0; i < destination.Length && sequence.Length > 0; i++)
        {
            destination[i] = ReadValueInternal(ref sequence, out var consumed);
            bytesConsumed += consumed;
            itemsRead++;
        }

        return itemsRead;
    }
    
    private string ReadValueInternal(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
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
            // For larger strings, we can use SIMD-accelerated UTF-8 validation
            result = DecodeUtf8Optimized(sequence.First.Span[..(int)length]);
        }
        else
        {
            // Slow path: string data spans multiple segments
            var buffer = length <= 1024 
                ? stackalloc byte[(int)length] 
                : new byte[length];
            sequence.Slice(0, (long)length).CopyTo(buffer);
            result = DecodeUtf8Optimized(buffer);
        }

        sequence = sequence.Slice((long)length);
        bytesConsumed = varintBytes + (int)length;
        return result;
    }
    
    private string DecodeUtf8Optimized(ReadOnlySpan<byte> utf8Bytes)
    {
        // Use SIMD-optimized memory copy for larger strings
        if (utf8Bytes.Length >= 64 && SimdCapabilities.IsAvx2Supported)
        {
            return DecodeUtf8WithSimdCopy(utf8Bytes);
        }
        
        return Encoding.UTF8.GetString(utf8Bytes);
    }
    
    private string DecodeUtf8WithSimdCopy(ReadOnlySpan<byte> utf8Bytes)
    {
        // For ASCII-heavy content, we can validate and copy faster with SIMD
        // Check if string is ASCII-only using SIMD
        if (IsAsciiOnlySimd(utf8Bytes))
        {
            // Fast ASCII-only path
            return CreateAsciiStringFast(utf8Bytes);
        }
        
        // Fallback to standard UTF-8 decoding
        return Encoding.UTF8.GetString(utf8Bytes);
    }
    
    private unsafe bool IsAsciiOnlySimd(ReadOnlySpan<byte> bytes)
    {
        if (!SimdCapabilities.IsAvx2Supported || bytes.Length < 32)
            return IsAsciiOnlyScalar(bytes);
        
        fixed (byte* ptr = bytes)
        {
            var i = 0;
            var len = bytes.Length;
            
            // Process 32 bytes at a time with AVX2
            for (; i <= len - 32; i += 32)
            {
                var vec = Avx.LoadVector256(ptr + i);
                // Check if any byte has the high bit set (non-ASCII)
                if (!Avx.TestZ(vec, Vector256.Create((byte)0x80)))
                    return false;
            }
            
            // Handle remaining bytes
            for (; i < len; i++)
            {
                if (ptr[i] >= 128)
                    return false;
            }
        }
        
        return true;
    }
    
    private static bool IsAsciiOnlyScalar(ReadOnlySpan<byte> bytes)
    {
        foreach (byte b in bytes)
        {
            if (b >= 128)
                return false;
        }
        return true;
    }
    
    private unsafe string CreateAsciiStringFast(ReadOnlySpan<byte> asciiBytes)
    {
        // Fast path for ASCII: direct byte-to-char conversion with SIMD
        var result = new string('\0', asciiBytes.Length);
        
        fixed (byte* src = asciiBytes)
        fixed (char* dest = result)
        {
            var i = 0;
            var len = asciiBytes.Length;
            
            if (SimdCapabilities.IsSse2Supported && len >= 16)
            {
                // Convert bytes to chars using SSE2 (16 bytes to 16 chars)
                for (; i <= len - 16; i += 16)
                {
                    var bytes128 = Sse2.LoadVector128(src + i);
                    var chars256Lo = Sse2.UnpackLow(bytes128, Vector128<byte>.Zero).AsInt16();
                    var chars256Hi = Sse2.UnpackHigh(bytes128, Vector128<byte>.Zero).AsInt16();
                    
                    Sse2.Store((short*)(dest + i), chars256Lo);
                    Sse2.Store((short*)(dest + i + 8), chars256Hi);
                }
            }
            
            // Handle remaining bytes
            for (; i < len; i++)
            {
                dest[i] = (char)src[i];
            }
        }
        
        return result;
    }
    
    private ulong ReadVarint(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        bytesConsumed = 0;
        
        if (sequence.Length == 0)
            throw new InvalidOperationException("No data available for varint");
            
        // Fast path for single-segment sequences with optimized small value handling
        if (sequence.First.Length >= 1)
        {
            var span = sequence.First.Span;
            var firstByte = span[0];
            
            // Most common case: single byte varint (value < 128)
            if ((firstByte & 0x80) == 0)
            {
                bytesConsumed = 1;
                sequence = sequence.Slice(1);
                return firstByte;
            }
            
            // Two byte case (common for string lengths)
            if (span.Length >= 2)
            {
                var secondByte = span[1];
                if ((secondByte & 0x80) == 0)
                {
                    bytesConsumed = 2;
                    sequence = sequence.Slice(2);
                    var low = (ulong)(firstByte & 0x7F);
                    var high = (ulong)secondByte << 7;
                    return low | high;
                }
            }
            
            // General case for longer varints
            if (span.Length >= 5) // Handle up to 5 bytes efficiently
            {
                return ReadVarintFromSpan(span, ref sequence);
            }
        }
        
        // Fallback to segment-by-segment reading
        return ReadVarintSlow(ref sequence, out bytesConsumed);
    }
    
    private static ulong ReadVarintFromSpan(ReadOnlySpan<byte> span, ref ReadOnlySequence<byte> sequence)
    {
        ulong result = 0;
        var shift = 0;
        var consumed = 0;
        
        for (var i = 0; i < Math.Min(span.Length, 10); i++)
        {
            var b = span[i];
            consumed++;
            
            result |= (ulong)(b & 0x7F) << shift;
            
            if ((b & 0x80) == 0)
            {
                sequence = sequence.Slice(consumed);
                return result;
            }
            
            shift += 7;
            if (shift >= 64)
                throw new InvalidOperationException("Varint is too long");
        }
        
        throw new InvalidOperationException("Incomplete varint");
    }
    
    private ulong ReadVarintSlow(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
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


    public override void WriteValue(IBufferWriter<byte> writer, string value)
        => WriteValueInternal(writer, value);

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<string> values)
    {
        if (values.IsEmpty)
            return;
            
        // Try to batch small ASCII strings for better SIMD utilization
        if (SimdCapabilities.IsAvx2Supported && ShouldUseBatchEncoding(values))
        {
            WriteValuesBatched(writer, values);
        }
        else
        {
            // Standard path with individual optimizations
            foreach (var value in values)
            {
                WriteValueInternal(writer, value);
            }
        }
    }
    
    private bool ShouldUseBatchEncoding(ReadOnlySpan<string> values)
    {
        // Use batching for small to medium ASCII strings
        if (values.Length < 4)
            return false;
            
        var totalLength = 0;
        var asciiCount = 0;
        
        foreach (var str in values)
        {
            totalLength += str.Length;
            if (totalLength > 1024) // Don't batch very large sets
                return false;
                
            if (str.Length <= 64 && IsAsciiOnlyString(str))
                asciiCount++;
        }
        
        return asciiCount >= values.Length * 0.7; // 70% ASCII strings
    }
    
    private void WriteValuesBatched(IBufferWriter<byte> writer, ReadOnlySpan<string> values)
    {
        // Estimate total size needed
        var estimatedSize = 0;
        foreach (var str in values)
        {
            estimatedSize += str.Length + 2; // +2 for varint overhead
        }
        
        var span = writer.GetSpan(estimatedSize);
        var pos = 0;
        
        foreach (var value in values)
        {
            if (string.IsNullOrEmpty(value))
            {
                span[pos++] = 0; // varint 0
                continue;
            }
            
            // Write varint length
            var utf8Length = value.Length; // ASCII only
            if (utf8Length < 0x80)
            {
                span[pos++] = (byte)utf8Length;
            }
            else if (utf8Length < 0x4000)
            {
                span[pos++] = (byte)(utf8Length | 0x80);
                span[pos++] = (byte)(utf8Length >> 7);
            }
            else
            {
                // Fallback to individual encoding for longer strings
                writer.Advance(pos);
                WriteValueInternal(writer, value);
                var newSpan = writer.GetSpan(estimatedSize - pos);
                span = newSpan;
                pos = 0;
                continue;
            }
            
            // Copy ASCII bytes directly
            foreach (var b in value)
            {
                span[pos++] = (byte)b;
            }
        }
        
        writer.Advance(pos);
    }
    
    private void WriteValueInternal(IBufferWriter<byte> writer, string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            WriteVarint(writer, 0);
            return;
        }

        var utf8Bytes = Encoding.UTF8.GetByteCount(value);
        WriteVarint(writer, (ulong)utf8Bytes);

        // Use optimized UTF-8 encoding for larger strings
        if (utf8Bytes > 128 && SimdCapabilities.IsSse2Supported)
        {
            // For larger strings, get a larger span to reduce overhead
            var span = writer.GetSpan(utf8Bytes);
            var actualBytes = EncodeUtf8Optimized(value, span);
            writer.Advance(actualBytes);
        }
        else
        {
            // Standard path for smaller strings
            var span = writer.GetSpan(utf8Bytes);
            var actualBytes = Encoding.UTF8.GetBytes(value, span);
            writer.Advance(actualBytes);
        }
    }
    
    private int EncodeUtf8Optimized(string value, Span<byte> destination)
    {
        // Fast path for ASCII-only strings
        if (IsAsciiOnlyString(value) && SimdCapabilities.IsAvx2Supported && value.Length >= 16)
        {
            return EncodeAsciiStringFast(value, destination);
        }
        
        return Encoding.UTF8.GetBytes(value, destination);
    }
    
    private static bool IsAsciiOnlyString(string str)
    {
        foreach (char c in str)
        {
            if (c >= 128)
                return false;
        }
        return true;
    }
    
    private unsafe int EncodeAsciiStringFast(string value, Span<byte> destination)
    {
        fixed (char* src = value)
        fixed (byte* dest = destination)
        {
            var i = 0;
            var len = value.Length;
            
            // Convert chars to bytes using AVX2 (16 chars to 16 bytes)
            for (; i <= len - 16; i += 16)
            {
                var chars256Lo = Avx.LoadVector256((short*)(src + i));
                var chars256Hi = Avx.LoadVector256((short*)(src + i + 8));
                
                // Pack chars to bytes (truncate to lower 8 bits)
                var packed = Avx2.PackUnsignedSaturate(chars256Lo, chars256Hi);
                Avx.Store(dest + i, packed);
            }
            
            // Handle remaining chars
            for (; i < len; i++)
            {
                dest[i] = (byte)src[i];
            }
        }
        
        return value.Length;
    }
    
    private void WriteVarint(IBufferWriter<byte> writer, ulong value)
    {
        // Fast paths for common small values
        if (value < 0x80)
        {
            var writeSpan = writer.GetSpan(1);
            writeSpan[0] = (byte)value;
            writer.Advance(1);
            return;
        }
        
        if (value < 0x4000) // 2 bytes
        {
            var writeSpan2 = writer.GetSpan(2);
            writeSpan2[0] = (byte)(value | 0x80);
            writeSpan2[1] = (byte)(value >> 7);
            writer.Advance(2);
            return;
        }
        
        // General case - encode to buffer then copy
        Span<byte> buffer = stackalloc byte[10];
        var pos = 0;
        
        while (value >= 0x80)
        {
            buffer[pos++] = (byte)(value | 0x80);
            value >>= 7;
        }
        buffer[pos++] = (byte)value;
        
        var writeSpanFinal = writer.GetSpan(pos);
        buffer[..pos].CopyTo(writeSpanFinal);
        writer.Advance(pos);
    }

}