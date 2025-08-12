using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse DateTime type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/datetime
/// 
/// Stores date-time values as 32-bit unsigned integers representing Unix timestamp (seconds since 1970-01-01 00:00:00 UTC).
/// Range: 1970-01-01 00:00:00 to 2106-02-07 06:28:15
/// 
/// This implementation provides:
/// - AVX512F: Processes 16 timestamps simultaneously (64 bytes) using 512-bit vectors  
/// - AVX2: Processes 8 timestamps simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 4 timestamps simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class DateTimeType(ISimdCapabilities simdCapabilities) : BaseClickHouseType<DateTime>
{
    public static readonly DateTimeType Instance = new();
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    
    // Shuffle masks for endianness conversion (32-bit values)
    private static readonly Vector128<byte> ShuffleMaskSse2 = Vector128.Create(
        (byte)3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12);
    private static readonly Vector256<byte> ShuffleMaskAvx2 = Vector256.Create(
        (byte)3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12,
        19, 18, 17, 16, 23, 22, 21, 20, 27, 26, 25, 24, 31, 30, 29, 28);
    private static readonly Vector512<byte> ShuffleMaskAvx512 = Vector512.Create(
        (byte)3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12,
        19, 18, 17, 16, 23, 22, 21, 20, 27, 26, 25, 24, 31, 30, 29, 28,
        35, 34, 33, 32, 39, 38, 37, 36, 43, 42, 41, 40, 47, 46, 45, 44,
        51, 50, 49, 48, 55, 54, 53, 52, 59, 58, 57, 56, 63, 62, 61, 60);
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public DateTimeType() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x11;
    public override string TypeName => "DateTime";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(uint);

    public override DateTime ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < sizeof(uint))
            throw new InvalidOperationException($"Insufficient data to read DateTime. Expected {sizeof(uint)} bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[sizeof(uint)];
        sequence.Slice(0, sizeof(uint)).CopyTo(buffer);
        sequence = sequence.Slice(sizeof(uint));
        bytesConsumed = sizeof(uint);

        var seconds = BitConverter.IsLittleEndian
            ? BinaryPrimitives.ReadUInt32LittleEndian(buffer)
            : BinaryPrimitives.ReadUInt32BigEndian(buffer);

        return UnixEpoch.AddSeconds(seconds);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<DateTime> destination, out int bytesConsumed)
    {
        var valuesRead = Math.Min(destination.Length, (int)(sequence.Length / sizeof(uint)));
        bytesConsumed = valuesRead * sizeof(uint);

        if (valuesRead == 0)
            return valuesRead;

        // Read raw uint values first
        Span<uint> rawValues = stackalloc uint[Math.Min(valuesRead, 512)];
        
        var remaining = valuesRead;
        var destIndex = 0;
        
        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, rawValues.Length);
            var currentRawValues = rawValues[..batchSize];
            var byteCount = batchSize * sizeof(uint);
            var sourceSequence = sequence.Slice(0, byteCount);

            if (sourceSequence.IsSingleSegment)
            {
                var sourceSpan = sourceSequence.FirstSpan;
                if (SimdCapabilities.IsAvx512FSupported && batchSize >= 16)
                    ReadValuesAvx512(sourceSpan, currentRawValues);
                else if (SimdCapabilities.IsAvx2Supported && batchSize >= 8)
                    ReadValuesAvx2(sourceSpan, currentRawValues);
                else if (SimdCapabilities.IsSse2Supported && batchSize >= 4)
                    ReadValuesSse2(sourceSpan, currentRawValues);
                else
                    ReadValuesScalar(sourceSpan, currentRawValues);
            }
            else
            {
                ReadValuesScalar(sourceSequence, currentRawValues);
            }

            // Convert uint seconds to DateTime
            for (var i = 0; i < batchSize; i++)
            {
                destination[destIndex++] = UnixEpoch.AddSeconds(currentRawValues[i]);
            }

            sequence = sequence.Slice(byteCount);
            remaining -= batchSize;
        }

        return valuesRead;
    }

    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<uint> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<byte, uint>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < destination.Length; i++)
            {
                destination[i] = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(i * sizeof(uint), sizeof(uint)));
            }
        }
    }

    private static void ReadValuesScalar(ReadOnlySequence<byte> source, Span<uint> destination)
    {
        Span<byte> buffer = stackalloc byte[sizeof(uint)];
        var reader = new SequenceReader<byte>(source);
        
        for (var i = 0; i < destination.Length; i++)
        {
            reader.TryCopyTo(buffer);
            reader.Advance(sizeof(uint));
            destination[i] = BitConverter.IsLittleEndian
                ? BinaryPrimitives.ReadUInt32LittleEndian(buffer)
                : BinaryPrimitives.ReadUInt32BigEndian(buffer);
        }
    }

    private static unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<uint> destination)
    {
        var vectorCount = destination.Length / 4;
        var remainder = destination.Length % 4;

        fixed (byte* srcPtr = source)
        fixed (uint* dstPtr = destination)
        {
            var src = srcPtr;
            var dst = dstPtr;

            for (var i = 0; i < vectorCount; i++)
            {
                var vector = Sse2.LoadVector128(src);
                if (!BitConverter.IsLittleEndian)
                    vector = Ssse3.Shuffle(vector, ShuffleMaskSse2);
                Sse2.Store((byte*)dst, vector);
                src += 16;
                dst += 4;
            }

            if (remainder > 0)
            {
                ReadValuesScalar(source[(vectorCount * 16)..], destination.Slice(vectorCount * 4, remainder));
            }
        }
    }

    private static unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<uint> destination)
    {
        var vectorCount = destination.Length / 8;
        var remainder = destination.Length % 8;

        fixed (byte* srcPtr = source)
        fixed (uint* dstPtr = destination)
        {
            var src = srcPtr;
            var dst = dstPtr;

            for (var i = 0; i < vectorCount; i++)
            {
                var vector = Avx.LoadVector256(src);
                if (!BitConverter.IsLittleEndian)
                    vector = Avx2.Shuffle(vector, ShuffleMaskAvx2);
                Avx.Store((byte*)dst, vector);
                src += 32;
                dst += 8;
            }

            if (remainder > 0)
            {
                var processed = vectorCount * 8;
                if (remainder >= 4 && Sse2.IsSupported)
                    ReadValuesSse2(source[(processed * sizeof(uint))..], destination.Slice(processed, remainder));
                else
                    ReadValuesScalar(source[(processed * sizeof(uint))..], destination.Slice(processed, remainder));
            }
        }
    }

    private static unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<uint> destination)
    {
        var vectorCount = destination.Length / 16;
        var remainder = destination.Length % 16;

        fixed (byte* srcPtr = source)
        fixed (uint* dstPtr = destination)
        {
            var src = srcPtr;
            var dst = dstPtr;

            for (var i = 0; i < vectorCount; i++)
            {
                var vector = Avx512F.LoadVector512(src);
                if (!BitConverter.IsLittleEndian)
                    vector = Avx512BW.Shuffle(vector, ShuffleMaskAvx512);
                Avx512F.Store((byte*)dst, vector);
                src += 64;
                dst += 16;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 16;
            switch (remainder)
            {
                case >= 8 when Avx2.IsSupported:
                    ReadValuesAvx2(source[(processed * sizeof(uint))..], destination.Slice(processed, remainder));
                    break;
                case >= 4 when Sse2.IsSupported:
                    ReadValuesSse2(source[(processed * sizeof(uint))..], destination.Slice(processed, remainder));
                    break;
                default:
                    ReadValuesScalar(source[(processed * sizeof(uint))..], destination.Slice(processed, remainder));
                    break;
            }
        }
    }

    public override void WriteValue(IBufferWriter<byte> writer, DateTime value)
    {
        var span = writer.GetSpan(sizeof(uint));
        var seconds = (uint)(value.ToUniversalTime() - UnixEpoch).TotalSeconds;
        
        if (BitConverter.IsLittleEndian)
            BinaryPrimitives.WriteUInt32LittleEndian(span, seconds);
        else
            BinaryPrimitives.WriteUInt32BigEndian(span, seconds);
        writer.Advance(sizeof(uint));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<DateTime> values)
    {
        if (values.IsEmpty)
            return;

        // Convert DateTime to uint seconds first
        Span<uint> rawValues = stackalloc uint[Math.Min(values.Length, 512)];
        
        var remaining = values.Length;
        var srcIndex = 0;
        
        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, rawValues.Length);
            var currentRawValues = rawValues[..batchSize];
            
            // Convert batch of DateTime to uint
            for (var i = 0; i < batchSize; i++)
            {
                currentRawValues[i] = (uint)(values[srcIndex++].ToUniversalTime() - UnixEpoch).TotalSeconds;
            }

            var byteCount = batchSize * sizeof(uint);
            var span = writer.GetSpan(byteCount);

            if (SimdCapabilities.IsAvx512FSupported && batchSize >= 16)
                WriteValuesAvx512(currentRawValues, span);
            else if (SimdCapabilities.IsAvx2Supported && batchSize >= 8)
                WriteValuesAvx2(currentRawValues, span);
            else if (SimdCapabilities.IsSse2Supported && batchSize >= 4)
                WriteValuesSse2(currentRawValues, span);
            else
                WriteValuesScalar(currentRawValues, span);

            writer.Advance(byteCount);
            remaining -= batchSize;
        }
    }

    private static void WriteValuesScalar(ReadOnlySpan<uint> source, Span<byte> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<uint, byte>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < source.Length; i++)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(i * sizeof(uint), sizeof(uint)), source[i]);
            }
        }
    }

    private static unsafe void WriteValuesSse2(ReadOnlySpan<uint> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 4;
        var remainder = source.Length % 4;

        fixed (uint* srcPtr = source)
        fixed (byte* dstPtr = destination)
        {
            var src = srcPtr;
            var dst = dstPtr;

            for (var i = 0; i < vectorCount; i++)
            {
                var vector = Sse2.LoadVector128((byte*)src);
                if (!BitConverter.IsLittleEndian)
                    vector = Ssse3.Shuffle(vector, ShuffleMaskSse2);
                Sse2.Store(dst, vector);
                src += 4;
                dst += 16;
            }

            if (remainder > 0)
            {
                WriteValuesScalar(source.Slice(vectorCount * 4, remainder), destination[(vectorCount * 16)..]);
            }
        }
    }

    private static unsafe void WriteValuesAvx2(ReadOnlySpan<uint> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 8;
        var remainder = source.Length % 8;

        fixed (uint* srcPtr = source)
        fixed (byte* dstPtr = destination)
        {
            var src = srcPtr;
            var dst = dstPtr;

            for (var i = 0; i < vectorCount; i++)
            {
                var vector = Avx.LoadVector256((byte*)src);
                if (!BitConverter.IsLittleEndian)
                    vector = Avx2.Shuffle(vector, ShuffleMaskAvx2);
                Avx.Store(dst, vector);
                src += 8;
                dst += 32;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 8;
            if (remainder >= 4 && Sse2.IsSupported)
                WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(uint))..]);
            else
                WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(uint))..]);
        }
    }

    private static unsafe void WriteValuesAvx512(ReadOnlySpan<uint> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 16;
        var remainder = source.Length % 16;

        fixed (uint* srcPtr = source)
        fixed (byte* dstPtr = destination)
        {
            var src = srcPtr;
            var dst = dstPtr;

            for (var i = 0; i < vectorCount; i++)
            {
                var vector = Avx512F.LoadVector512((byte*)src);
                if (!BitConverter.IsLittleEndian)
                    vector = Avx512BW.Shuffle(vector, ShuffleMaskAvx512);
                Avx512F.Store(dst, vector);
                src += 16;
                dst += 64;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 16;
            switch (remainder)
            {
                case >= 8 when Avx2.IsSupported:
                    WriteValuesAvx2(source.Slice(processed, remainder), destination[(processed * sizeof(uint))..]);
                    break;
                case >= 4 when Sse2.IsSupported:
                    WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(uint))..]);
                    break;
                default:
                    WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(uint))..]);
                    break;
            }
        }
    }
}