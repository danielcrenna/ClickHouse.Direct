using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse DateTime64 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/datetime64
/// 
/// Stores date-time values as 64-bit integers with configurable sub-second precision.
/// The value represents ticks since Unix epoch (1970-01-01 00:00:00 UTC).
/// Precision parameter (0-9) determines the tick resolution:
/// - 0: seconds (10^0)
/// - 3: milliseconds (10^3)
/// - 6: microseconds (10^6)
/// - 9: nanoseconds (10^9)
/// 
/// This implementation provides:
/// - AVX512F: Processes 8 timestamps simultaneously (64 bytes) using 512-bit vectors  
/// - AVX2: Processes 4 timestamps simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 2 timestamps simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class DateTime64Type : BaseClickHouseType<DateTime>
{
    // Default precision is 3 (milliseconds) to match .NET DateTime precision
    public const byte DefaultPrecision = 3;
    
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    
    // Shuffle masks for endianness conversion (64-bit values)
    private static readonly Vector128<byte> ShuffleMaskSse2 = Vector128.Create(
        (byte)7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
    private static readonly Vector256<byte> ShuffleMaskAvx2 = Vector256.Create(
        (byte)7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
        23, 22, 21, 20, 19, 18, 17, 16, 31, 30, 29, 28, 27, 26, 25, 24);
    private static readonly Vector512<byte> ShuffleMaskAvx512 = Vector512.Create(
        (byte)7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
        23, 22, 21, 20, 19, 18, 17, 16, 31, 30, 29, 28, 27, 26, 25, 24,
        39, 38, 37, 36, 35, 34, 33, 32, 47, 46, 45, 44, 43, 42, 41, 40,
        55, 54, 53, 52, 51, 50, 49, 48, 63, 62, 61, 60, 59, 58, 57, 56);
    
    // Pre-calculated scale factors for each precision level
    private static readonly long[] ScaleFactors =
    [
        1L,                    // 0: seconds
        10L,                   // 1: deciseconds
        100L,                  // 2: centiseconds
        1_000L,                // 3: milliseconds
        10_000L,               // 4: 10 microseconds
        100_000L,              // 5: 100 microseconds
        1_000_000L,            // 6: microseconds
        10_000_000L,           // 7: 100 nanoseconds (DateTime ticks)
        100_000_000L,          // 8: 10 nanoseconds
        1_000_000_000L         // 9: nanoseconds
    ];
    
    public ISimdCapabilities SimdCapabilities { get; }
    public byte Precision { get; }
    private readonly long _scaleFactor;
    private readonly double _inverseScaleFactor;

    public DateTime64Type(byte precision = DefaultPrecision, ISimdCapabilities? simdCapabilities = null)
    {
        if (precision > 9)
            throw new ArgumentOutOfRangeException(nameof(precision), precision, "Precision must be between 0 and 9");
            
        Precision = precision;
        _scaleFactor = ScaleFactors[precision];
        _inverseScaleFactor = 1.0 / _scaleFactor;
        SimdCapabilities = simdCapabilities ?? DefaultSimdCapabilities.Instance;
    }

    public static DateTime64Type CreateWithPrecision(byte precision) => new(precision);
    public static readonly DateTime64Type Instance = new();

    public override byte ProtocolCode => 0x19;
    public override string TypeName => $"DateTime64({Precision})";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(long);

    public override DateTime ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < sizeof(long))
            throw new InvalidOperationException($"Insufficient data to read DateTime64. Expected {sizeof(long)} bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[sizeof(long)];
        sequence.Slice(0, sizeof(long)).CopyTo(buffer);
        sequence = sequence.Slice(sizeof(long));
        bytesConsumed = sizeof(long);

        var ticks = BitConverter.IsLittleEndian
            ? BinaryPrimitives.ReadInt64LittleEndian(buffer)
            : BinaryPrimitives.ReadInt64BigEndian(buffer);

        // Convert from ClickHouse ticks to .NET DateTime
        var seconds = ticks / _scaleFactor;
        var subSecondTicks = ticks % _scaleFactor;
        var dateTimeTicks = subSecondTicks * TimeSpan.TicksPerSecond / _scaleFactor;
        
        return UnixEpoch.AddSeconds(seconds).AddTicks(dateTimeTicks);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<DateTime> destination, out int bytesConsumed)
    {
        var valuesRead = Math.Min(destination.Length, (int)(sequence.Length / sizeof(long)));
        bytesConsumed = valuesRead * sizeof(long);

        if (valuesRead == 0)
            return valuesRead;

        // Read raw long values first
        Span<long> rawValues = stackalloc long[Math.Min(valuesRead, 256)];
        
        var remaining = valuesRead;
        var destIndex = 0;
        
        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, rawValues.Length);
            var currentRawValues = rawValues[..batchSize];
            var byteCount = batchSize * sizeof(long);
            var sourceSequence = sequence.Slice(0, byteCount);

            if (sourceSequence.IsSingleSegment)
            {
                var sourceSpan = sourceSequence.FirstSpan;
                if (SimdCapabilities.IsAvx512FSupported && batchSize >= 8)
                    ReadValuesAvx512(sourceSpan, currentRawValues);
                else if (SimdCapabilities.IsAvx2Supported && batchSize >= 4)
                    ReadValuesAvx2(sourceSpan, currentRawValues);
                else if (SimdCapabilities.IsSse2Supported && batchSize >= 2)
                    ReadValuesSse2(sourceSpan, currentRawValues);
                else
                    ReadValuesScalar(sourceSpan, currentRawValues);
            }
            else
            {
                ReadValuesScalar(sourceSequence, currentRawValues);
            }

            // Convert long ticks to DateTime
            for (var i = 0; i < batchSize; i++)
            {
                var ticks = currentRawValues[i];
                var seconds = ticks / _scaleFactor;
                var subSecondTicks = ticks % _scaleFactor;
                var dateTimeTicks = subSecondTicks * TimeSpan.TicksPerSecond / _scaleFactor;
                destination[destIndex++] = UnixEpoch.AddSeconds(seconds).AddTicks(dateTimeTicks);
            }

            sequence = sequence.Slice(byteCount);
            remaining -= batchSize;
        }

        return valuesRead;
    }

    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<long> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<byte, long>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < destination.Length; i++)
            {
                destination[i] = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(i * sizeof(long), sizeof(long)));
            }
        }
    }

    private static void ReadValuesScalar(ReadOnlySequence<byte> source, Span<long> destination)
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        var reader = new SequenceReader<byte>(source);
        
        for (var i = 0; i < destination.Length; i++)
        {
            reader.TryCopyTo(buffer);
            reader.Advance(sizeof(long));
            destination[i] = BitConverter.IsLittleEndian
                ? BinaryPrimitives.ReadInt64LittleEndian(buffer)
                : BinaryPrimitives.ReadInt64BigEndian(buffer);
        }
    }

    private static unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<long> destination)
    {
        var vectorCount = destination.Length / 2;
        var remainder = destination.Length % 2;

        fixed (byte* srcPtr = source)
        fixed (long* dstPtr = destination)
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
                dst += 2;
            }

            if (remainder > 0)
            {
                ReadValuesScalar(source[(vectorCount * 16)..], destination.Slice(vectorCount * 2, remainder));
            }
        }
    }

    private static unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<long> destination)
    {
        var vectorCount = destination.Length / 4;
        var remainder = destination.Length % 4;

        fixed (byte* srcPtr = source)
        fixed (long* dstPtr = destination)
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
                dst += 4;
            }

            if (remainder > 0)
            {
                var processed = vectorCount * 4;
                if (remainder >= 2 && Sse2.IsSupported)
                    ReadValuesSse2(source[(processed * sizeof(long))..], destination.Slice(processed, remainder));
                else
                    ReadValuesScalar(source[(processed * sizeof(long))..], destination.Slice(processed, remainder));
            }
        }
    }

    private static unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<long> destination)
    {
        var vectorCount = destination.Length / 8;
        var remainder = destination.Length % 8;

        fixed (byte* srcPtr = source)
        fixed (long* dstPtr = destination)
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
                dst += 8;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 8;
            switch (remainder)
            {
                case >= 4 when Avx2.IsSupported:
                    ReadValuesAvx2(source[(processed * sizeof(long))..], destination.Slice(processed, remainder));
                    break;
                case >= 2 when Sse2.IsSupported:
                    ReadValuesSse2(source[(processed * sizeof(long))..], destination.Slice(processed, remainder));
                    break;
                default:
                    ReadValuesScalar(source[(processed * sizeof(long))..], destination.Slice(processed, remainder));
                    break;
            }
        }
    }

    public override void WriteValue(IBufferWriter<byte> writer, DateTime value)
    {
        var span = writer.GetSpan(sizeof(long));
        
        // Convert from .NET DateTime to ClickHouse ticks
        var totalSeconds = (long)(value.ToUniversalTime() - UnixEpoch).TotalSeconds;
        var subSecondTicks = value.Ticks % TimeSpan.TicksPerSecond;
        var clickHouseTicks = totalSeconds * _scaleFactor + (subSecondTicks * _scaleFactor / TimeSpan.TicksPerSecond);
        
        if (BitConverter.IsLittleEndian)
            BinaryPrimitives.WriteInt64LittleEndian(span, clickHouseTicks);
        else
            BinaryPrimitives.WriteInt64BigEndian(span, clickHouseTicks);
        writer.Advance(sizeof(long));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<DateTime> values)
    {
        if (values.IsEmpty)
            return;

        // Convert DateTime to long ticks first
        Span<long> rawValues = stackalloc long[Math.Min(values.Length, 256)];
        
        var remaining = values.Length;
        var srcIndex = 0;
        
        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, rawValues.Length);
            var currentRawValues = rawValues[..batchSize];
            
            // Convert batch of DateTime to long
            for (var i = 0; i < batchSize; i++)
            {
                var value = values[srcIndex++];
                var totalSeconds = (long)(value.ToUniversalTime() - UnixEpoch).TotalSeconds;
                var subSecondTicks = value.Ticks % TimeSpan.TicksPerSecond;
                currentRawValues[i] = totalSeconds * _scaleFactor + (subSecondTicks * _scaleFactor / TimeSpan.TicksPerSecond);
            }

            var byteCount = batchSize * sizeof(long);
            var span = writer.GetSpan(byteCount);

            if (SimdCapabilities.IsAvx512FSupported && batchSize >= 8)
                WriteValuesAvx512(currentRawValues, span);
            else if (SimdCapabilities.IsAvx2Supported && batchSize >= 4)
                WriteValuesAvx2(currentRawValues, span);
            else if (SimdCapabilities.IsSse2Supported && batchSize >= 2)
                WriteValuesSse2(currentRawValues, span);
            else
                WriteValuesScalar(currentRawValues, span);

            writer.Advance(byteCount);
            remaining -= batchSize;
        }
    }

    private static void WriteValuesScalar(ReadOnlySpan<long> source, Span<byte> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<long, byte>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < source.Length; i++)
            {
                BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(i * sizeof(long), sizeof(long)), source[i]);
            }
        }
    }

    private static unsafe void WriteValuesSse2(ReadOnlySpan<long> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 2;
        var remainder = source.Length % 2;

        fixed (long* srcPtr = source)
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
                src += 2;
                dst += 16;
            }

            if (remainder > 0)
            {
                WriteValuesScalar(source.Slice(vectorCount * 2, remainder), destination[(vectorCount * 16)..]);
            }
        }
    }

    private static unsafe void WriteValuesAvx2(ReadOnlySpan<long> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 4;
        var remainder = source.Length % 4;

        fixed (long* srcPtr = source)
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
                src += 4;
                dst += 32;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 4;
            if (remainder >= 2 && Sse2.IsSupported)
                WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(long))..]);
            else
                WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(long))..]);
        }
    }

    private static unsafe void WriteValuesAvx512(ReadOnlySpan<long> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 8;
        var remainder = source.Length % 8;

        fixed (long* srcPtr = source)
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
                src += 8;
                dst += 64;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 8;
            switch (remainder)
            {
                case >= 4 when Avx2.IsSupported:
                    WriteValuesAvx2(source.Slice(processed, remainder), destination[(processed * sizeof(long))..]);
                    break;
                case >= 2 when Sse2.IsSupported:
                    WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(long))..]);
                    break;
                default:
                    WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(long))..]);
                    break;
            }
        }
    }
}