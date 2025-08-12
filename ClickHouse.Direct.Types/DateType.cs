using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse Date type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/date
/// 
/// Stores dates as 16-bit unsigned integers representing the number of days since Unix epoch (1970-01-01).
/// Range: 1970-01-01 to 2149-06-06 (0 to 65535 days)
/// 
/// This implementation provides:
/// - AVX512BW: Processes 32 dates simultaneously (64 bytes) using 512-bit vectors  
/// - AVX2: Processes 16 dates simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 8 dates simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class DateType(ISimdCapabilities simdCapabilities) : BaseClickHouseType<DateOnly>
{
    public static readonly DateType Instance = new();
    private static readonly DateOnly UnixEpoch = new(1970, 1, 1);
    
    // Shuffle masks for endianness conversion (16-bit values)
    private static readonly Vector128<byte> ShuffleMaskSse2 = Vector128.Create(
        (byte)1, 0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14);
    private static readonly Vector256<byte> ShuffleMaskAvx2 = Vector256.Create(
        (byte)1, 0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14,
        17, 16, 19, 18, 21, 20, 23, 22, 25, 24, 27, 26, 29, 28, 31, 30);
    private static readonly Vector512<byte> ShuffleMaskAvx512 = Vector512.Create(
        (byte)1, 0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14,
        17, 16, 19, 18, 21, 20, 23, 22, 25, 24, 27, 26, 29, 28, 31, 30,
        33, 32, 35, 34, 37, 36, 39, 38, 41, 40, 43, 42, 45, 44, 47, 46,
        49, 48, 51, 50, 53, 52, 55, 54, 57, 56, 59, 58, 61, 60, 63, 62);
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public DateType() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x10;
    public override string TypeName => "Date";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(ushort);

    public override DateOnly ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < sizeof(ushort))
            throw new InvalidOperationException($"Insufficient data to read Date. Expected {sizeof(ushort)} bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[sizeof(ushort)];
        sequence.Slice(0, sizeof(ushort)).CopyTo(buffer);
        sequence = sequence.Slice(sizeof(ushort));
        bytesConsumed = sizeof(ushort);

        var days = BitConverter.IsLittleEndian
            ? BinaryPrimitives.ReadUInt16LittleEndian(buffer)
            : BinaryPrimitives.ReadUInt16BigEndian(buffer);

        return UnixEpoch.AddDays(days);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<DateOnly> destination, out int bytesConsumed)
    {
        var valuesRead = Math.Min(destination.Length, (int)(sequence.Length / sizeof(ushort)));
        bytesConsumed = valuesRead * sizeof(ushort);

        if (valuesRead == 0)
            return valuesRead;

        // Read raw ushort values first
        Span<ushort> rawValues = stackalloc ushort[Math.Min(valuesRead, 2048)];
        
        var remaining = valuesRead;
        var destIndex = 0;
        
        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, rawValues.Length);
            var currentRawValues = rawValues[..batchSize];
            var byteCount = batchSize * sizeof(ushort);
            var sourceSequence = sequence.Slice(0, byteCount);

            if (sourceSequence.IsSingleSegment)
            {
                var sourceSpan = sourceSequence.FirstSpan;
                if (SimdCapabilities.IsAvx512BwSupported && batchSize >= 32)
                    ReadValuesAvx512(sourceSpan, currentRawValues);
                else if (SimdCapabilities.IsAvx2Supported && batchSize >= 16)
                    ReadValuesAvx2(sourceSpan, currentRawValues);
                else if (SimdCapabilities.IsSse2Supported && batchSize >= 8)
                    ReadValuesSse2(sourceSpan, currentRawValues);
                else
                    ReadValuesScalar(sourceSpan, currentRawValues);
            }
            else
            {
                ReadValuesScalar(sourceSequence, currentRawValues);
            }

            // Convert ushort days to DateOnly
            for (var i = 0; i < batchSize; i++)
            {
                destination[destIndex++] = UnixEpoch.AddDays(currentRawValues[i]);
            }

            sequence = sequence.Slice(byteCount);
            remaining -= batchSize;
        }

        return valuesRead;
    }

    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<ushort> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<byte, ushort>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < destination.Length; i++)
            {
                destination[i] = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(i * sizeof(ushort), sizeof(ushort)));
            }
        }
    }

    private static void ReadValuesScalar(ReadOnlySequence<byte> source, Span<ushort> destination)
    {
        Span<byte> buffer = stackalloc byte[sizeof(ushort)];
        var reader = new SequenceReader<byte>(source);
        
        for (var i = 0; i < destination.Length; i++)
        {
            reader.TryCopyTo(buffer);
            reader.Advance(sizeof(ushort));
            destination[i] = BitConverter.IsLittleEndian
                ? BinaryPrimitives.ReadUInt16LittleEndian(buffer)
                : BinaryPrimitives.ReadUInt16BigEndian(buffer);
        }
    }

    private static unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<ushort> destination)
    {
        var vectorCount = destination.Length / 8;
        var remainder = destination.Length % 8;

        fixed (byte* srcPtr = source)
        fixed (ushort* dstPtr = destination)
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
                dst += 8;
            }

            if (remainder > 0)
            {
                ReadValuesScalar(source[(vectorCount * 16)..], destination.Slice(vectorCount * 8, remainder));
            }
        }
    }

    private static unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<ushort> destination)
    {
        var vectorCount = destination.Length / 16;
        var remainder = destination.Length % 16;

        fixed (byte* srcPtr = source)
        fixed (ushort* dstPtr = destination)
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
                dst += 16;
            }

            if (remainder > 0)
            {
                var processed = vectorCount * 16;
                if (remainder >= 8 && Sse2.IsSupported)
                    ReadValuesSse2(source[(processed * sizeof(ushort))..], destination.Slice(processed, remainder));
                else
                    ReadValuesScalar(source[(processed * sizeof(ushort))..], destination.Slice(processed, remainder));
            }
        }
    }

    private static unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<ushort> destination)
    {
        var vectorCount = destination.Length / 32;
        var remainder = destination.Length % 32;

        fixed (byte* srcPtr = source)
        fixed (ushort* dstPtr = destination)
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
                dst += 32;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 32;
            switch (remainder)
            {
                case >= 16 when Avx2.IsSupported:
                    ReadValuesAvx2(source[(processed * sizeof(ushort))..], destination.Slice(processed, remainder));
                    break;
                case >= 8 when Sse2.IsSupported:
                    ReadValuesSse2(source[(processed * sizeof(ushort))..], destination.Slice(processed, remainder));
                    break;
                default:
                    ReadValuesScalar(source[(processed * sizeof(ushort))..], destination.Slice(processed, remainder));
                    break;
            }
        }
    }

    public override void WriteValue(IBufferWriter<byte> writer, DateOnly value)
    {
        var span = writer.GetSpan(sizeof(ushort));
        var days = (ushort)value.DayNumber;
        
        if (BitConverter.IsLittleEndian)
            BinaryPrimitives.WriteUInt16LittleEndian(span, days);
        else
            BinaryPrimitives.WriteUInt16BigEndian(span, days);
        writer.Advance(sizeof(ushort));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<DateOnly> values)
    {
        if (values.IsEmpty)
            return;

        // Convert DateOnly to ushort days first
        Span<ushort> rawValues = stackalloc ushort[Math.Min(values.Length, 2048)];
        
        var remaining = values.Length;
        var srcIndex = 0;
        
        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, rawValues.Length);
            var currentRawValues = rawValues[..batchSize];
            
            // Convert batch of DateOnly to ushort
            for (var i = 0; i < batchSize; i++)
            {
                currentRawValues[i] = (ushort)values[srcIndex++].DayNumber;
            }

            var byteCount = batchSize * sizeof(ushort);
            var span = writer.GetSpan(byteCount);

            if (SimdCapabilities.IsAvx512BwSupported && batchSize >= 32)
                WriteValuesAvx512(currentRawValues, span);
            else if (SimdCapabilities.IsAvx2Supported && batchSize >= 16)
                WriteValuesAvx2(currentRawValues, span);
            else if (SimdCapabilities.IsSse2Supported && batchSize >= 8)
                WriteValuesSse2(currentRawValues, span);
            else
                WriteValuesScalar(currentRawValues, span);

            writer.Advance(byteCount);
            remaining -= batchSize;
        }
    }

    private static void WriteValuesScalar(ReadOnlySpan<ushort> source, Span<byte> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<ushort, byte>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < source.Length; i++)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(i * sizeof(ushort), sizeof(ushort)), source[i]);
            }
        }
    }

    private static unsafe void WriteValuesSse2(ReadOnlySpan<ushort> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 8;
        var remainder = source.Length % 8;

        fixed (ushort* srcPtr = source)
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
                src += 8;
                dst += 16;
            }

            if (remainder > 0)
            {
                WriteValuesScalar(source.Slice(vectorCount * 8, remainder), destination[(vectorCount * 16)..]);
            }
        }
    }

    private static unsafe void WriteValuesAvx2(ReadOnlySpan<ushort> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 16;
        var remainder = source.Length % 16;

        fixed (ushort* srcPtr = source)
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
                src += 16;
                dst += 32;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 16;
            if (remainder >= 8 && Sse2.IsSupported)
                WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(ushort))..]);
            else
                WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(ushort))..]);
        }
    }

    private static unsafe void WriteValuesAvx512(ReadOnlySpan<ushort> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 32;
        var remainder = source.Length % 32;

        fixed (ushort* srcPtr = source)
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
                src += 32;
                dst += 64;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 32;
            switch (remainder)
            {
                case >= 16 when Avx2.IsSupported:
                    WriteValuesAvx2(source.Slice(processed, remainder), destination[(processed * sizeof(ushort))..]);
                    break;
                case >= 8 when Sse2.IsSupported:
                    WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(ushort))..]);
                    break;
                default:
                    WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(ushort))..]);
                    break;
            }
        }
    }
}