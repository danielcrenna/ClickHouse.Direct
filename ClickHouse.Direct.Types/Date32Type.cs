using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse Date32 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/date32
/// 
/// Stores dates as 32-bit signed integers representing the number of days since 1900-01-01.
/// Range: 1900-01-01 to 2299-12-31
/// 
/// This implementation provides:
/// - AVX512F: Processes 16 dates simultaneously (64 bytes) using 512-bit vectors  
/// - AVX2: Processes 8 dates simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 4 dates simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class Date32Type(ISimdCapabilities simdCapabilities) : BaseClickHouseType<DateOnly>
{
    public static readonly Date32Type Instance = new();
    private static readonly DateOnly Epoch = new(1900, 1, 1);
    
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

    public Date32Type() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x1E;
    public override string TypeName => "Date32";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(int);

    public override DateOnly ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < sizeof(int))
            throw new InvalidOperationException($"Insufficient data to read Date32. Expected {sizeof(int)} bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[sizeof(int)];
        sequence.Slice(0, sizeof(int)).CopyTo(buffer);
        sequence = sequence.Slice(sizeof(int));
        bytesConsumed = sizeof(int);

        var days = BitConverter.IsLittleEndian
            ? BinaryPrimitives.ReadInt32LittleEndian(buffer)
            : BinaryPrimitives.ReadInt32BigEndian(buffer);

        return Epoch.AddDays(days);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<DateOnly> destination, out int bytesConsumed)
    {
        var valuesRead = Math.Min(destination.Length, (int)(sequence.Length / sizeof(int)));
        bytesConsumed = valuesRead * sizeof(int);

        if (valuesRead == 0)
            return valuesRead;

        // Read raw int values first
        Span<int> rawValues = stackalloc int[Math.Min(valuesRead, 512)];
        
        var remaining = valuesRead;
        var destIndex = 0;
        
        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, rawValues.Length);
            var currentRawValues = rawValues[..batchSize];
            var byteCount = batchSize * sizeof(int);
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

            // Convert int days to DateOnly
            for (var i = 0; i < batchSize; i++)
            {
                destination[destIndex++] = Epoch.AddDays(currentRawValues[i]);
            }

            sequence = sequence.Slice(byteCount);
            remaining -= batchSize;
        }

        return valuesRead;
    }

    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<int> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<byte, int>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < destination.Length; i++)
            {
                destination[i] = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(i * sizeof(int), sizeof(int)));
            }
        }
    }

    private static void ReadValuesScalar(ReadOnlySequence<byte> source, Span<int> destination)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        var reader = new SequenceReader<byte>(source);
        
        for (var i = 0; i < destination.Length; i++)
        {
            reader.TryCopyTo(buffer);
            reader.Advance(sizeof(int));
            destination[i] = BitConverter.IsLittleEndian
                ? BinaryPrimitives.ReadInt32LittleEndian(buffer)
                : BinaryPrimitives.ReadInt32BigEndian(buffer);
        }
    }

    private static unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<int> destination)
    {
        var vectorCount = destination.Length / 4;
        var remainder = destination.Length % 4;

        fixed (byte* srcPtr = source)
        fixed (int* dstPtr = destination)
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

    private static unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<int> destination)
    {
        var vectorCount = destination.Length / 8;
        var remainder = destination.Length % 8;

        fixed (byte* srcPtr = source)
        fixed (int* dstPtr = destination)
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
                    ReadValuesSse2(source[(processed * sizeof(int))..], destination.Slice(processed, remainder));
                else
                    ReadValuesScalar(source[(processed * sizeof(int))..], destination.Slice(processed, remainder));
            }
        }
    }

    private static unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<int> destination)
    {
        var vectorCount = destination.Length / 16;
        var remainder = destination.Length % 16;

        fixed (byte* srcPtr = source)
        fixed (int* dstPtr = destination)
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
                    ReadValuesAvx2(source[(processed * sizeof(int))..], destination.Slice(processed, remainder));
                    break;
                case >= 4 when Sse2.IsSupported:
                    ReadValuesSse2(source[(processed * sizeof(int))..], destination.Slice(processed, remainder));
                    break;
                default:
                    ReadValuesScalar(source[(processed * sizeof(int))..], destination.Slice(processed, remainder));
                    break;
            }
        }
    }

    public override void WriteValue(IBufferWriter<byte> writer, DateOnly value)
    {
        var span = writer.GetSpan(sizeof(int));
        var days = (value.DayNumber - Epoch.DayNumber);
        
        if (BitConverter.IsLittleEndian)
            BinaryPrimitives.WriteInt32LittleEndian(span, days);
        else
            BinaryPrimitives.WriteInt32BigEndian(span, days);
        writer.Advance(sizeof(int));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<DateOnly> values)
    {
        if (values.IsEmpty)
            return;

        // Convert DateOnly to int days first
        Span<int> rawValues = stackalloc int[Math.Min(values.Length, 512)];
        
        var remaining = values.Length;
        var srcIndex = 0;
        
        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, rawValues.Length);
            var currentRawValues = rawValues[..batchSize];
            
            // Convert batch of DateOnly to int
            for (var i = 0; i < batchSize; i++)
            {
                currentRawValues[i] = values[srcIndex++].DayNumber - Epoch.DayNumber;
            }

            var byteCount = batchSize * sizeof(int);
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

    private static void WriteValuesScalar(ReadOnlySpan<int> source, Span<byte> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<int, byte>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < source.Length; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(i * sizeof(int), sizeof(int)), source[i]);
            }
        }
    }

    private static unsafe void WriteValuesSse2(ReadOnlySpan<int> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 4;
        var remainder = source.Length % 4;

        fixed (int* srcPtr = source)
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

    private static unsafe void WriteValuesAvx2(ReadOnlySpan<int> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 8;
        var remainder = source.Length % 8;

        fixed (int* srcPtr = source)
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
                WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(int))..]);
            else
                WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(int))..]);
        }
    }

    private static unsafe void WriteValuesAvx512(ReadOnlySpan<int> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 16;
        var remainder = source.Length % 16;

        fixed (int* srcPtr = source)
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
                    WriteValuesAvx2(source.Slice(processed, remainder), destination[(processed * sizeof(int))..]);
                    break;
                case >= 4 when Sse2.IsSupported:
                    WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(int))..]);
                    break;
                default:
                    WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(int))..]);
                    break;
            }
        }
    }
}