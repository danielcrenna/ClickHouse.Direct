using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse Decimal64 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/decimal
/// 
/// Decimal64 stores fixed-point decimal numbers with up to 18 digits total.
/// The value is stored as a 64-bit signed integer representing the unscaled value.
/// Scale parameter (0-18) determines the number of decimal places.
/// 
/// This implementation provides:
/// - AVX512F: Processes 8 decimals simultaneously (64 bytes) using 512-bit vectors  
/// - AVX2: Processes 4 decimals simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 2 decimals simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class Decimal64Type : BaseClickHouseType<decimal>
{
    public const byte MaxPrecision = 18;
    public const byte DefaultScale = 2;
    
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
    
    // Pre-calculated scale factors for each scale level
    private static readonly decimal[] ScaleFactors =
    [
        1m,                            // 0
        10m,                           // 1
        100m,                          // 2
        1_000m,                        // 3
        10_000m,                       // 4
        100_000m,                      // 5
        1_000_000m,                    // 6
        10_000_000m,                   // 7
        100_000_000m,                  // 8
        1_000_000_000m,                // 9
        10_000_000_000m,               // 10
        100_000_000_000m,              // 11
        1_000_000_000_000m,            // 12
        10_000_000_000_000m,           // 13
        100_000_000_000_000m,          // 14
        1_000_000_000_000_000m,        // 15
        10_000_000_000_000_000m,       // 16
        100_000_000_000_000_000m,      // 17
        1_000_000_000_000_000_000m     // 18
    ];
    
    public ISimdCapabilities SimdCapabilities { get; }
    public byte Precision { get; }
    public byte Scale { get; }
    private readonly decimal _scaleFactor;

    public Decimal64Type(byte precision = MaxPrecision, byte scale = DefaultScale, ISimdCapabilities? simdCapabilities = null)
    {
        if (precision > MaxPrecision)
            throw new ArgumentOutOfRangeException(nameof(precision), precision, $"Precision must be between 1 and {MaxPrecision}");
        if (scale > precision)
            throw new ArgumentOutOfRangeException(nameof(scale), scale, "Scale cannot be greater than precision");
            
        Precision = precision;
        Scale = scale;
        _scaleFactor = ScaleFactors[scale];
        SimdCapabilities = simdCapabilities ?? DefaultSimdCapabilities.Instance;
    }

    public static Decimal64Type CreateWithScale(byte scale) => new(MaxPrecision, scale);
    public static readonly Decimal64Type Instance = new();

    public override byte ProtocolCode => 0x17;
    public override string TypeName => $"Decimal64({Precision},{Scale})";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(long);

    public override decimal ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < sizeof(long))
            throw new InvalidOperationException($"Insufficient data to read Decimal64. Expected {sizeof(long)} bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[sizeof(long)];
        sequence.Slice(0, sizeof(long)).CopyTo(buffer);
        sequence = sequence.Slice(sizeof(long));
        bytesConsumed = sizeof(long);

        var unscaledValue = BitConverter.IsLittleEndian
            ? BinaryPrimitives.ReadInt64LittleEndian(buffer)
            : BinaryPrimitives.ReadInt64BigEndian(buffer);

        return unscaledValue / _scaleFactor;
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<decimal> destination, out int bytesConsumed)
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

            // Convert unscaled values to decimal
            for (var i = 0; i < batchSize; i++)
            {
                destination[destIndex++] = currentRawValues[i] / _scaleFactor;
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

    public override void WriteValue(IBufferWriter<byte> writer, decimal value)
    {
        var span = writer.GetSpan(sizeof(long));
        var unscaledValue = (long)(value * _scaleFactor);
        
        if (BitConverter.IsLittleEndian)
            BinaryPrimitives.WriteInt64LittleEndian(span, unscaledValue);
        else
            BinaryPrimitives.WriteInt64BigEndian(span, unscaledValue);
        writer.Advance(sizeof(long));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<decimal> values)
    {
        if (values.IsEmpty)
            return;

        // Convert decimal to long unscaled values first
        Span<long> rawValues = stackalloc long[Math.Min(values.Length, 256)];
        
        var remaining = values.Length;
        var srcIndex = 0;
        
        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, rawValues.Length);
            var currentRawValues = rawValues[..batchSize];
            
            // Convert batch of decimal to long
            for (var i = 0; i < batchSize; i++)
            {
                currentRawValues[i] = (long)(values[srcIndex++] * _scaleFactor);
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