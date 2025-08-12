using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse Float64 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/float
/// 
/// Handles 64-bit IEEE 754 double-precision floating-point values.
/// Range: ±2.225074e-308 to ±1.797693e+308
/// 
/// This implementation provides:
/// - AVX512F: Processes 8 doubles simultaneously (64 bytes) using 512-bit vectors
/// - AVX2: Processes 4 doubles simultaneously (32 bytes) using 256-bit vectors  
/// - SSE2: Processes 2 doubles simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class Float64Type(ISimdCapabilities simdCapabilities) : BaseClickHouseType<double>
{
    public static readonly Float64Type Instance = new();
    
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
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public Float64Type() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x44;
    public override string TypeName => "Float64";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(double);

    public override double ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < sizeof(double))
            throw new InvalidOperationException($"Insufficient data to read Float64. Expected {sizeof(double)} bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[sizeof(double)];
        sequence.Slice(0, sizeof(double)).CopyTo(buffer);
        sequence = sequence.Slice(sizeof(double));
        bytesConsumed = sizeof(double);

        return BitConverter.IsLittleEndian
            ? BinaryPrimitives.ReadDoubleLittleEndian(buffer)
            : BinaryPrimitives.ReadDoubleBigEndian(buffer);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<double> destination, out int bytesConsumed)
    {
        var valuesRead = Math.Min(destination.Length, (int)(sequence.Length / sizeof(double)));
        bytesConsumed = valuesRead * sizeof(double);

        if (valuesRead == 0)
            return valuesRead;

        var toRead = destination[..valuesRead];
        var byteCount = valuesRead * sizeof(double);
        var sourceSequence = sequence.Slice(0, byteCount);

        if (sourceSequence.IsSingleSegment)
        {
            var sourceSpan = sourceSequence.FirstSpan;
            if (SimdCapabilities.IsAvx512FSupported && valuesRead >= 8)
                ReadValuesAvx512(sourceSpan, toRead);
            else if (SimdCapabilities.IsAvx2Supported && valuesRead >= 4)
                ReadValuesAvx2(sourceSpan, toRead);
            else if (SimdCapabilities.IsSse2Supported && valuesRead >= 2)
                ReadValuesSse2(sourceSpan, toRead);
            else
                ReadValuesScalar(sourceSpan, toRead);
        }
        else
        {
            ReadValuesScalar(sourceSequence, toRead);
        }

        sequence = sequence.Slice(bytesConsumed);
        return valuesRead;
    }

    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<double> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<byte, double>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < destination.Length; i++)
            {
                destination[i] = BinaryPrimitives.ReadDoubleLittleEndian(source.Slice(i * sizeof(double), sizeof(double)));
            }
        }
    }

    private static void ReadValuesScalar(ReadOnlySequence<byte> source, Span<double> destination)
    {
        Span<byte> buffer = stackalloc byte[sizeof(double)];
        var reader = new SequenceReader<byte>(source);
        
        for (var i = 0; i < destination.Length; i++)
        {
            reader.TryCopyTo(buffer);
            reader.Advance(sizeof(double));
            destination[i] = BitConverter.IsLittleEndian
                ? BinaryPrimitives.ReadDoubleLittleEndian(buffer)
                : BinaryPrimitives.ReadDoubleBigEndian(buffer);
        }
    }

    private static unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<double> destination)
    {
        var vectorCount = destination.Length / 2;
        var remainder = destination.Length % 2;

        fixed (byte* srcPtr = source)
        fixed (double* dstPtr = destination)
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

    private static unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<double> destination)
    {
        var vectorCount = destination.Length / 4;
        var remainder = destination.Length % 4;

        fixed (byte* srcPtr = source)
        fixed (double* dstPtr = destination)
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
                    ReadValuesSse2(source[(processed * sizeof(double))..], destination.Slice(processed, remainder));
                else
                    ReadValuesScalar(source[(processed * sizeof(double))..], destination.Slice(processed, remainder));
            }
        }
    }

    private static unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<double> destination)
    {
        var vectorCount = destination.Length / 8;
        var remainder = destination.Length % 8;

        fixed (byte* srcPtr = source)
        fixed (double* dstPtr = destination)
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
                    ReadValuesAvx2(source[(processed * sizeof(double))..], destination.Slice(processed, remainder));
                    break;
                case >= 2 when Sse2.IsSupported:
                    ReadValuesSse2(source[(processed * sizeof(double))..], destination.Slice(processed, remainder));
                    break;
                default:
                    ReadValuesScalar(source[(processed * sizeof(double))..], destination.Slice(processed, remainder));
                    break;
            }
        }
    }

    public override void WriteValue(IBufferWriter<byte> writer, double value)
    {
        var span = writer.GetSpan(sizeof(double));
        if (BitConverter.IsLittleEndian)
            BinaryPrimitives.WriteDoubleLittleEndian(span, value);
        else
            BinaryPrimitives.WriteDoubleBigEndian(span, value);
        writer.Advance(sizeof(double));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<double> values)
    {
        if (values.IsEmpty)
            return;

        var byteCount = values.Length * sizeof(double);
        var span = writer.GetSpan(byteCount);

        if (SimdCapabilities.IsAvx512FSupported && values.Length >= 8)
            WriteValuesAvx512(values, span);
        else if (SimdCapabilities.IsAvx2Supported && values.Length >= 4)
            WriteValuesAvx2(values, span);
        else if (SimdCapabilities.IsSse2Supported && values.Length >= 2)
            WriteValuesSse2(values, span);
        else
            WriteValuesScalar(values, span);

        writer.Advance(byteCount);
    }

    private static void WriteValuesScalar(ReadOnlySpan<double> source, Span<byte> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<double, byte>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < source.Length; i++)
            {
                BinaryPrimitives.WriteDoubleLittleEndian(destination.Slice(i * sizeof(double), sizeof(double)), source[i]);
            }
        }
    }

    private static unsafe void WriteValuesSse2(ReadOnlySpan<double> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 2;
        var remainder = source.Length % 2;

        fixed (double* srcPtr = source)
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

    private static unsafe void WriteValuesAvx2(ReadOnlySpan<double> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 4;
        var remainder = source.Length % 4;

        fixed (double* srcPtr = source)
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
                WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(double))..]);
            else
                WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(double))..]);
        }
    }

    private static unsafe void WriteValuesAvx512(ReadOnlySpan<double> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 8;
        var remainder = source.Length % 8;

        fixed (double* srcPtr = source)
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
                    WriteValuesAvx2(source.Slice(processed, remainder), destination[(processed * sizeof(double))..]);
                    break;
                case >= 2 when Sse2.IsSupported:
                    WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(double))..]);
                    break;
                default:
                    WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(double))..]);
                    break;
            }
        }
    }
}