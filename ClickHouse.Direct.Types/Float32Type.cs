using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse Float32 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/float
/// 
/// Handles 32-bit IEEE 754 single-precision floating-point values.
/// Range: ±1.175494e-38 to ±3.402823e+38
/// 
/// This implementation provides:
/// - AVX512F: Processes 16 floats simultaneously (64 bytes) using 512-bit vectors
/// - AVX2: Processes 8 floats simultaneously (32 bytes) using 256-bit vectors  
/// - SSE2: Processes 4 floats simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class Float32Type(ISimdCapabilities simdCapabilities) : BaseClickHouseType<float>
{
    public static readonly Float32Type Instance = new();
    
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

    public Float32Type() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x43;
    public override string TypeName => "Float32";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(float);

    public override float ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < sizeof(float))
            throw new InvalidOperationException($"Insufficient data to read Float32. Expected {sizeof(float)} bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[sizeof(float)];
        sequence.Slice(0, sizeof(float)).CopyTo(buffer);
        sequence = sequence.Slice(sizeof(float));
        bytesConsumed = sizeof(float);

        return BitConverter.IsLittleEndian
            ? BinaryPrimitives.ReadSingleLittleEndian(buffer)
            : BinaryPrimitives.ReadSingleBigEndian(buffer);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<float> destination, out int bytesConsumed)
    {
        var valuesRead = Math.Min(destination.Length, (int)(sequence.Length / sizeof(float)));
        bytesConsumed = valuesRead * sizeof(float);

        if (valuesRead == 0)
            return valuesRead;

        var toRead = destination[..valuesRead];
        var byteCount = valuesRead * sizeof(float);
        var sourceSequence = sequence.Slice(0, byteCount);

        if (sourceSequence.IsSingleSegment)
        {
            var sourceSpan = sourceSequence.FirstSpan;
            if (SimdCapabilities.IsAvx512FSupported && valuesRead >= 16)
                ReadValuesAvx512(sourceSpan, toRead);
            else if (SimdCapabilities.IsAvx2Supported && valuesRead >= 8)
                ReadValuesAvx2(sourceSpan, toRead);
            else if (SimdCapabilities.IsSse2Supported && valuesRead >= 4)
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

    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<float> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<byte, float>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < destination.Length; i++)
            {
                destination[i] = BinaryPrimitives.ReadSingleLittleEndian(source.Slice(i * sizeof(float), sizeof(float)));
            }
        }
    }

    private static void ReadValuesScalar(ReadOnlySequence<byte> source, Span<float> destination)
    {
        Span<byte> buffer = stackalloc byte[sizeof(float)];
        var reader = new SequenceReader<byte>(source);
        
        for (var i = 0; i < destination.Length; i++)
        {
            reader.TryCopyTo(buffer);
            reader.Advance(sizeof(float));
            destination[i] = BitConverter.IsLittleEndian
                ? BinaryPrimitives.ReadSingleLittleEndian(buffer)
                : BinaryPrimitives.ReadSingleBigEndian(buffer);
        }
    }

    private static unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<float> destination)
    {
        var vectorCount = destination.Length / 4;
        var remainder = destination.Length % 4;

        fixed (byte* srcPtr = source)
        fixed (float* dstPtr = destination)
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

    private static unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<float> destination)
    {
        var vectorCount = destination.Length / 8;
        var remainder = destination.Length % 8;

        fixed (byte* srcPtr = source)
        fixed (float* dstPtr = destination)
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
                    ReadValuesSse2(source[(processed * sizeof(float))..], destination.Slice(processed, remainder));
                else
                    ReadValuesScalar(source[(processed * sizeof(float))..], destination.Slice(processed, remainder));
            }
        }
    }

    private static unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<float> destination)
    {
        var vectorCount = destination.Length / 16;
        var remainder = destination.Length % 16;

        fixed (byte* srcPtr = source)
        fixed (float* dstPtr = destination)
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
                    ReadValuesAvx2(source[(processed * sizeof(float))..], destination.Slice(processed, remainder));
                    break;
                case >= 4 when Sse2.IsSupported:
                    ReadValuesSse2(source[(processed * sizeof(float))..], destination.Slice(processed, remainder));
                    break;
                default:
                    ReadValuesScalar(source[(processed * sizeof(float))..], destination.Slice(processed, remainder));
                    break;
            }
        }
    }

    public override void WriteValue(IBufferWriter<byte> writer, float value)
    {
        var span = writer.GetSpan(sizeof(float));
        if (BitConverter.IsLittleEndian)
            BinaryPrimitives.WriteSingleLittleEndian(span, value);
        else
            BinaryPrimitives.WriteSingleBigEndian(span, value);
        writer.Advance(sizeof(float));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<float> values)
    {
        if (values.IsEmpty)
            return;

        var byteCount = values.Length * sizeof(float);
        var span = writer.GetSpan(byteCount);

        if (SimdCapabilities.IsAvx512FSupported && values.Length >= 16)
            WriteValuesAvx512(values, span);
        else if (SimdCapabilities.IsAvx2Supported && values.Length >= 8)
            WriteValuesAvx2(values, span);
        else if (SimdCapabilities.IsSse2Supported && values.Length >= 4)
            WriteValuesSse2(values, span);
        else
            WriteValuesScalar(values, span);

        writer.Advance(byteCount);
    }

    private static void WriteValuesScalar(ReadOnlySpan<float> source, Span<byte> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<float, byte>(source).CopyTo(destination);
        }
        else
        {
            for (var i = 0; i < source.Length; i++)
            {
                BinaryPrimitives.WriteSingleLittleEndian(destination.Slice(i * sizeof(float), sizeof(float)), source[i]);
            }
        }
    }

    private static unsafe void WriteValuesSse2(ReadOnlySpan<float> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 4;
        var remainder = source.Length % 4;

        fixed (float* srcPtr = source)
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

    private static unsafe void WriteValuesAvx2(ReadOnlySpan<float> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 8;
        var remainder = source.Length % 8;

        fixed (float* srcPtr = source)
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
                WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(float))..]);
            else
                WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(float))..]);
        }
    }

    private static unsafe void WriteValuesAvx512(ReadOnlySpan<float> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 16;
        var remainder = source.Length % 16;

        fixed (float* srcPtr = source)
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
                    WriteValuesAvx2(source.Slice(processed, remainder), destination[(processed * sizeof(float))..]);
                    break;
                case >= 4 when Sse2.IsSupported:
                    WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * sizeof(float))..]);
                    break;
                default:
                    WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * sizeof(float))..]);
                    break;
            }
        }
    }
}