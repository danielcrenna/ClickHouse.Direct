using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

// ReSharper disable InconsistentNaming

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse IPv4 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/ipv4
/// 
/// Stores IPv4 addresses as 32-bit unsigned integers in big-endian format.
/// Maps to .NET IPAddress type.
/// 
/// This implementation provides:
/// - AVX512F: Processes 16 addresses simultaneously (64 bytes) using 512-bit vectors  
/// - AVX2: Processes 8 addresses simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 4 addresses simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// </summary>
public sealed class IPv4Type(ISimdCapabilities simdCapabilities) : BaseClickHouseType<IPAddress>
{
    public static readonly IPv4Type Instance = new();

    // Note: IPv4 is stored in big-endian format in ClickHouse
    // No shuffle masks needed as we handle endianness during conversion
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public IPv4Type() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x13;
    public override string TypeName => "IPv4";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(uint);

    public override IPAddress ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < sizeof(uint))
            throw new InvalidOperationException($"Insufficient data to read IPv4. Expected {sizeof(uint)} bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[sizeof(uint)];
        sequence.Slice(0, sizeof(uint)).CopyTo(buffer);
        sequence = sequence.Slice(sizeof(uint));
        bytesConsumed = sizeof(uint);

        // IPv4 is stored in big-endian in ClickHouse
        var addressBytes = buffer.ToArray();
        return new IPAddress(addressBytes);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<IPAddress> destination, out int bytesConsumed)
    {
        var valuesRead = Math.Min(destination.Length, (int)(sequence.Length / sizeof(uint)));
        bytesConsumed = valuesRead * sizeof(uint);

        if (valuesRead == 0)
            return valuesRead;

        // Read raw uint values first
        Span<uint> rawValues = stackalloc uint[Math.Min(valuesRead, 512)];
        Span<byte> addressBytes = stackalloc byte[4];
        
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

            // Convert uint to IPAddress (already in big-endian)
            for (var i = 0; i < batchSize; i++)
            {
                BinaryPrimitives.WriteUInt32BigEndian(addressBytes, currentRawValues[i]);
                destination[destIndex++] = new IPAddress(addressBytes);
            }

            sequence = sequence.Slice(byteCount);
            remaining -= batchSize;
        }

        return valuesRead;
    }

    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<uint> destination)
    {
        // IPv4 is stored as big-endian in ClickHouse, read directly
        for (var i = 0; i < destination.Length; i++)
        {
            destination[i] = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(i * sizeof(uint), sizeof(uint)));
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
            destination[i] = BinaryPrimitives.ReadUInt32BigEndian(buffer);
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
                // No shuffle needed - IPv4 is stored as big-endian
                Sse2.Store((byte*)dst, vector);
                
                // Convert to big-endian after loading
                for (var j = 0; j < 4; j++)
                {
                    dst[j] = BinaryPrimitives.ReverseEndianness(dst[j]);
                }
                
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
                Avx.Store((byte*)dst, vector);
                
                // Convert to big-endian after loading
                for (var j = 0; j < 8; j++)
                {
                    dst[j] = BinaryPrimitives.ReverseEndianness(dst[j]);
                }
                
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
                Avx512F.Store((byte*)dst, vector);
                
                // Convert to big-endian after loading
                for (var j = 0; j < 16; j++)
                {
                    dst[j] = BinaryPrimitives.ReverseEndianness(dst[j]);
                }
                
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

    public override void WriteValue(IBufferWriter<byte> writer, IPAddress value)
    {
        if (value.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
            throw new ArgumentException("IPv4Type can only write IPv4 addresses", nameof(value));

        var span = writer.GetSpan(sizeof(uint));
        var bytes = value.GetAddressBytes();
        bytes.CopyTo(span);
        writer.Advance(sizeof(uint));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<IPAddress> values)
    {
        if (values.IsEmpty)
            return;

        // Convert IPAddress to uint first
        Span<uint> rawValues = stackalloc uint[Math.Min(values.Length, 512)];
        
        var remaining = values.Length;
        var srcIndex = 0;
        
        while (remaining > 0)
        {
            var batchSize = Math.Min(remaining, rawValues.Length);
            var currentRawValues = rawValues[..batchSize];
            
            // Convert batch of IPAddress to uint
            for (var i = 0; i < batchSize; i++)
            {
                var address = values[srcIndex++];
                if (address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
                    throw new ArgumentException($"IPv4Type can only write IPv4 addresses at index {srcIndex - 1}");
                
                var bytes = address.GetAddressBytes();
                currentRawValues[i] = BinaryPrimitives.ReadUInt32BigEndian(bytes);
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
        // Write as big-endian
        for (var i = 0; i < source.Length; i++)
        {
            BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(i * sizeof(uint), sizeof(uint)), source[i]);
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
                // Convert to big-endian before storing
                var dstUint = (uint*)dst;
                for (var j = 0; j < 4; j++)
                {
                    dstUint[j] = BinaryPrimitives.ReverseEndianness(src[j]);
                }
                
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
                // Convert to big-endian before storing
                var dstUint = (uint*)dst;
                for (var j = 0; j < 8; j++)
                {
                    dstUint[j] = BinaryPrimitives.ReverseEndianness(src[j]);
                }
                
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
                // Convert to big-endian before storing
                var dstUint = (uint*)dst;
                for (var j = 0; j < 16; j++)
                {
                    dstUint[j] = BinaryPrimitives.ReverseEndianness(src[j]);
                }
                
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