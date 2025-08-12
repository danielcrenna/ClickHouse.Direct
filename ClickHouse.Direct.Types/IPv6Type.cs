using System.Buffers;
using System.Net;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

// ReSharper disable InconsistentNaming

/// <summary>
/// ClickHouse IPv6 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/ipv6
/// 
/// Stores IPv6 addresses as 16-byte values in big-endian format.
/// Maps to .NET IPAddress type.
/// 
/// This implementation provides:
/// - AVX512F: Processes 4 addresses simultaneously (64 bytes) using 512-bit vectors  
/// - AVX2: Processes 2 addresses simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 1 address (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// </summary>
public sealed class IPv6Type(ISimdCapabilities simdCapabilities) : BaseClickHouseType<IPAddress>
{
    public static readonly IPv6Type Instance = new();
    private const int IPv6ByteLength = 16;
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public IPv6Type() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x14;
    public override string TypeName => "IPv6";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => IPv6ByteLength;

    public override IPAddress ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < IPv6ByteLength)
            throw new InvalidOperationException($"Insufficient data to read IPv6. Expected {IPv6ByteLength} bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[IPv6ByteLength];
        sequence.Slice(0, IPv6ByteLength).CopyTo(buffer);
        sequence = sequence.Slice(IPv6ByteLength);
        bytesConsumed = IPv6ByteLength;

        // IPv6 is stored in big-endian in ClickHouse
        return new IPAddress(buffer);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<IPAddress> destination, out int bytesConsumed)
    {
        var valuesRead = Math.Min(destination.Length, (int)(sequence.Length / IPv6ByteLength));
        bytesConsumed = valuesRead * IPv6ByteLength;

        if (valuesRead == 0)
            return valuesRead;

        var toRead = destination[..valuesRead];
        var byteCount = valuesRead * IPv6ByteLength;
        var sourceSequence = sequence.Slice(0, byteCount);

        if (sourceSequence.IsSingleSegment)
        {
            var sourceSpan = sourceSequence.FirstSpan;
            if (SimdCapabilities.IsAvx512FSupported && valuesRead >= 4)
                ReadValuesAvx512(sourceSpan, toRead);
            else if (SimdCapabilities.IsAvx2Supported && valuesRead >= 2)
                ReadValuesAvx2(sourceSpan, toRead);
            else if (SimdCapabilities.IsSse2Supported && valuesRead >= 1)
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

    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<IPAddress> destination)
    {
        for (var i = 0; i < destination.Length; i++)
        {
            var addressBytes = source.Slice(i * IPv6ByteLength, IPv6ByteLength);
            destination[i] = new IPAddress(addressBytes);
        }
    }

    private static void ReadValuesScalar(ReadOnlySequence<byte> source, Span<IPAddress> destination)
    {
        Span<byte> buffer = stackalloc byte[IPv6ByteLength];
        var reader = new SequenceReader<byte>(source);
        
        for (var i = 0; i < destination.Length; i++)
        {
            reader.TryCopyTo(buffer);
            reader.Advance(IPv6ByteLength);
            destination[i] = new IPAddress(buffer);
        }
    }

    private static unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<IPAddress> destination)
    {
        var vectorCount = destination.Length;

        fixed (byte* srcPtr = source)
        {
            var src = srcPtr;
            Span<byte> addressBytes = stackalloc byte[IPv6ByteLength];

            for (var i = 0; i < vectorCount; i++)
            {
                // Load 16 bytes directly
                new ReadOnlySpan<byte>(src, IPv6ByteLength).CopyTo(addressBytes);
                destination[i] = new IPAddress(addressBytes);
                src += IPv6ByteLength;
            }
        }
    }

    private static unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<IPAddress> destination)
    {
        var vectorCount = destination.Length / 2;
        var remainder = destination.Length % 2;

        fixed (byte* srcPtr = source)
        {
            var src = srcPtr;
            var destIndex = 0;

            Span<byte> addr1 = stackalloc byte[IPv6ByteLength];
            Span<byte> addr2 = stackalloc byte[IPv6ByteLength];
            
            for (var i = 0; i < vectorCount; i++)
            {
                // Process 2 IPv6 addresses at once (32 bytes)
                var vector = Avx.LoadVector256(src);
                
                // Extract first IPv6 address
                new ReadOnlySpan<byte>(src, IPv6ByteLength).CopyTo(addr1);
                destination[destIndex++] = new IPAddress(addr1);
                
                // Extract second IPv6 address
                new ReadOnlySpan<byte>(src + IPv6ByteLength, IPv6ByteLength).CopyTo(addr2);
                destination[destIndex++] = new IPAddress(addr2);
                
                src += 32;
            }

            if (remainder > 0)
            {
                ReadValuesScalar(source[(vectorCount * 32)..], destination.Slice(vectorCount * 2, remainder));
            }
        }
    }

    private static unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<IPAddress> destination)
    {
        var vectorCount = destination.Length / 4;
        var remainder = destination.Length % 4;

        fixed (byte* srcPtr = source)
        {
            var src = srcPtr;
            var destIndex = 0;

            Span<byte> addr = stackalloc byte[IPv6ByteLength];
            
            for (var i = 0; i < vectorCount; i++)
            {
                // Process 4 IPv6 addresses at once (64 bytes)
                var vector = Avx512F.LoadVector512(src);
                
                // Extract each IPv6 address
                for (var j = 0; j < 4; j++)
                {
                    new ReadOnlySpan<byte>(src + j * IPv6ByteLength, IPv6ByteLength).CopyTo(addr);
                    destination[destIndex++] = new IPAddress(addr);
                }
                
                src += 64;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 4;
            switch (remainder)
            {
                case >= 2 when Avx2.IsSupported:
                    ReadValuesAvx2(source[(processed * IPv6ByteLength)..], destination.Slice(processed, remainder));
                    break;
                case >= 1 when Sse2.IsSupported:
                    ReadValuesSse2(source[(processed * IPv6ByteLength)..], destination.Slice(processed, remainder));
                    break;
                default:
                    ReadValuesScalar(source[(processed * IPv6ByteLength)..], destination.Slice(processed, remainder));
                    break;
            }
        }
    }

    public override void WriteValue(IBufferWriter<byte> writer, IPAddress value)
    {
        if (value.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
            throw new ArgumentException("IPv6Type can only write IPv6 addresses", nameof(value));

        var span = writer.GetSpan(IPv6ByteLength);
        var bytes = value.GetAddressBytes();
        bytes.CopyTo(span);
        writer.Advance(IPv6ByteLength);
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<IPAddress> values)
    {
        if (values.IsEmpty)
            return;

        var byteCount = values.Length * IPv6ByteLength;
        var span = writer.GetSpan(byteCount);

        if (SimdCapabilities.IsAvx512FSupported && values.Length >= 4)
            WriteValuesAvx512(values, span);
        else if (SimdCapabilities.IsAvx2Supported && values.Length >= 2)
            WriteValuesAvx2(values, span);
        else if (SimdCapabilities.IsSse2Supported && values.Length >= 1)
            WriteValuesSse2(values, span);
        else
            WriteValuesScalar(values, span);

        writer.Advance(byteCount);
    }

    private static void WriteValuesScalar(ReadOnlySpan<IPAddress> source, Span<byte> destination)
    {
        for (var i = 0; i < source.Length; i++)
        {
            var address = source[i];
            if (address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
                throw new ArgumentException($"IPv6Type can only write IPv6 addresses at index {i}");
            
            var bytes = address.GetAddressBytes();
            bytes.CopyTo(destination.Slice(i * IPv6ByteLength, IPv6ByteLength));
        }
    }

    private static unsafe void WriteValuesSse2(ReadOnlySpan<IPAddress> source, Span<byte> destination)
    {
        fixed (byte* dstPtr = destination)
        {
            var dst = dstPtr;

            for (var i = 0; i < source.Length; i++)
            {
                var address = source[i];
                if (address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
                    throw new ArgumentException($"IPv6Type can only write IPv6 addresses at index {i}");
                
                var bytes = address.GetAddressBytes();
                bytes.CopyTo(new Span<byte>(dst, IPv6ByteLength));
                dst += IPv6ByteLength;
            }
        }
    }

    private static unsafe void WriteValuesAvx2(ReadOnlySpan<IPAddress> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 2;
        var remainder = source.Length % 2;

        fixed (byte* dstPtr = destination)
        {
            var dst = dstPtr;
            var srcIndex = 0;

            for (var i = 0; i < vectorCount; i++)
            {
                // Write 2 IPv6 addresses at once (32 bytes)
                for (var j = 0; j < 2; j++)
                {
                    var address = source[srcIndex++];
                    if (address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
                        throw new ArgumentException($"IPv6Type can only write IPv6 addresses at index {srcIndex - 1}");
                    
                    var bytes = address.GetAddressBytes();
                    bytes.CopyTo(new Span<byte>(dst + j * IPv6ByteLength, IPv6ByteLength));
                }
                
                dst += 32;
            }

            if (remainder > 0)
            {
                WriteValuesScalar(source.Slice(vectorCount * 2, remainder), destination[(vectorCount * 32)..]);
            }
        }
    }

    private static unsafe void WriteValuesAvx512(ReadOnlySpan<IPAddress> source, Span<byte> destination)
    {
        var vectorCount = source.Length / 4;
        var remainder = source.Length % 4;

        fixed (byte* dstPtr = destination)
        {
            var dst = dstPtr;
            var srcIndex = 0;

            for (var i = 0; i < vectorCount; i++)
            {
                // Write 4 IPv6 addresses at once (64 bytes)
                for (var j = 0; j < 4; j++)
                {
                    var address = source[srcIndex++];
                    if (address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
                        throw new ArgumentException($"IPv6Type can only write IPv6 addresses at index {srcIndex - 1}");
                    
                    var bytes = address.GetAddressBytes();
                    bytes.CopyTo(new Span<byte>(dst + j * IPv6ByteLength, IPv6ByteLength));
                }
                
                dst += 64;
            }

            if (remainder <= 0)
                return;

            var processed = vectorCount * 4;
            switch (remainder)
            {
                case >= 2 when Avx2.IsSupported:
                    WriteValuesAvx2(source.Slice(processed, remainder), destination[(processed * IPv6ByteLength)..]);
                    break;
                case >= 1 when Sse2.IsSupported:
                    WriteValuesSse2(source.Slice(processed, remainder), destination[(processed * IPv6ByteLength)..]);
                    break;
                default:
                    WriteValuesScalar(source.Slice(processed, remainder), destination[(processed * IPv6ByteLength)..]);
                    break;
            }
        }
    }
}