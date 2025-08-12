using System.Buffers;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse UInt8 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/int-uint
/// 
/// The UInt8 data type represents unsigned 8-bit integers with a value range from 0 to 255.
/// Values are stored directly as bytes for RowBinary format (1 byte per value).
/// 
/// This implementation provides:
/// - AVX512BW: Processes 64 UInt8 values simultaneously (64 bytes) using 512-bit vectors
/// - AVX2: Processes 32 UInt8 values simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 16 UInt8 values simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// </summary>
public sealed class UInt8Type(ISimdCapabilities simdCapabilities) : BaseClickHouseType<byte>
{
    public static readonly UInt8Type Instance = new();
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public UInt8Type() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x01;
    public override string TypeName => "UInt8";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(byte);

    public override byte ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        const int size = sizeof(byte);
        bytesConsumed = size;

        if (sequence.Length < size)
            throw new InvalidOperationException($"Insufficient data: expected {size} byte, got {sequence.Length}");

        var value = sequence.First.Span[0];
        sequence = sequence.Slice(size);
        return value;
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<byte> destination, out int bytesConsumed)
    {
        var availableBytes = (int)sequence.Length;
        var maxItems = Math.Min(destination.Length, availableBytes);
        bytesConsumed = maxItems;

        if (maxItems == 0)
            return 0;

        if (sequence.First.Length >= maxItems)
        {
            // Fast path: all data is in first segment
            if (SimdCapabilities.IsAvx512BwSupported && maxItems >= 64)
            {
                // AVX512BW path: process 64 uint8s at once (64 bytes)
                ReadValuesAvx512(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsAvx2Supported && maxItems >= 32)
            {
                // AVX2 path: process 32 uint8s at once (32 bytes)
                ReadValuesAvx2(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsSse2Supported && maxItems >= 16)
            {
                // SSE2 path: process 16 uint8s at once (16 bytes)
                ReadValuesSse2(sequence.First.Span, destination[..maxItems]);
            }
            else
            {
                // Scalar path
                ReadValuesScalar(sequence.First.Span, destination[..maxItems]);
            }
            
            sequence = sequence.Slice(maxItems);
        }
        else
        {
            // Slow path: copy each value individually
            for (var i = 0; i < maxItems; i++)
            {
                destination[i] = ReadValue(ref sequence, out _);
            }
        }

        return maxItems;
    }

    public override void WriteValue(IBufferWriter<byte> writer, byte value)
    {
        var span = writer.GetSpan(sizeof(byte));
        span[0] = value;
        writer.Advance(sizeof(byte));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<byte> values)
    {
        if (values.IsEmpty)
            return;

        var totalBytes = values.Length;
        var span = writer.GetSpan(totalBytes);

        if (SimdCapabilities.IsAvx512BwSupported && values.Length >= 64)
        {
            // AVX512BW path: process 64 uint8s at once (64 bytes)
            WriteValuesAvx512(values, span);
        }
        else if (SimdCapabilities.IsAvx2Supported && values.Length >= 32)
        {
            // AVX2 path: process 32 uint8s at once (32 bytes)
            WriteValuesAvx2(values, span);
        }
        else if (SimdCapabilities.IsSse2Supported && values.Length >= 16)
        {
            // SSE2 path: process 16 uint8s at once (16 bytes)
            WriteValuesSse2(values, span);
        }
        else
        {
            // Scalar path
            WriteValuesScalar(values, span);
        }

        writer.Advance(totalBytes);
    }
    
    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        // Direct memory copy
        source[..destination.Length].CopyTo(destination);
    }
    
    private static unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 16; // 16 uint8s per SSE2 vector
        
        fixed (byte* srcPtr = source)
        fixed (byte* destPtr = destination)
        {
            for (; i + vectorSize <= destination.Length; i += vectorSize)
            {
                var vec = Sse2.LoadVector128(srcPtr + i);
                Sse2.Store(destPtr + i, vec);
            }
        }
        
        // Handle remaining elements
        if (i < destination.Length)
        {
            ReadValuesScalar(source[i..], destination[i..]);
        }
    }
    
    private static unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 32; // 32 uint8s per AVX2 vector
        
        fixed (byte* srcPtr = source)
        fixed (byte* destPtr = destination)
        {
            for (; i + vectorSize <= destination.Length; i += vectorSize)
            {
                var vec = Avx.LoadVector256(srcPtr + i);
                Avx.Store(destPtr + i, vec);
            }
        }
        
        if (i >= destination.Length)
            return;

        // Handle remaining elements with SSE2 or scalar
        if (destination.Length - i >= 16)
        {
            ReadValuesSse2(source[i..], destination[i..]);
        }
        else
        {
            ReadValuesScalar(source[i..], destination[i..]);
        }
    }
    
    private static unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 64; // 64 uint8s per AVX512 vector
        
        fixed (byte* srcPtr = source)
        fixed (byte* destPtr = destination)
        {
            for (; i + vectorSize <= destination.Length; i += vectorSize)
            {
                var vec = Avx512BW.LoadVector512(srcPtr + i);
                Avx512F.Store(destPtr + i, vec);
            }
        }
        
        if (i >= destination.Length)
            return;

        // Handle remaining elements with AVX2, SSE2, or scalar
        if (destination.Length - i >= 32)
        {
            ReadValuesAvx2(source[i..], destination[i..]);
        }
        else if (destination.Length - i >= 16)
        {
            ReadValuesSse2(source[i..], destination[i..]);
        }
        else
        {
            ReadValuesScalar(source[i..], destination[i..]);
        }
    }
    
    private static void WriteValuesScalar(ReadOnlySpan<byte> values, Span<byte> destination)
    {
        // Direct memory copy
        values.CopyTo(destination);
    }
    
    private static unsafe void WriteValuesSse2(ReadOnlySpan<byte> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 16; // 16 uint8s per SSE2 vector
        
        fixed (byte* srcPtr = values)
        fixed (byte* destPtr = destination)
        {
            for (; i + vectorSize <= values.Length; i += vectorSize)
            {
                var vec = Sse2.LoadVector128(srcPtr + i);
                Sse2.Store(destPtr + i, vec);
            }
        }
        
        // Handle remaining elements
        if (i < values.Length)
        {
            WriteValuesScalar(values[i..], destination[i..]); 
        }
    }
    
    private static unsafe void WriteValuesAvx2(ReadOnlySpan<byte> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 32; // 32 uint8s per AVX2 vector
        
        fixed (byte* srcPtr = values)
        fixed (byte* destPtr = destination)
        {
            for (; i + vectorSize <= values.Length; i += vectorSize)
            {
                var vec = Avx.LoadVector256(srcPtr + i);
                Avx.Store(destPtr + i, vec);
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining elements with SSE2 or scalar
        if (values.Length - i >= 16)
        {
            WriteValuesSse2(values[i..], destination[i..]); 
        }
        else
        {
            WriteValuesScalar(values[i..], destination[i..]); 
        }
    }
    
    private static unsafe void WriteValuesAvx512(ReadOnlySpan<byte> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 64; // 64 uint8s per AVX512 vector
        
        fixed (byte* srcPtr = values)
        fixed (byte* destPtr = destination)
        {
            for (; i + vectorSize <= values.Length; i += vectorSize)
            {
                var vec = Avx512BW.LoadVector512(srcPtr + i);
                Avx512F.Store(destPtr + i, vec);
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining elements with AVX2, SSE2, or scalar
        if (values.Length - i >= 32)
        {
            WriteValuesAvx2(values[i..], destination[i..]); 
        }
        else if (values.Length - i >= 16)
        {
            WriteValuesSse2(values[i..], destination[i..]); 
        }
        else
        {
            WriteValuesScalar(values[i..], destination[i..]); 
        }
    }
}