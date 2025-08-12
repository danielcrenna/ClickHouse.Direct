using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse UInt64 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/int-uint
/// 
/// The UInt64 data type represents unsigned 64-bit integers with a value range from 0 to 18,446,744,073,709,551,615.
/// Values are stored in little-endian byte order for RowBinary format (8 bytes per value).
/// 
/// This implementation provides:
/// - AVX512F: Processes 8 UInt64 values simultaneously (64 bytes) using 512-bit vectors
/// - AVX2: Processes 4 UInt64 values simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 2 UInt64 values simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class UInt64Type(ISimdCapabilities simdCapabilities) : BaseClickHouseType<ulong>
{
    public static readonly UInt64Type Instance = new();
    
    private static readonly Vector128<byte> ShuffleMaskSse2 = Vector128.Create(
        (byte)7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8
    );
    
    private static readonly Vector256<byte> ShuffleMaskAvx2 = Vector256.Create(
        (byte)7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
        23, 22, 21, 20, 19, 18, 17, 16, 31, 30, 29, 28, 27, 26, 25, 24
    );
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public UInt64Type() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x04;
    public override string TypeName => "UInt64";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(ulong);

    public override ulong ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        const int size = sizeof(ulong);
        bytesConsumed = size;

        if (sequence.Length < size)
            throw new InvalidOperationException($"Insufficient data: expected {size} bytes, got {sequence.Length}");

        if (sequence.First.Length >= size)
        {
            // Fast path: data is in a single segment
            var value = BinaryPrimitives.ReadUInt64LittleEndian(sequence.First.Span);
            sequence = sequence.Slice(size);
            return value;
        }

        // Slow path: data spans multiple segments
        Span<byte> buffer = stackalloc byte[size];
        sequence.Slice(0, size).CopyTo(buffer);
        sequence = sequence.Slice(size);
        return BinaryPrimitives.ReadUInt64LittleEndian(buffer);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<ulong> destination, out int bytesConsumed)
    {
        const int itemSize = sizeof(ulong);
        var availableBytes = (int)sequence.Length;
        var maxItems = Math.Min(destination.Length, availableBytes / itemSize);
        var totalBytes = maxItems * itemSize;
        
        bytesConsumed = totalBytes;

        if (maxItems == 0)
            return 0;

        if (sequence.First.Length >= totalBytes)
        {
            // Fast path: all data is in first segment
            if (SimdCapabilities.IsAvx512FSupported && maxItems >= 8)
            {
                // AVX512 path: process 8 uint64s at once (64 bytes)
                ReadValuesAvx512(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsAvx2Supported && maxItems >= 4)
            {
                // AVX2 path: process 4 uint64s at once (32 bytes)
                ReadValuesAvx2(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsSse2Supported && maxItems >= 2)
            {
                // SSE2 path: process 2 uint64s at once (16 bytes)
                ReadValuesSse2(sequence.First.Span, destination[..maxItems]);
            }
            else
            {
                // Scalar path
                ReadValuesScalar(sequence.First.Span, destination[..maxItems]);
            }
            
            sequence = sequence.Slice(totalBytes);
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

    public override void WriteValue(IBufferWriter<byte> writer, ulong value)
    {
        var span = writer.GetSpan(sizeof(ulong));
        BinaryPrimitives.WriteUInt64LittleEndian(span, value);
        writer.Advance(sizeof(ulong));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<ulong> values)
    {
        if (values.IsEmpty)
            return;

        const int itemSize = sizeof(ulong);
        var totalBytes = values.Length * itemSize;
        var span = writer.GetSpan(totalBytes);

        if (SimdCapabilities.IsAvx512FSupported && values.Length >= 8)
        {
            // AVX512 path: process 8 uint64s at once (64 bytes)
            WriteValuesAvx512(values, span);
        }
        else if (SimdCapabilities.IsAvx2Supported && values.Length >= 4)
        {
            // AVX2 path: process 4 uint64s at once (32 bytes)
            WriteValuesAvx2(values, span);
        }
        else if (SimdCapabilities.IsSse2Supported && values.Length >= 2)
        {
            // SSE2 path: process 2 uint64s at once (16 bytes)
            WriteValuesSse2(values, span);
        }
        else
        {
            // Scalar path
            WriteValuesScalar(values, span);
        }

        writer.Advance(totalBytes);
    }
    
    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<ulong> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            // Fast path: can directly copy memory on little-endian systems
            var sourceBytes = source[..(destination.Length * sizeof(ulong))];
            var destBytes = MemoryMarshal.AsBytes(destination);
            sourceBytes.CopyTo(destBytes);
        }
        else
        {
            // Slow path: convert endianness
            for (var i = 0; i < destination.Length; i++)
            {
                destination[i] = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(i * sizeof(ulong), sizeof(ulong)));
            }
        }
    }
    
    private unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<ulong> destination)
    {
        var i = 0;
        const int vectorSize = 2; // 2 uint64s per SSE2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (ulong* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128((ulong*)(srcPtr + i * sizeof(ulong)));
                    Sse2.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<ulong> temp = stackalloc ulong[2];
            
            fixed (byte* srcPtr = source)
            fixed (ulong* destPtr = destination)
            fixed (ulong* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128((ulong*)(srcPtr + i * sizeof(ulong)));
                    // Reverse bytes within each uint64 for endianness
                    if (SimdCapabilities.IsSsse3Supported)
                    {
                        vec = Ssse3.Shuffle(vec.AsByte(), ShuffleMaskSse2).AsUInt64();
                    }
                    else
                    {
                        // Fallback for non-SSSE3
                        Sse2.Store(tempPtr, vec);
                        for (var j = 0; j < 2; j++)
                        {
                            temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                        }
                        vec = Sse2.LoadVector128(tempPtr);
                    }
                    Sse2.Store(destPtr + i, vec);
                }
            }
        }
        
        // Handle remaining elements
        if (i < destination.Length)
        {
            ReadValuesScalar(source[(i * sizeof(ulong))..], destination[i..]);
        }
    }
    
    private unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<ulong> destination)
    {
        var i = 0;
        const int vectorSize = 4; // 4 uint64s per AVX2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (ulong* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256((ulong*)(srcPtr + i * sizeof(ulong)));
                    Avx.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<ulong> temp = stackalloc ulong[4];
            
            fixed (byte* srcPtr = source)
            fixed (ulong* destPtr = destination)
            fixed (ulong* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256((ulong*)(srcPtr + i * sizeof(ulong)));
                    if (SimdCapabilities.IsAvx2Supported)
                    {
                        // Reverse bytes within each uint64 for endianness
                        vec = Avx2.Shuffle(vec.AsByte(), ShuffleMaskAvx2).AsUInt64();
                    }
                    else
                    {
                        // Fallback
                        Avx.Store(tempPtr, vec);
                        for (var j = 0; j < 4; j++)
                        {
                            temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                        }
                        vec = Avx.LoadVector256(tempPtr);
                    }
                    Avx.Store(destPtr + i, vec);
                }
            }
        }
        
        if (i >= destination.Length)
            return;

        // Handle remaining elements with SSE2 or scalar
        if (SimdCapabilities.IsSse2Supported && destination.Length - i >= 2)
        {
            ReadValuesSse2(source[(i * sizeof(ulong))..], destination[i..]);
        }
        else
        {
            ReadValuesScalar(source[(i * sizeof(ulong))..], destination[i..]);
        }
    }
    
    private unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<ulong> destination)
    {
        var i = 0;
        const int vectorSize = 8; // 8 uint64s per AVX512 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (ulong* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512((ulong*)(srcPtr + i * sizeof(ulong)));
                    Avx512F.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion
            Span<ulong> temp = stackalloc ulong[8];
            
            fixed (byte* srcPtr = source)
            fixed (ulong* destPtr = destination)
            fixed (ulong* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512((ulong*)(srcPtr + i * sizeof(ulong)));
                    // For AVX512, we could use byte shuffle with AVX512BW, but for now use uint64 operations
                    Avx512F.Store(tempPtr, vec);
                    for (var j = 0; j < 8; j++)
                    {
                        temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                    }
                    vec = Avx512F.LoadVector512(tempPtr);
                    Avx512F.Store(destPtr + i, vec);
                }
            }
        }
        
        if (i >= destination.Length)
            return;

        // Handle remaining elements with AVX2, SSE2, or scalar
        if (SimdCapabilities.IsAvx2Supported && destination.Length - i >= 4)
        {
            ReadValuesAvx2(source[(i * sizeof(ulong))..], destination[i..]);
        }
        else if (SimdCapabilities.IsSse2Supported && destination.Length - i >= 2)
        {
            ReadValuesSse2(source[(i * sizeof(ulong))..], destination[i..]);
        }
        else
        {
            ReadValuesScalar(source[(i * sizeof(ulong))..], destination[i..]);
        }
    }
    
    private static void WriteValuesScalar(ReadOnlySpan<ulong> values, Span<byte> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            // Fast path: can directly copy memory on little-endian systems
            var sourceBytes = MemoryMarshal.AsBytes(values);
            sourceBytes.CopyTo(destination);
        }
        else
        {
            // Slow path: convert endianness
            for (var i = 0; i < values.Length; i++)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(i * sizeof(ulong), sizeof(ulong)), values[i]);
            }
        }
    }
    
    private unsafe void WriteValuesSse2(ReadOnlySpan<ulong> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 2; // 2 uint64s per SSE2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (ulong* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128(srcPtr + i);
                    Sse2.Store((ulong*)(destPtr + i * sizeof(ulong)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<ulong> temp = stackalloc ulong[2];
            
            fixed (ulong* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (ulong* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128(srcPtr + i);
                    // Reverse bytes within each uint64 for endianness
                    if (SimdCapabilities.IsSsse3Supported)
                    {
                        vec = Ssse3.Shuffle(vec.AsByte(), ShuffleMaskSse2).AsUInt64();
                    }
                    else
                    {
                        // Fallback for non-SSSE3
                        Sse2.Store(tempPtr, vec);
                        for (var j = 0; j < 2; j++)
                        {
                            temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                        }
                        vec = Sse2.LoadVector128(tempPtr);
                    }
                    Sse2.Store((ulong*)(destPtr + i * sizeof(ulong)), vec);
                }
            }
        }
        
        // Handle remaining elements
        if (i < values.Length)
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(ulong))..]); 
        }
    }
    
    private unsafe void WriteValuesAvx2(ReadOnlySpan<ulong> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 4; // 4 uint64s per AVX2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (ulong* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256(srcPtr + i);
                    Avx.Store((ulong*)(destPtr + i * sizeof(ulong)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<ulong> temp = stackalloc ulong[4];
            
            fixed (ulong* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (ulong* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256(srcPtr + i);
                    if (SimdCapabilities.IsAvx2Supported)
                    {
                        // Reverse bytes within each uint64 for endianness
                        vec = Avx2.Shuffle(vec.AsByte(), ShuffleMaskAvx2).AsUInt64();
                    }
                    else
                    {
                        // Fallback
                        Avx.Store(tempPtr, vec);
                        for (var j = 0; j < 4; j++)
                        {
                            temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                        }
                        vec = Avx.LoadVector256(tempPtr);
                    }
                    Avx.Store((ulong*)(destPtr + i * sizeof(ulong)), vec);
                }
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining elements with SSE2 or scalar
        if (SimdCapabilities.IsSse2Supported && values.Length - i >= 2)
        {
            WriteValuesSse2(values[i..], destination[(i * sizeof(ulong))..]); 
        }
        else
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(ulong))..]); 
        }
    }
    
    private unsafe void WriteValuesAvx512(ReadOnlySpan<ulong> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 8; // 8 uint64s per AVX512 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (ulong* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512(srcPtr + i);
                    Avx512F.Store((ulong*)(destPtr + i * sizeof(ulong)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion
            Span<ulong> temp = stackalloc ulong[8];
            
            fixed (ulong* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (ulong* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512(srcPtr + i);
                    // For AVX512, we could use byte shuffle with AVX512BW, but for now use uint64 operations
                    Avx512F.Store(tempPtr, vec);
                    for (var j = 0; j < 8; j++)
                    {
                        temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                    }
                    vec = Avx512F.LoadVector512(tempPtr);
                    Avx512F.Store((ulong*)(destPtr + i * sizeof(ulong)), vec);
                }
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining elements with AVX2, SSE2, or scalar
        if (SimdCapabilities.IsAvx2Supported && values.Length - i >= 4)
        {
            WriteValuesAvx2(values[i..], destination[(i * sizeof(ulong))..]); 
        }
        else if (SimdCapabilities.IsSse2Supported && values.Length - i >= 2)
        {
            WriteValuesSse2(values[i..], destination[(i * sizeof(ulong))..]); 
        }
        else
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(ulong))..]); 
        }
    }
}