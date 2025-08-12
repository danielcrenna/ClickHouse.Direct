using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse UInt32 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/int-uint
/// 
/// The UInt32 data type represents unsigned 32-bit integers with a value range from 0 to 4,294,967,295.
/// Values are stored in little-endian byte order for RowBinary format (4 bytes per value).
/// 
/// This implementation provides:
/// - AVX512F: Processes 16 UInt32 values simultaneously (64 bytes) using 512-bit vectors
/// - AVX2: Processes 8 UInt32 values simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 4 UInt32 values simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class UInt32Type(ISimdCapabilities simdCapabilities) : BaseClickHouseType<uint>
{
    public static readonly UInt32Type Instance = new();
    
    private static readonly Vector128<byte> ShuffleMaskSse2 = Vector128.Create(
        (byte)3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12
    );
    
    private static readonly Vector256<byte> ShuffleMaskAvx2 = Vector256.Create(
        (byte)3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12,
        19, 18, 17, 16, 23, 22, 21, 20, 27, 26, 25, 24, 31, 30, 29, 28
    );
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public UInt32Type() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x03;
    public override string TypeName => "UInt32";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(uint);

    public override uint ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        const int size = sizeof(uint);
        bytesConsumed = size;

        if (sequence.Length < size)
            throw new InvalidOperationException($"Insufficient data: expected {size} bytes, got {sequence.Length}");

        if (sequence.First.Length >= size)
        {
            // Fast path: data is in a single segment
            var value = BinaryPrimitives.ReadUInt32LittleEndian(sequence.First.Span);
            sequence = sequence.Slice(size);
            return value;
        }

        // Slow path: data spans multiple segments
        Span<byte> buffer = stackalloc byte[size];
        sequence.Slice(0, size).CopyTo(buffer);
        sequence = sequence.Slice(size);
        return BinaryPrimitives.ReadUInt32LittleEndian(buffer);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<uint> destination, out int bytesConsumed)
    {
        const int itemSize = sizeof(uint);
        var availableBytes = (int)sequence.Length;
        var maxItems = Math.Min(destination.Length, availableBytes / itemSize);
        var totalBytes = maxItems * itemSize;
        
        bytesConsumed = totalBytes;

        if (maxItems == 0)
            return 0;

        if (sequence.First.Length >= totalBytes)
        {
            // Fast path: all data is in first segment
            if (SimdCapabilities.IsAvx512FSupported && maxItems >= 16)
            {
                // AVX512 path: process 16 uint32s at once (64 bytes)
                ReadValuesAvx512(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsAvx2Supported && maxItems >= 8)
            {
                // AVX2 path: process 8 uint32s at once (32 bytes)
                ReadValuesAvx2(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsSse2Supported && maxItems >= 4)
            {
                // SSE2 path: process 4 uint32s at once (16 bytes)
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

    public override void WriteValue(IBufferWriter<byte> writer, uint value)
    {
        var span = writer.GetSpan(sizeof(uint));
        BinaryPrimitives.WriteUInt32LittleEndian(span, value);
        writer.Advance(sizeof(uint));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<uint> values)
    {
        if (values.IsEmpty)
            return;

        const int itemSize = sizeof(uint);
        var totalBytes = values.Length * itemSize;
        var span = writer.GetSpan(totalBytes);

        if (SimdCapabilities.IsAvx512FSupported && values.Length >= 16)
        {
            // AVX512 path: process 16 uint32s at once (64 bytes)
            WriteValuesAvx512(values, span);
        }
        else if (SimdCapabilities.IsAvx2Supported && values.Length >= 8)
        {
            // AVX2 path: process 8 uint32s at once (32 bytes)
            WriteValuesAvx2(values, span);
        }
        else if (SimdCapabilities.IsSse2Supported && values.Length >= 4)
        {
            // SSE2 path: process 4 uint32s at once (16 bytes)
            WriteValuesSse2(values, span);
        }
        else
        {
            // Scalar path
            WriteValuesScalar(values, span);
        }

        writer.Advance(totalBytes);
    }
    
    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<uint> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            // Fast path: can directly copy memory on little-endian systems
            var sourceBytes = source[..(destination.Length * sizeof(uint))];
            var destBytes = MemoryMarshal.AsBytes(destination);
            sourceBytes.CopyTo(destBytes);
        }
        else
        {
            // Slow path: convert endianness
            for (var i = 0; i < destination.Length; i++)
            {
                destination[i] = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(i * sizeof(uint), sizeof(uint)));
            }
        }
    }
    
    private unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<uint> destination)
    {
        var i = 0;
        const int vectorSize = 4; // 4 uint32s per SSE2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (uint* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128((uint*)(srcPtr + i * sizeof(uint)));
                    Sse2.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<uint> temp = stackalloc uint[4];
            
            fixed (byte* srcPtr = source)
            fixed (uint* destPtr = destination)
            fixed (uint* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128((uint*)(srcPtr + i * sizeof(uint)));
                    // Reverse bytes within each uint32 for endianness
                    if (SimdCapabilities.IsSsse3Supported)
                    {
                        vec = Ssse3.Shuffle(vec.AsByte(), ShuffleMaskSse2).AsUInt32();
                    }
                    else
                    {
                        // Fallback for non-SSSE3
                        Sse2.Store(tempPtr, vec);
                        for (var j = 0; j < 4; j++)
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
            ReadValuesScalar(source[(i * sizeof(uint))..], destination[i..]);
        }
    }
    
    private unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<uint> destination)
    {
        var i = 0;
        const int vectorSize = 8; // 8 uint32s per AVX2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (uint* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256((uint*)(srcPtr + i * sizeof(uint)));
                    Avx.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<uint> temp = stackalloc uint[8];
            
            fixed (byte* srcPtr = source)
            fixed (uint* destPtr = destination)
            fixed (uint* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256((uint*)(srcPtr + i * sizeof(uint)));
                    if (SimdCapabilities.IsAvx2Supported)
                    {
                        // Reverse bytes within each uint32 for endianness
                        vec = Avx2.Shuffle(vec.AsByte(), ShuffleMaskAvx2).AsUInt32();
                    }
                    else
                    {
                        // Fallback
                        Avx.Store(tempPtr, vec);
                        for (var j = 0; j < 8; j++)
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
        if (SimdCapabilities.IsSse2Supported && destination.Length - i >= 4)
        {
            ReadValuesSse2(source[(i * sizeof(uint))..], destination[i..]);
        }
        else
        {
            ReadValuesScalar(source[(i * sizeof(uint))..], destination[i..]);
        }
    }
    
    private unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<uint> destination)
    {
        var i = 0;
        const int vectorSize = 16; // 16 uint32s per AVX512 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (uint* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512((uint*)(srcPtr + i * sizeof(uint)));
                    Avx512F.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<uint> temp = stackalloc uint[16];
            
            fixed (byte* srcPtr = source)
            fixed (uint* destPtr = destination)
            fixed (uint* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512((uint*)(srcPtr + i * sizeof(uint)));
                    // For AVX512BW, we could use byte shuffle, but for now use uint32 operations
                    // This would need AVX512BW support which we'll handle separately
                    Avx512F.Store(tempPtr, vec);
                    for (var j = 0; j < 16; j++)
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
        if (SimdCapabilities.IsAvx2Supported && destination.Length - i >= 8)
        {
            ReadValuesAvx2(source[(i * sizeof(uint))..], destination[i..]);
        }
        else if (SimdCapabilities.IsSse2Supported && destination.Length - i >= 4)
        {
            ReadValuesSse2(source[(i * sizeof(uint))..], destination[i..]);
        }
        else
        {
            ReadValuesScalar(source[(i * sizeof(uint))..], destination[i..]);
        }
    }
    
    private static void WriteValuesScalar(ReadOnlySpan<uint> values, Span<byte> destination)
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
                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(i * sizeof(uint), sizeof(uint)), values[i]);
            }
        }
    }
    
    private unsafe void WriteValuesSse2(ReadOnlySpan<uint> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 4; // 4 uint32s per SSE2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (uint* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128(srcPtr + i);
                    Sse2.Store((uint*)(destPtr + i * sizeof(uint)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<uint> temp = stackalloc uint[4];
            
            fixed (uint* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (uint* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128(srcPtr + i);
                    // Reverse bytes within each uint32 for endianness
                    if (SimdCapabilities.IsSsse3Supported)
                    {
                        vec = Ssse3.Shuffle(vec.AsByte(), ShuffleMaskSse2).AsUInt32();
                    }
                    else
                    {
                        // Fallback for non-SSSE3
                        Sse2.Store(tempPtr, vec);
                        for (var j = 0; j < 4; j++)
                        {
                            temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                        }
                        vec = Sse2.LoadVector128(tempPtr);
                    }
                    Sse2.Store((uint*)(destPtr + i * sizeof(uint)), vec);
                }
            }
        }
        
        // Handle remaining elements
        if (i < values.Length)
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(uint))..]); 
        }
    }
    
    private unsafe void WriteValuesAvx2(ReadOnlySpan<uint> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 8; // 8 uint32s per AVX2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (uint* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256(srcPtr + i);
                    Avx.Store((uint*)(destPtr + i * sizeof(uint)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<uint> temp = stackalloc uint[8];
            
            fixed (uint* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (uint* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256(srcPtr + i);
                    if (SimdCapabilities.IsAvx2Supported)
                    {
                        // Reverse bytes within each uint32 for endianness
                        vec = Avx2.Shuffle(vec.AsByte(), ShuffleMaskAvx2).AsUInt32();
                    }
                    else
                    {
                        // Fallback
                        Avx.Store(tempPtr, vec);
                        for (var j = 0; j < 8; j++)
                        {
                            temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                        }
                        vec = Avx.LoadVector256(tempPtr);
                    }
                    Avx.Store((uint*)(destPtr + i * sizeof(uint)), vec);
                }
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining elements with SSE2 or scalar
        if (SimdCapabilities.IsSse2Supported && values.Length - i >= 4)
        {
            WriteValuesSse2(values[i..], destination[(i * sizeof(uint))..]); 
        }
        else
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(uint))..]); 
        }
    }
    
    private unsafe void WriteValuesAvx512(ReadOnlySpan<uint> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 16; // 16 uint32s per AVX512 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (uint* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512(srcPtr + i);
                    Avx512F.Store((uint*)(destPtr + i * sizeof(uint)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<uint> temp = stackalloc uint[16];
            
            fixed (uint* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (uint* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512(srcPtr + i);
                    // For AVX512BW, we could use byte shuffle, but for now use uint32 operations
                    Avx512F.Store(tempPtr, vec);
                    for (var j = 0; j < 16; j++)
                    {
                        temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                    }
                    vec = Avx512F.LoadVector512(tempPtr);
                    Avx512F.Store((uint*)(destPtr + i * sizeof(uint)), vec);
                }
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining elements with AVX2, SSE2, or scalar
        if (SimdCapabilities.IsAvx2Supported && values.Length - i >= 8)
        {
            WriteValuesAvx2(values[i..], destination[(i * sizeof(uint))..]); 
        }
        else if (SimdCapabilities.IsSse2Supported && values.Length - i >= 4)
        {
            WriteValuesSse2(values[i..], destination[(i * sizeof(uint))..]); 
        }
        else
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(uint))..]); 
        }
    }
}