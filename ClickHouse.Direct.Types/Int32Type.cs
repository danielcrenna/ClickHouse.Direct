using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse Int32 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/int-uint
/// 
/// The Int32 data type represents signed 32-bit integers with a value range from -2,147,483,648 to 2,147,483,647.
/// Values are stored in little-endian byte order for RowBinary format (4 bytes per value).
/// 
/// This implementation provides:
/// - AVX512F: Processes 16 Int32 values simultaneously (64 bytes) using 512-bit vectors
/// - AVX2: Processes 8 Int32 values simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 4 Int32 values simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class Int32Type(ISimdCapabilities simdCapabilities) : BaseClickHouseType<int>
{
    public static readonly Int32Type Instance = new();
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public Int32Type() : this(DefaultSimdCapabilities.Instance) { }

    public override ClickHouseDataType DataType => ClickHouseDataType.Int32;
    public override string TypeName => "Int32";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(int);

    public override int ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        const int size = sizeof(int);
        bytesConsumed = size;

        if (sequence.Length < size)
            throw new InvalidOperationException($"Insufficient data: expected {size} bytes, got {sequence.Length}");

        if (sequence.First.Length >= size)
        {
            // Fast path: data is in a single segment
            var value = BinaryPrimitives.ReadInt32LittleEndian(sequence.First.Span);
            sequence = sequence.Slice(size);
            return value;
        }

        // Slow path: data spans multiple segments
        Span<byte> buffer = stackalloc byte[size];
        sequence.Slice(0, size).CopyTo(buffer);
        sequence = sequence.Slice(size);
        return BinaryPrimitives.ReadInt32LittleEndian(buffer);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<int> destination, out int bytesConsumed)
    {
        const int itemSize = sizeof(int);
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
                // AVX512 path: process 16 int32s at once (64 bytes)
                ReadValuesAvx512(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsAvx2Supported && maxItems >= 8)
            {
                // AVX2 path: process 8 int32s at once (32 bytes)
                ReadValuesAvx2(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsSse2Supported && maxItems >= 4)
            {
                // SSE2 path: process 4 int32s at once (16 bytes)
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
            // sequence has already been advanced by ReadValue calls
        }

        return maxItems;
    }

    public override void WriteValue(IBufferWriter<byte> writer, int value)
    {
        var span = writer.GetSpan(sizeof(int));
        BinaryPrimitives.WriteInt32LittleEndian(span, value);
        writer.Advance(sizeof(int));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<int> values)
    {
        if (values.IsEmpty)
            return;

        const int itemSize = sizeof(int);
        var totalBytes = values.Length * itemSize;
        var span = writer.GetSpan(totalBytes);

        if (SimdCapabilities.IsAvx512FSupported && values.Length >= 16)
        {
            // AVX512 path: process 16 int32s at once (64 bytes)
            WriteValuesAvx512(values, span);
        }
        else if (SimdCapabilities.IsAvx2Supported && values.Length >= 8)
        {
            // AVX2 path: process 8 int32s at once (32 bytes)
            WriteValuesAvx2(values, span);
        }
        else if (SimdCapabilities.IsSse2Supported && values.Length >= 4)
        {
            // SSE2 path: process 4 int32s at once (16 bytes)
            WriteValuesSse2(values, span);
        }
        else
        {
            // Scalar path
            WriteValuesScalar(values, span);
        }

        writer.Advance(totalBytes);
    }
    
    private void ReadValuesScalar(ReadOnlySpan<byte> source, Span<int> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            // Fast path: can directly copy memory on little-endian systems
            var sourceBytes = source[..(destination.Length * sizeof(int))];
            var destBytes = MemoryMarshal.AsBytes(destination);
            sourceBytes.CopyTo(destBytes);
        }
        else
        {
            // Slow path: convert endianness
            for (var i = 0; i < destination.Length; i++)
            {
                destination[i] = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(i * sizeof(int), sizeof(int)));
            }
        }
    }
    
    private unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<int> destination)
    {
        var i = 0;
        const int vectorSize = 4; // 4 int32s per SSE2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (int* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128((int*)(srcPtr + i * sizeof(int)));
                    Sse2.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<int> temp = stackalloc int[4];
            var shuffleMask = Vector128.Create(
                (byte)3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12
            );
            
            fixed (byte* srcPtr = source)
            fixed (int* destPtr = destination)
            fixed (int* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128((int*)(srcPtr + i * sizeof(int)));
                    // Reverse bytes within each int32 for endianness
                    if (SimdCapabilities.IsSsse3Supported)
                    {
                        vec = Ssse3.Shuffle(vec.AsByte(), shuffleMask).AsInt32();
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
            ReadValuesScalar(source[(i * sizeof(int))..], destination[i..]);
        }
    }
    
    private unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<int> destination)
    {
        var i = 0;
        const int vectorSize = 8; // 8 int32s per AVX2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (int* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256((int*)(srcPtr + i * sizeof(int)));
                    Avx.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<int> temp = stackalloc int[8];
            var shuffleMask = Vector256.Create(
                (byte)3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12,
                3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12
            );
            
            fixed (byte* srcPtr = source)
            fixed (int* destPtr = destination)
            fixed (int* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256((int*)(srcPtr + i * sizeof(int)));
                    if (SimdCapabilities.IsAvx2Supported)
                    {
                        // Reverse bytes within each int32 for endianness
                        vec = Avx2.Shuffle(vec.AsByte(), shuffleMask).AsInt32();
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
        
        // Handle remaining elements with SSE2 or scalar
        if (i < destination.Length)
        {
            if (SimdCapabilities.IsSse2Supported && destination.Length - i >= 4)
            {
                ReadValuesSse2(source[(i * sizeof(int))..], destination[i..]);
            }
            else
            {
                ReadValuesScalar(source[(i * sizeof(int))..], destination[i..]);
            }
        }
    }
    
    private unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<int> destination)
    {
        var i = 0;
        const int vectorSize = 16; // 16 int32s per AVX512 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (int* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512((int*)(srcPtr + i * sizeof(int)));
                    Avx512F.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<int> temp = stackalloc int[16];
            
            fixed (byte* srcPtr = source)
            fixed (int* destPtr = destination)
            fixed (int* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512((int*)(srcPtr + i * sizeof(int)));
                    // For AVX512BW, we could use byte shuffle, but for now use int32 operations
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
            ReadValuesAvx2(source[(i * sizeof(int))..], destination[i..]);
        }
        else if (SimdCapabilities.IsSse2Supported && destination.Length - i >= 4)
        {
            ReadValuesSse2(source[(i * sizeof(int))..], destination[i..]);
        }
        else
        {
            ReadValuesScalar(source[(i * sizeof(int))..], destination[i..]);
        }
    }
    
    private static void WriteValuesScalar(ReadOnlySpan<int> values, Span<byte> destination)
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
                BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(i * sizeof(int), sizeof(int)), values[i]);
            }
        }
    }
    
    private unsafe void WriteValuesSse2(ReadOnlySpan<int> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 4; // 4 int32s per SSE2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (int* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128(srcPtr + i);
                    Sse2.Store((int*)(destPtr + i * sizeof(int)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<int> temp = stackalloc int[4];
            var shuffleMask = Vector128.Create(
                (byte)3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12
            );
            
            fixed (int* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (int* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128(srcPtr + i);
                    // Reverse bytes within each int32 for endianness
                    if (SimdCapabilities.IsSsse3Supported)
                    {
                        vec = Ssse3.Shuffle(vec.AsByte(), shuffleMask).AsInt32();
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
                    Sse2.Store((int*)(destPtr + i * sizeof(int)), vec);
                }
            }
        }
        
        // Handle remaining elements
        if (i < values.Length)
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(int))..]); 
        }
    }
    
    private unsafe void WriteValuesAvx2(ReadOnlySpan<int> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 8; // 8 int32s per AVX2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (int* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256(srcPtr + i);
                    Avx.Store((int*)(destPtr + i * sizeof(int)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<int> temp = stackalloc int[8];
            var shuffleMask = Vector256.Create(
                (byte)3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12,
                3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12
            );
            
            fixed (int* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (int* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256(srcPtr + i);
                    if (SimdCapabilities.IsAvx2Supported)
                    {
                        // Reverse bytes within each int32 for endianness
                        vec = Avx2.Shuffle(vec.AsByte(), shuffleMask).AsInt32();
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
                    Avx.Store((int*)(destPtr + i * sizeof(int)), vec);
                }
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining elements with SSE2 or scalar
        if (SimdCapabilities.IsSse2Supported && values.Length - i >= 4)
        {
            WriteValuesSse2(values[i..], destination[(i * sizeof(int))..]); 
        }
        else
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(int))..]); 
        }
    }
    
    private unsafe void WriteValuesAvx512(ReadOnlySpan<int> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 16; // 16 int32s per AVX512 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (int* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512(srcPtr + i);
                    Avx512F.Store((int*)(destPtr + i * sizeof(int)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<int> temp = stackalloc int[16];
            
            fixed (int* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (int* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512(srcPtr + i);
                    // For AVX512BW, we could use byte shuffle, but for now use int32 operations
                    Avx512F.Store(tempPtr, vec);
                    for (var j = 0; j < 16; j++)
                    {
                        temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                    }
                    vec = Avx512F.LoadVector512(tempPtr);
                    Avx512F.Store((int*)(destPtr + i * sizeof(int)), vec);
                }
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining elements with AVX2, SSE2, or scalar

        if (SimdCapabilities.IsAvx2Supported && values.Length - i >= 8)
        {
            WriteValuesAvx2(values[i..], destination[(i * sizeof(int))..]); 
        }
        else if (SimdCapabilities.IsSse2Supported && values.Length - i >= 4)
        {
            WriteValuesSse2(values[i..], destination[(i * sizeof(int))..]); 
        }
        else
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(int))..]); 
        }
    }
}