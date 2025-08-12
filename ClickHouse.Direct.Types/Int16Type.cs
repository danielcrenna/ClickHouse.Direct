using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse Int16 type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/int-uint
/// 
/// The Int16 data type represents signed 16-bit integers with a value range from -32,768 to 32,767.
/// Values are stored in little-endian byte order for RowBinary format (2 bytes per value).
/// 
/// This implementation provides:
/// - AVX512F: Processes 32 Int16 values simultaneously (64 bytes) using 512-bit vectors
/// - AVX2: Processes 16 Int16 values simultaneously (32 bytes) using 256-bit vectors
/// - SSE2: Processes 8 Int16 values simultaneously (16 bytes) using 128-bit vectors
/// - Scalar fallback: Handles unaligned data and platforms without SIMD support
/// - Automatic endianness conversion for big-endian systems
/// </summary>
public sealed class Int16Type(ISimdCapabilities simdCapabilities) : BaseClickHouseType<short>
{
    public static readonly Int16Type Instance = new();
    
    private static readonly Vector128<byte> ShuffleMaskSse2 = Vector128.Create(
        (byte)1, 0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14
    );
    
    private static readonly Vector256<byte> ShuffleMaskAvx2 = Vector256.Create(
        (byte)1, 0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14,
        17, 16, 19, 18, 21, 20, 23, 22, 25, 24, 27, 26, 29, 28, 31, 30
    );
    
    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public Int16Type() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x08;
    public override string TypeName => "Int16";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => sizeof(short);

    public override short ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        const int size = sizeof(short);
        bytesConsumed = size;

        if (sequence.Length < size)
            throw new InvalidOperationException($"Insufficient data: expected {size} bytes, got {sequence.Length}");

        if (sequence.First.Length >= size)
        {
            // Fast path: data is in a single segment
            var value = BinaryPrimitives.ReadInt16LittleEndian(sequence.First.Span);
            sequence = sequence.Slice(size);
            return value;
        }

        // Slow path: data spans multiple segments
        Span<byte> buffer = stackalloc byte[size];
        sequence.Slice(0, size).CopyTo(buffer);
        sequence = sequence.Slice(size);
        return BinaryPrimitives.ReadInt16LittleEndian(buffer);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<short> destination, out int bytesConsumed)
    {
        const int itemSize = sizeof(short);
        var availableBytes = (int)sequence.Length;
        var maxItems = Math.Min(destination.Length, availableBytes / itemSize);
        var totalBytes = maxItems * itemSize;
        
        bytesConsumed = totalBytes;

        if (maxItems == 0)
            return 0;

        if (sequence.First.Length >= totalBytes)
        {
            // Fast path: all data is in first segment
            if (SimdCapabilities.IsAvx512BwSupported && maxItems >= 32)
            {
                // AVX512BW path: process 32 int16s at once (64 bytes)
                ReadValuesAvx512(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsAvx2Supported && maxItems >= 16)
            {
                // AVX2 path: process 16 int16s at once (32 bytes)
                ReadValuesAvx2(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsSse2Supported && maxItems >= 8)
            {
                // SSE2 path: process 8 int16s at once (16 bytes)
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

    public override void WriteValue(IBufferWriter<byte> writer, short value)
    {
        var span = writer.GetSpan(sizeof(short));
        BinaryPrimitives.WriteInt16LittleEndian(span, value);
        writer.Advance(sizeof(short));
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<short> values)
    {
        if (values.IsEmpty)
            return;

        const int itemSize = sizeof(short);
        var totalBytes = values.Length * itemSize;
        var span = writer.GetSpan(totalBytes);

        if (SimdCapabilities.IsAvx512BwSupported && values.Length >= 32)
        {
            // AVX512BW path: process 32 int16s at once (64 bytes)
            WriteValuesAvx512(values, span);
        }
        else if (SimdCapabilities.IsAvx2Supported && values.Length >= 16)
        {
            // AVX2 path: process 16 int16s at once (32 bytes)
            WriteValuesAvx2(values, span);
        }
        else if (SimdCapabilities.IsSse2Supported && values.Length >= 8)
        {
            // SSE2 path: process 8 int16s at once (16 bytes)
            WriteValuesSse2(values, span);
        }
        else
        {
            // Scalar path
            WriteValuesScalar(values, span);
        }

        writer.Advance(totalBytes);
    }
    
    private static void ReadValuesScalar(ReadOnlySpan<byte> source, Span<short> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            // Fast path: can directly copy memory on little-endian systems
            var sourceBytes = source[..(destination.Length * sizeof(short))];
            var destBytes = MemoryMarshal.AsBytes(destination);
            sourceBytes.CopyTo(destBytes);
        }
        else
        {
            // Slow path: convert endianness
            for (var i = 0; i < destination.Length; i++)
            {
                destination[i] = BinaryPrimitives.ReadInt16LittleEndian(source.Slice(i * sizeof(short), sizeof(short)));
            }
        }
    }
    
    private unsafe void ReadValuesSse2(ReadOnlySpan<byte> source, Span<short> destination)
    {
        var i = 0;
        const int vectorSize = 8; // 8 int16s per SSE2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (short* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128((short*)(srcPtr + i * sizeof(short)));
                    Sse2.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<short> temp = stackalloc short[8];
            
            fixed (byte* srcPtr = source)
            fixed (short* destPtr = destination)
            fixed (short* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128((short*)(srcPtr + i * sizeof(short)));
                    // Reverse bytes within each int16 for endianness
                    if (SimdCapabilities.IsSsse3Supported)
                    {
                        vec = Ssse3.Shuffle(vec.AsByte(), ShuffleMaskSse2).AsInt16();
                    }
                    else
                    {
                        // Fallback for non-SSSE3
                        Sse2.Store(tempPtr, vec);
                        for (var j = 0; j < 8; j++)
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
            ReadValuesScalar(source[(i * sizeof(short))..], destination[i..]);
        }
    }
    
    private unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<short> destination)
    {
        var i = 0;
        const int vectorSize = 16; // 16 int16s per AVX2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (short* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256((short*)(srcPtr + i * sizeof(short)));
                    Avx.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<short> temp = stackalloc short[16];
            
            fixed (byte* srcPtr = source)
            fixed (short* destPtr = destination)
            fixed (short* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256((short*)(srcPtr + i * sizeof(short)));
                    if (SimdCapabilities.IsAvx2Supported)
                    {
                        // Reverse bytes within each int16 for endianness
                        vec = Avx2.Shuffle(vec.AsByte(), ShuffleMaskAvx2).AsInt16();
                    }
                    else
                    {
                        // Fallback
                        Avx.Store(tempPtr, vec);
                        for (var j = 0; j < 16; j++)
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
        if (SimdCapabilities.IsSse2Supported && destination.Length - i >= 8)
        {
            ReadValuesSse2(source[(i * sizeof(short))..], destination[i..]);
        }
        else
        {
            ReadValuesScalar(source[(i * sizeof(short))..], destination[i..]);
        }
    }
    
    private unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<short> destination)
    {
        var i = 0;
        const int vectorSize = 32; // 32 int16s per AVX512 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (byte* srcPtr = source)
            fixed (short* destPtr = destination)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512((short*)(srcPtr + i * sizeof(short)));
                    Avx512F.Store(destPtr + i, vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion
            Span<short> temp = stackalloc short[32];
            
            fixed (byte* srcPtr = source)
            fixed (short* destPtr = destination)
            fixed (short* tempPtr = temp)
            {
                for (; i + vectorSize <= destination.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512((short*)(srcPtr + i * sizeof(short)));
                    // For AVX512BW, we could use byte shuffle, but for now use int16 operations
                    Avx512F.Store(tempPtr, vec);
                    for (var j = 0; j < 32; j++)
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
        if (SimdCapabilities.IsAvx2Supported && destination.Length - i >= 16)
        {
            ReadValuesAvx2(source[(i * sizeof(short))..], destination[i..]);
        }
        else if (SimdCapabilities.IsSse2Supported && destination.Length - i >= 8)
        {
            ReadValuesSse2(source[(i * sizeof(short))..], destination[i..]);
        }
        else
        {
            ReadValuesScalar(source[(i * sizeof(short))..], destination[i..]);
        }
    }
    
    private static void WriteValuesScalar(ReadOnlySpan<short> values, Span<byte> destination)
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
                BinaryPrimitives.WriteInt16LittleEndian(destination.Slice(i * sizeof(short), sizeof(short)), values[i]);
            }
        }
    }
    
    private unsafe void WriteValuesSse2(ReadOnlySpan<short> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 8; // 8 int16s per SSE2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (short* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128(srcPtr + i);
                    Sse2.Store((short*)(destPtr + i * sizeof(short)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<short> temp = stackalloc short[8];
            
            fixed (short* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (short* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Sse2.LoadVector128(srcPtr + i);
                    // Reverse bytes within each int16 for endianness
                    if (SimdCapabilities.IsSsse3Supported)
                    {
                        vec = Ssse3.Shuffle(vec.AsByte(), ShuffleMaskSse2).AsInt16();
                    }
                    else
                    {
                        // Fallback for non-SSSE3
                        Sse2.Store(tempPtr, vec);
                        for (var j = 0; j < 8; j++)
                        {
                            temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                        }
                        vec = Sse2.LoadVector128(tempPtr);
                    }
                    Sse2.Store((short*)(destPtr + i * sizeof(short)), vec);
                }
            }
        }
        
        // Handle remaining elements
        if (i < values.Length)
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(short))..]); 
        }
    }
    
    private unsafe void WriteValuesAvx2(ReadOnlySpan<short> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 16; // 16 int16s per AVX2 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (short* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256(srcPtr + i);
                    Avx.Store((short*)(destPtr + i * sizeof(short)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion with SIMD
            Span<short> temp = stackalloc short[16];
            
            fixed (short* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (short* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx.LoadVector256(srcPtr + i);
                    if (SimdCapabilities.IsAvx2Supported)
                    {
                        // Reverse bytes within each int16 for endianness
                        vec = Avx2.Shuffle(vec.AsByte(), ShuffleMaskAvx2).AsInt16();
                    }
                    else
                    {
                        // Fallback
                        Avx.Store(tempPtr, vec);
                        for (var j = 0; j < 16; j++)
                        {
                            temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                        }
                        vec = Avx.LoadVector256(tempPtr);
                    }
                    Avx.Store((short*)(destPtr + i * sizeof(short)), vec);
                }
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining elements with SSE2 or scalar
        if (SimdCapabilities.IsSse2Supported && values.Length - i >= 8)
        {
            WriteValuesSse2(values[i..], destination[(i * sizeof(short))..]); 
        }
        else
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(short))..]); 
        }
    }
    
    private unsafe void WriteValuesAvx512(ReadOnlySpan<short> values, Span<byte> destination)
    {
        var i = 0;
        const int vectorSize = 32; // 32 int16s per AVX512 vector
        
        if (BitConverter.IsLittleEndian)
        {
            fixed (short* srcPtr = values)
            fixed (byte* destPtr = destination)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512(srcPtr + i);
                    Avx512F.Store((short*)(destPtr + i * sizeof(short)), vec);
                }
            }
        }
        else
        {
            // Handle endianness conversion
            Span<short> temp = stackalloc short[32];
            
            fixed (short* srcPtr = values)
            fixed (byte* destPtr = destination)
            fixed (short* tempPtr = temp)
            {
                for (; i + vectorSize <= values.Length; i += vectorSize)
                {
                    var vec = Avx512F.LoadVector512(srcPtr + i);
                    // For AVX512BW, we could use byte shuffle, but for now use int16 operations
                    Avx512F.Store(tempPtr, vec);
                    for (var j = 0; j < 32; j++)
                    {
                        temp[j] = BinaryPrimitives.ReverseEndianness(temp[j]);
                    }
                    vec = Avx512F.LoadVector512(tempPtr);
                    Avx512F.Store((short*)(destPtr + i * sizeof(short)), vec);
                }
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining elements with AVX2, SSE2, or scalar
        if (SimdCapabilities.IsAvx2Supported && values.Length - i >= 16)
        {
            WriteValuesAvx2(values[i..], destination[(i * sizeof(short))..]); 
        }
        else if (SimdCapabilities.IsSse2Supported && values.Length - i >= 8)
        {
            WriteValuesSse2(values[i..], destination[(i * sizeof(short))..]); 
        }
        else
        {
            WriteValuesScalar(values[i..], destination[(i * sizeof(short))..]); 
        }
    }
}