using System.Buffers;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse UUID type handler with SIMD-optimized serialization/deserialization.
///
/// https://clickhouse.com/docs/sql-reference/data-types/uuid
/// https://github.com/ClickHouse/ClickHouse/issues/33910
/// 
/// ClickHouse uses a specific UUID binary format that differs from .NET's Guid.ToByteArray():
/// .NET Guid bytes:       [0-3] [4-5] [6-7] [8-15]
/// ClickHouse UUID bytes: [6-7] [4-5] [0-3] [8-15 reversed]
/// 
/// This implementation provides:
/// - AVX512F: Processes 4 UUIDs simultaneously (64 bytes) using 512-bit vectors
/// - AVX2: Processes 2 UUIDs simultaneously (32 bytes) using 256-bit vectors  
/// - Scalar: Fallback for single UUID or unsupported hardware
/// </summary>
public sealed class UuidType(ISimdCapabilities simdCapabilities) : BaseClickHouseType<Guid>
{
    public static readonly UuidType Instance = new();
    
    private static readonly Vector128<byte> ShuffleMaskToClickHouse = Vector128.Create(
        (byte)6, 7, 4, 5, 0, 1, 2, 3, 15, 14, 13, 12, 11, 10, 9, 8
    );
    
    private static readonly Vector128<byte> ShuffleMaskFromClickHouse = Vector128.Create(
        (byte)4, 5, 6, 7, 2, 3, 0, 1, 15, 14, 13, 12, 11, 10, 9, 8
    );

    public ISimdCapabilities SimdCapabilities { get; } = simdCapabilities ?? throw new ArgumentNullException(nameof(simdCapabilities));

    public UuidType() : this(DefaultSimdCapabilities.Instance) { }

    public override byte ProtocolCode => 0x1D;
    public override string TypeName => "UUID";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => 16;

    public override Guid ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        const int size = 16;
        bytesConsumed = size;

        if (sequence.Length < size)
            throw new InvalidOperationException($"Insufficient data: expected {size} bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[size];
        
        if (sequence.First.Length >= size)
        {
            // Fast path: data is in a single segment
            sequence.First.Span[..size].CopyTo(buffer);
        }
        else
        {
            // Slow path: data spans multiple segments
            sequence.Slice(0, size).CopyTo(buffer);
        }

        sequence = sequence.Slice(size);
        return ConvertClickHouseUuidBytesToGuid(buffer);
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<Guid> destination, out int bytesConsumed)
    {
        const int itemSize = 16;
        var availableBytes = (int)sequence.Length;
        var maxItems = Math.Min(destination.Length, availableBytes / itemSize);
        var totalBytes = maxItems * itemSize;
        
        bytesConsumed = totalBytes;

        if (maxItems == 0)
            return 0;

        if (sequence.First.Length >= totalBytes)
        {
            // Fast path: all data is in first segment
            if (SimdCapabilities.IsAvx512FSupported && maxItems >= 4)
            {
                // AVX512 path: process 4 UUIDs at once (64 bytes)
                ReadValuesAvx512(sequence.First.Span, destination[..maxItems]);
            }
            else if (SimdCapabilities.IsAvx2Supported && maxItems >= 2)
            {
                // AVX2 path: process 2 UUIDs at once (32 bytes)
                ReadValuesAvx2(sequence.First.Span, destination[..maxItems]);
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
            // Slow path: data spans multiple segments, read individually
            var tempSequence = sequence;
            for (var i = 0; i < maxItems; i++)
            {
                destination[i] = ReadValue(ref tempSequence, out _);
            }
            sequence = tempSequence;
        }

        return maxItems;
    }

    public override void WriteValue(IBufferWriter<byte> writer, Guid value)
    {
        var span = writer.GetSpan(16);
        ConvertGuidToClickHouseUuidBytes(value, span);
        writer.Advance(16);
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<Guid> values)
    {
        if (values.IsEmpty)
            return;

        const int itemSize = 16;
        var totalBytes = values.Length * itemSize;
        var span = writer.GetSpan(totalBytes);

        if (SimdCapabilities.IsAvx512FSupported && values.Length >= 4)
        {
            // AVX512 path: process 4 UUIDs at once (64 bytes)
            WriteValuesAvx512(values, span);
        }
        else if (SimdCapabilities.IsAvx2Supported && values.Length >= 2)
        {
            // AVX2 path: process 2 UUIDs at once (32 bytes)
            WriteValuesAvx2(values, span);
        }
        else
        {
            // Scalar path
            WriteValuesScalar(values, span);
        }

        writer.Advance(totalBytes);
    }

    private void ConvertGuidToClickHouseUuidBytes(Guid guid, Span<byte> destination)
    {
        if (SimdCapabilities.IsSsse3Supported)
        {
            ConvertGuidToClickHouseUuidBytesSimd(guid, destination);
        }
        else
        {
            ConvertGuidToClickHouseUuidBytesScalar(guid, destination);
        }
    }
    
    private static unsafe void ConvertGuidToClickHouseUuidBytesSimd(Guid guid, Span<byte> destination)
    {
        Span<byte> guidBytes = stackalloc byte[16];
        guid.TryWriteBytes(guidBytes);
        
        fixed (byte* srcPtr = guidBytes)
        fixed (byte* destPtr = destination)
        {
            var vec = Sse2.LoadVector128(srcPtr);
            var shuffled = Ssse3.Shuffle(vec, ShuffleMaskToClickHouse);
            Sse2.Store(destPtr, shuffled);
        }
    }
    
    private static void ConvertGuidToClickHouseUuidBytesScalar(Guid guid, Span<byte> destination)
    {
        // Get .NET GUID bytes
        Span<byte> guidBytes = stackalloc byte[16];
        guid.TryWriteBytes(guidBytes);
        
        // Reverse last 8 bytes before shuffling (ClickHouse requirement)
        guidBytes[8..16].Reverse();
        
        // Apply ClickHouse byte shuffle pattern: [6,2], [4,2], [0,4], [8,8]
        guidBytes[6..8].CopyTo(destination[..2]);     // bytes[6,2] -> [0,2]
        guidBytes[4..6].CopyTo(destination[2..4]);    // bytes[4,2] -> [2,4] 
        guidBytes[..4].CopyTo(destination[4..8]);     // bytes[0,4] -> [4,8]
        guidBytes[8..16].CopyTo(destination[8..16]);  // bytes[8,8] -> [8,16]
    }

    private Guid ConvertClickHouseUuidBytesToGuid(ReadOnlySpan<byte> clickHouseBytes) =>
        SimdCapabilities.IsSsse3Supported
            ? ConvertClickHouseUuidBytesToGuidSimd(clickHouseBytes)
            : ConvertClickHouseUuidBytesToGuidScalar(clickHouseBytes);

    private static unsafe Guid ConvertClickHouseUuidBytesToGuidSimd(ReadOnlySpan<byte> clickHouseBytes)
    {
        Span<byte> guidBytes = stackalloc byte[16];
        
        fixed (byte* srcPtr = clickHouseBytes)
        fixed (byte* destPtr = guidBytes)
        {
            var vec = Sse2.LoadVector128(srcPtr);
            var shuffled = Ssse3.Shuffle(vec, ShuffleMaskFromClickHouse);
            Sse2.Store(destPtr, shuffled);
        }
        
        return new Guid(guidBytes);
    }
    
    private static Guid ConvertClickHouseUuidBytesToGuidScalar(ReadOnlySpan<byte> clickHouseBytes)
    {
        Span<byte> guidBytes = stackalloc byte[16];
        
        // Reverse the ClickHouse shuffle pattern: [0,2] -> [6,2], [2,4] -> [4,2], [4,8] -> [0,4], [8,16] -> [8,8]
        clickHouseBytes[..2].CopyTo(guidBytes[6..8]);     // [0,2] -> bytes[6,2]
        clickHouseBytes[2..4].CopyTo(guidBytes[4..6]);    // [2,4] -> bytes[4,2]
        clickHouseBytes[4..8].CopyTo(guidBytes[..4]);     // [4,8] -> bytes[0,4]
        clickHouseBytes[8..16].CopyTo(guidBytes[8..16]);  // [8,16] -> bytes[8,8]
        
        // Reverse last 8 bytes to restore .NET GUID format
        guidBytes[8..16].Reverse();
        
        return new Guid(guidBytes);
    }

    private void ReadValuesScalar(ReadOnlySpan<byte> source, Span<Guid> destination)
    {
        for (var i = 0; i < destination.Length; i++)
        {
            var uuidBytes = source.Slice(i * 16, 16);
            destination[i] = ConvertClickHouseUuidBytesToGuid(uuidBytes);
        }
    }

    private unsafe void ReadValuesAvx2(ReadOnlySpan<byte> source, Span<Guid> destination)
    {
        var i = 0;
        
        if (SimdCapabilities.IsSsse3Supported)
        {
            // Process 2 UUIDs at a time (32 bytes) using AVX2 with SIMD shuffle
            Span<byte> temp1 = stackalloc byte[16];
            Span<byte> temp2 = stackalloc byte[16];
            
            fixed (byte* sourcePtr = source)
            {
                for (; i + 1 < destination.Length; i += 2)
                {
                    // Load 32 bytes (2 UUIDs)
                    var vec = Avx.LoadVector256(sourcePtr + i * 16);
                    
                    // Shuffle each UUID using SIMD
                    var uuid1 = Ssse3.Shuffle(vec.GetLower(), ShuffleMaskFromClickHouse);
                    var uuid2 = Ssse3.Shuffle(vec.GetUpper(), ShuffleMaskFromClickHouse);
                    
                    fixed (byte* temp1Ptr = temp1)
                    fixed (byte* temp2Ptr = temp2)
                    {
                        Sse2.Store(temp1Ptr, uuid1);
                        Sse2.Store(temp2Ptr, uuid2);
                    }
                    
                    destination[i] = new Guid(temp1);
                    destination[i + 1] = new Guid(temp2);
                }
            }
        }
        else
        {
            // Fallback to scalar for systems without SSSE3
            for (; i + 1 < destination.Length; i += 2)
            {
                var uuidBytes1 = source.Slice(i * 16, 16);
                var uuidBytes2 = source.Slice((i + 1) * 16, 16);
                destination[i] = ConvertClickHouseUuidBytesToGuid(uuidBytes1);
                destination[i + 1] = ConvertClickHouseUuidBytesToGuid(uuidBytes2);
            }
        }
        
        // Handle remaining UUIDs with scalar approach
        ReadValuesScalar(source[(i * 16)..], destination[i..]);
    }

    private unsafe void ReadValuesAvx512(ReadOnlySpan<byte> source, Span<Guid> destination)
    {
        var i = 0;
        
        if (SimdCapabilities.IsSsse3Supported)
        {
            // Process 4 UUIDs at a time (64 bytes) using AVX512 with SIMD shuffle
            Span<byte> temp1 = stackalloc byte[16];
            Span<byte> temp2 = stackalloc byte[16];
            Span<byte> temp3 = stackalloc byte[16];
            Span<byte> temp4 = stackalloc byte[16];
            
            fixed (byte* sourcePtr = source)
            {
                for (; i + 3 < destination.Length; i += 4)
                {
                    // Load 64 bytes (4 UUIDs)
                    var vec = Avx512F.LoadVector512(sourcePtr + i * 16);
                    
                    // Extract and shuffle each UUID using SIMD
                    var uuid1 = Ssse3.Shuffle(vec.GetLower().GetLower(), ShuffleMaskFromClickHouse);
                    var uuid2 = Ssse3.Shuffle(vec.GetLower().GetUpper(), ShuffleMaskFromClickHouse);
                    var uuid3 = Ssse3.Shuffle(vec.GetUpper().GetLower(), ShuffleMaskFromClickHouse);
                    var uuid4 = Ssse3.Shuffle(vec.GetUpper().GetUpper(), ShuffleMaskFromClickHouse);
                    
                    fixed (byte* temp1Ptr = temp1)
                    fixed (byte* temp2Ptr = temp2)
                    fixed (byte* temp3Ptr = temp3)
                    fixed (byte* temp4Ptr = temp4)
                    {
                        Sse2.Store(temp1Ptr, uuid1);
                        Sse2.Store(temp2Ptr, uuid2);
                        Sse2.Store(temp3Ptr, uuid3);
                        Sse2.Store(temp4Ptr, uuid4);
                    }
                    
                    destination[i] = new Guid(temp1);
                    destination[i + 1] = new Guid(temp2);
                    destination[i + 2] = new Guid(temp3);
                    destination[i + 3] = new Guid(temp4);
                }
            }
        }
        else
        {
            // Fallback to scalar for systems without SSSE3
            for (; i + 3 < destination.Length; i += 4)
            {
                var offset = i * 16;
                destination[i] = ConvertClickHouseUuidBytesToGuid(source.Slice(offset, 16));
                destination[i + 1] = ConvertClickHouseUuidBytesToGuid(source.Slice(offset + 16, 16));
                destination[i + 2] = ConvertClickHouseUuidBytesToGuid(source.Slice(offset + 32, 16));
                destination[i + 3] = ConvertClickHouseUuidBytesToGuid(source.Slice(offset + 48, 16));
            }
        }
        
        
        if (i >= destination.Length)
            return;

        // Handle remaining UUIDs with AVX2 or scalar
        if (SimdCapabilities.IsAvx2Supported && destination.Length - i >= 2)
        {
            ReadValuesAvx2(source[(i * 16)..], destination[i..]);
        }
        else
        {
            ReadValuesScalar(source[(i * 16)..], destination[i..]);
        }
    }

    private void WriteValuesScalar(ReadOnlySpan<Guid> values, Span<byte> destination)
    {
        for (var i = 0; i < values.Length; i++)
        {
            ConvertGuidToClickHouseUuidBytes(values[i], destination.Slice(i * 16, 16));
        }
    }

    private unsafe void WriteValuesAvx2(ReadOnlySpan<Guid> values, Span<byte> destination)
    {
        var i = 0;
        
        if (SimdCapabilities.IsSsse3Supported)
        {
            // Process 2 UUIDs at a time using AVX2 with SIMD shuffle
            Span<byte> temp1 = stackalloc byte[16];
            Span<byte> temp2 = stackalloc byte[16];
            
            fixed (byte* destPtr = destination)
            {
                for (; i + 1 < values.Length; i += 2)
                {
                    // Get GUID bytes
                    values[i].TryWriteBytes(temp1);
                    values[i + 1].TryWriteBytes(temp2);
                    
                    // Load and shuffle using SIMD
                    fixed (byte* temp1Ptr = temp1)
                    fixed (byte* temp2Ptr = temp2)
                    {
                        var vec1 = Sse2.LoadVector128(temp1Ptr);
                        var vec2 = Sse2.LoadVector128(temp2Ptr);
                        
                        var shuffled1 = Ssse3.Shuffle(vec1, ShuffleMaskToClickHouse);
                        var shuffled2 = Ssse3.Shuffle(vec2, ShuffleMaskToClickHouse);
                        
                        var combined = Vector256.Create(shuffled1, shuffled2);
                        Avx.Store(destPtr + i * 16, combined);
                    }
                }
            }
        }
        else
        {
            // Fallback to scalar for systems without SSSE3
            for (; i + 1 < values.Length; i += 2)
            {
                ConvertGuidToClickHouseUuidBytes(values[i], destination.Slice(i * 16, 16));
                ConvertGuidToClickHouseUuidBytes(values[i + 1], destination.Slice((i + 1) * 16, 16));
            }
        }
        
        // Handle remaining UUIDs with scalar approach
        WriteValuesScalar(values[i..], destination[(i * 16)..]);
    }

    private unsafe void WriteValuesAvx512(ReadOnlySpan<Guid> values, Span<byte> destination)
    {
        var i = 0;
        
        if (SimdCapabilities.IsSsse3Supported)
        {
            // Process 4 UUIDs at a time using AVX512 with SIMD shuffle
            Span<byte> temp1 = stackalloc byte[16];
            Span<byte> temp2 = stackalloc byte[16];
            Span<byte> temp3 = stackalloc byte[16];
            Span<byte> temp4 = stackalloc byte[16];
            
            fixed (byte* destPtr = destination)
            {
                for (; i + 3 < values.Length; i += 4)
                {
                    // Get GUID bytes
                    values[i].TryWriteBytes(temp1);
                    values[i + 1].TryWriteBytes(temp2);
                    values[i + 2].TryWriteBytes(temp3);
                    values[i + 3].TryWriteBytes(temp4);
                    
                    // Load and shuffle using SIMD
                    fixed (byte* temp1Ptr = temp1)
                    fixed (byte* temp2Ptr = temp2)
                    fixed (byte* temp3Ptr = temp3)
                    fixed (byte* temp4Ptr = temp4)
                    {
                        var vec1 = Sse2.LoadVector128(temp1Ptr);
                        var vec2 = Sse2.LoadVector128(temp2Ptr);
                        var vec3 = Sse2.LoadVector128(temp3Ptr);
                        var vec4 = Sse2.LoadVector128(temp4Ptr);
                        
                        var shuffled1 = Ssse3.Shuffle(vec1, ShuffleMaskToClickHouse);
                        var shuffled2 = Ssse3.Shuffle(vec2, ShuffleMaskToClickHouse);
                        var shuffled3 = Ssse3.Shuffle(vec3, ShuffleMaskToClickHouse);
                        var shuffled4 = Ssse3.Shuffle(vec4, ShuffleMaskToClickHouse);
                        
                        var combined = Vector512.Create(
                            Vector256.Create(shuffled1, shuffled2),
                            Vector256.Create(shuffled3, shuffled4)
                        );
                        
                        Avx512F.Store(destPtr + i * 16, combined);
                    }
                }
            }
        }
        else
        {
            // Fallback to scalar for systems without SSSE3
            for (; i + 3 < values.Length; i += 4)
            {
                var offset = i * 16;
                ConvertGuidToClickHouseUuidBytes(values[i], destination.Slice(offset, 16));
                ConvertGuidToClickHouseUuidBytes(values[i + 1], destination.Slice(offset + 16, 16));
                ConvertGuidToClickHouseUuidBytes(values[i + 2], destination.Slice(offset + 32, 16));
                ConvertGuidToClickHouseUuidBytes(values[i + 3], destination.Slice(offset + 48, 16));
            }
        }
        
        if (i >= values.Length)
            return;

        // Handle remaining UUIDs with AVX2 or scalar
        if (SimdCapabilities.IsAvx2Supported && values.Length - i >= 2)
        {
            WriteValuesAvx2(values[i..], destination[(i * 16)..]);
        }
        else
        {
            WriteValuesScalar(values[i..], destination[(i * 16)..]);
        }
    }
}