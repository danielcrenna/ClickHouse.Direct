using System.Buffers;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Tests.Types;

public class Int64TypeTests
{
    [Fact]
    public void ReadValue_SingleValue_ReturnsCorrectValue()
    {
        // Arrange: 42 in little-endian bytes
        var bytes = new byte[] { 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int64Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(42L, result);
        Assert.Equal(8, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void ReadValue_NegativeValue_ReturnsCorrectValue()
    {
        // Arrange: -1 in little-endian bytes (0xFFFFFFFFFFFFFFFF)
        var bytes = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int64Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(-1L, result);
        Assert.Equal(8, bytesConsumed);
    }

    [Fact]
    public void ReadValue_MaxValue_ReturnsCorrectValue()
    {
        // Arrange: long.MaxValue in little-endian bytes
        var bytes = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int64Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(long.MaxValue, result);
        Assert.Equal(8, bytesConsumed);
    }

    [Fact]
    public void ReadValue_MinValue_ReturnsCorrectValue()
    {
        // Arrange: long.MinValue in little-endian bytes
        var bytes = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80 };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int64Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(long.MinValue, result);
        Assert.Equal(8, bytesConsumed);
    }

    [Fact]
    public void ReadValues_MultipleValues_ReturnsCorrectArray()
    {
        // Arrange: [1, 2, 3] in little-endian bytes
        var bytes = new byte[] 
        {
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 2
            0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  // 3
        };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int64Type.Instance;
        var destination = new long[3];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(3, itemsRead);
        Assert.Equal(24, bytesConsumed);
        Assert.Equal([1, 2, 3], destination);
        Assert.Equal(0, sequence.Length);
    }

    [Theory]
    [InlineData(2)]   // SSE2 boundary
    [InlineData(4)]   // AVX2 boundary
    [InlineData(8)]   // AVX512 boundary
    [InlineData(3)]   // Not aligned to vector size
    [InlineData(9)]   // Just over AVX512 boundary
    [InlineData(16)]  // Multiple AVX512 vectors
    [InlineData(100)] // Large batch
    public void ReadValues_VariousSizes_ReturnsCorrectArray(int count)
    {
        // Arrange
        var values = Enumerable.Range(1, count).Select(x => (long)x).ToArray();
        var bytes = new byte[count * 8];
        for (var i = 0; i < count; i++)
        {
            BitConverter.GetBytes(values[i]).CopyTo(bytes, i * 8);
        }
        
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int64Type.Instance;
        var destination = new long[count];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(count, itemsRead);
        Assert.Equal(count * 8, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void ReadValues_FragmentedSequence_ReturnsCorrectArray()
    {
        // Arrange: Create fragmented sequence
        var segment1 = new byte[] { 0x01, 0x00, 0x00, 0x00 };
        var segment2 = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x02, 0x00 };
        var segment3 = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        
        var first = new TestSequenceSegment<byte>(segment1, 0, segment1.Length);
        var second = first.Append(segment2);
        var third = second.Append(segment3);
        
        var sequence = new ReadOnlySequence<byte>(first, 0, third, segment3.Length);
        var reader = Int64Type.Instance;
        var destination = new long[2];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(2, itemsRead);
        Assert.Equal(16, bytesConsumed);
        Assert.Equal([1, 2], destination);
    }

    [Fact]
    public void WriteValue_SingleValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int64Type.Instance;

        // Act
        typeHandler.WriteValue(writer, 42L);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], result);
    }

    [Fact]
    public void WriteValue_NegativeValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int64Type.Instance;

        // Act
        typeHandler.WriteValue(writer, -1L);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], result);
    }

    [Fact]
    public void WriteValue_MaxValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int64Type.Instance;

        // Act
        typeHandler.WriteValue(writer, long.MaxValue);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F], result);
    }

    [Fact]
    public void WriteValue_MinValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int64Type.Instance;

        // Act
        typeHandler.WriteValue(writer, long.MinValue);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80], result);
    }

    [Fact]
    public void WriteValues_MultipleValues_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int64Type.Instance;
        var values = new long[] { 1, 2, 3 };

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        var expected = new byte[] 
        {
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 2
            0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  // 3
        };
        Assert.Equal(expected, writer.WrittenSpan.ToArray());
    }

    [Theory]
    [InlineData(2)]   // SSE2 boundary
    [InlineData(4)]   // AVX2 boundary
    [InlineData(8)]   // AVX512 boundary
    [InlineData(3)]   // Not aligned to vector size
    [InlineData(9)]   // Just over AVX512 boundary
    [InlineData(16)]  // Multiple AVX512 vectors
    [InlineData(100)] // Large batch
    public void WriteValues_VariousSizes_ProducesCorrectBytes(int count)
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int64Type.Instance;
        var values = Enumerable.Range(1, count).Select(x => (long)x).ToArray();

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal(count * 8, result.Length);
        
        // Verify each value
        for (var i = 0; i < count; i++)
        {
            var expectedBytes = BitConverter.GetBytes(values[i]);
            var actualBytes = result.AsSpan(i * 8, 8);
            Assert.True(expectedBytes.AsSpan().SequenceEqual(actualBytes));
        }
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(1L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    [InlineData(1234567890123456789L)]
    [InlineData(-1234567890123456789L)]
    public void RoundTrip_SingleValue_PreservesValue(long value)
    {
        // Arrange
        var typeHandler = Int64Type.Instance;
        var writer = new ArrayBufferWriter<byte>();

        // Act - Write
        typeHandler.WriteValue(writer, value);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var result = typeHandler.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(value, result);
        Assert.Equal(8, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void RoundTrip_LargeArray_PreservesAllValues()
    {
        // Arrange
        var typeHandler = Int64Type.Instance;
        var values = new long[1000];
        var random = new Random(42);
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = ((long)random.Next(int.MinValue, int.MaxValue) << 32) | (uint)random.Next();
        }

        // Act - Write
        var writer = new ArrayBufferWriter<byte>();
        typeHandler.WriteValues(writer, values);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new long[values.Length];
        var itemsRead = typeHandler.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(values.Length, itemsRead);
        Assert.Equal(values.Length * 8, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void ReadValue_InsufficientData_ThrowsException()
    {
        // Arrange
        var bytes = new byte[] { 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }; // Only 7 bytes instead of 8
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int64Type.Instance;

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(
            () => reader.ReadValue(ref sequence, out _));
        Assert.Contains("Insufficient data", exception.Message);
    }

    [Fact]
    public void Properties_ReturnCorrectValues()
    {
        // Arrange
        var typeHandler = Int64Type.Instance;

        // Assert
        Assert.Equal(0x0A, typeHandler.ProtocolCode);
        Assert.Equal("Int64", typeHandler.TypeName);
        Assert.True(typeHandler.IsFixedLength);
        Assert.Equal(8, typeHandler.FixedByteLength);
        Assert.Equal(typeof(long), typeHandler.ClrType);
    }

    [Fact]
    public void SimdCapabilities_DefaultInstance_UsesDefaultCapabilities()
    {
        // Arrange & Act
        var typeHandler = new Int64Type();

        // Assert
        Assert.NotNull(typeHandler.SimdCapabilities);
        Assert.IsType<DefaultSimdCapabilities>(typeHandler.SimdCapabilities);
    }

    [Fact]
    public void SimdCapabilities_CustomInstance_UsesProvidedCapabilities()
    {
        // Arrange
        var customCapabilities = new ConstrainedSimdCapabilities(
            DefaultSimdCapabilities.Instance,
            allowSse2: true,
            allowSsse3: false,
            allowAvx2: false,
            allowAvx512F: false
        );

        // Act
        var typeHandler = new Int64Type(customCapabilities);

        // Assert
        Assert.Same(customCapabilities, typeHandler.SimdCapabilities);
    }

    [Theory]
    [InlineData(true, false, false, false)]  // SSE2 only
    [InlineData(true, true, false, false)]   // SSE2 + SSSE3
    [InlineData(true, true, true, false)]    // SSE2 + SSSE3 + AVX2
    [InlineData(true, true, true, true)]     // All SIMD
    [InlineData(false, false, false, false)] // No SIMD
    public void SimdPaths_DifferentCapabilities_ProduceSameResults(
        bool sse2, bool ssse3, bool avx2, bool avx512)
    {
        // Arrange
        var capabilities = new ConstrainedSimdCapabilities(
            DefaultSimdCapabilities.Instance,
            allowSse2: sse2,
            allowSsse3: ssse3,
            allowAvx2: avx2,
            allowAvx512F: avx512
        );
        var typeHandler = new Int64Type(capabilities);
        
        var values = Enumerable.Range(1, 100).Select(x => (long)x).ToArray();
        var writer = new ArrayBufferWriter<byte>();

        // Act - Write
        typeHandler.WriteValues(writer, values);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new long[values.Length];
        var itemsRead = typeHandler.ReadValues(ref sequence, destination, out _);

        // Assert
        Assert.Equal(values.Length, itemsRead);
        Assert.Equal(values, destination);
    }

    private sealed class TestSequenceSegment<T> : ReadOnlySequenceSegment<T>
    {
        public TestSequenceSegment(ReadOnlyMemory<T> memory, long runningIndex, int length)
        {
            Memory = memory[..length];
            RunningIndex = runningIndex;
        }

        public TestSequenceSegment<T> Append(ReadOnlyMemory<T> memory)
        {
            var segment = new TestSequenceSegment<T>(memory, RunningIndex + Memory.Length, memory.Length);
            Next = segment;
            return segment;
        }
    }
}