using System.Buffers;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types.Tests;

public class Int32TypeTests
{
    [Fact]
    public void ReadValue_SingleValue_ReturnsCorrectInt32()
    {
        // Arrange: 42 in little-endian bytes
        var bytes = new byte[] { 0x2A, 0x00, 0x00, 0x00 };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int32Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(42, result);
        Assert.Equal(4, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void ReadValue_NegativeValue_ReturnsCorrectInt32()
    {
        // Arrange: -1 in little-endian bytes (0xFFFFFFFF)
        var bytes = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int32Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(-1, result);
        Assert.Equal(4, bytesConsumed);
    }

    [Fact]
    public void ReadValue_MaxValue_ReturnsCorrectInt32()
    {
        // Arrange: int.MaxValue (2147483647) in little-endian bytes
        var bytes = new byte[] { 0xFF, 0xFF, 0xFF, 0x7F };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int32Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(int.MaxValue, result);
        Assert.Equal(4, bytesConsumed);
    }

    [Fact]
    public void ReadValue_MinValue_ReturnsCorrectInt32()
    {
        // Arrange: int.MinValue (-2147483648) in little-endian bytes
        var bytes = new byte[] { 0x00, 0x00, 0x00, 0x80 };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int32Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(int.MinValue, result);
        Assert.Equal(4, bytesConsumed);
    }

    [Fact]
    public void ReadValues_MultipleValues_ReturnsCorrectArray()
    {
        // Arrange: [1, 2, 3] in little-endian bytes
        var bytes = new byte[] 
        {
            0x01, 0x00, 0x00, 0x00, // 1
            0x02, 0x00, 0x00, 0x00, // 2
            0x03, 0x00, 0x00, 0x00  // 3
        };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int32Type.Instance;
        var destination = new int[3];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(3, itemsRead);
        Assert.Equal(12, bytesConsumed);
        Assert.Equal([1, 2, 3], destination);
        Assert.Equal(0, sequence.Length);
    }

    [Theory]
    [InlineData(4)]   // SSE2 boundary
    [InlineData(8)]   // AVX2 boundary
    [InlineData(16)]  // AVX512 boundary
    [InlineData(5)]   // Not aligned to vector size
    [InlineData(17)]  // Just over AVX512 boundary
    [InlineData(32)]  // Multiple AVX512 vectors
    [InlineData(100)] // Large batch
    public void ReadValues_VariousSizes_ReturnsCorrectArray(int count)
    {
        // Arrange
        var values = Enumerable.Range(1, count).ToArray();
        var bytes = new byte[count * 4];
        for (var i = 0; i < count; i++)
        {
            BitConverter.GetBytes(values[i]).CopyTo(bytes, i * 4);
        }
        
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int32Type.Instance;
        var destination = new int[count];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(count, itemsRead);
        Assert.Equal(count * 4, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void ReadValues_FragmentedSequence_ReturnsCorrectArray()
    {
        // Arrange: Create fragmented sequence
        var segment1 = new byte[] { 0x01, 0x00 };
        var segment2 = new byte[] { 0x00, 0x00, 0x02, 0x00 };
        var segment3 = new byte[] { 0x00, 0x00 };
        
        var first = new TestSequenceSegment<byte>(segment1, 0, segment1.Length);
        var second = first.Append(segment2);
        var third = second.Append(segment3);
        
        var sequence = new ReadOnlySequence<byte>(first, 0, third, segment3.Length);
        var reader = Int32Type.Instance;
        var destination = new int[2];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(2, itemsRead);
        Assert.Equal(8, bytesConsumed);
        Assert.Equal([1, 2], destination);
    }

    [Fact]
    public void WriteValue_SingleValue_WritesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int32Type.Instance;

        // Act
        typeHandler.WriteValue(writer, 42);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0x2A, 0x00, 0x00, 0x00], result);
    }

    [Fact]
    public void WriteValue_MaxValue_WritesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int32Type.Instance;

        // Act
        typeHandler.WriteValue(writer, int.MaxValue);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0xFF, 0xFF, 0xFF, 0x7F], result);
    }

    [Fact]
    public void WriteValue_MinValue_WritesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int32Type.Instance;

        // Act
        typeHandler.WriteValue(writer, int.MinValue);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0x00, 0x00, 0x00, 0x80], result);
    }

    [Fact]
    public void WriteValues_MultipleValues_WritesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int32Type.Instance;
        var values = new[] { 1, 2, 3 };

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        var expected = new byte[] 
        {
            0x01, 0x00, 0x00, 0x00, // 1
            0x02, 0x00, 0x00, 0x00, // 2
            0x03, 0x00, 0x00, 0x00  // 3
        };
        Assert.Equal(expected, writer.WrittenSpan.ToArray());
    }

    [Theory]
    [InlineData(4)]   // SSE2 boundary
    [InlineData(8)]   // AVX2 boundary
    [InlineData(16)]  // AVX512 boundary
    [InlineData(5)]   // Not aligned to vector size
    [InlineData(17)]  // Just over AVX512 boundary
    [InlineData(32)]  // Multiple AVX512 vectors
    [InlineData(100)] // Large batch
    public void WriteValues_VariousSizes_WritesCorrectBytes(int count)
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int32Type.Instance;
        var values = Enumerable.Range(1, count).ToArray();

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal(count * 4, result.Length);
        
        // Verify each value
        for (var i = 0; i < count; i++)
        {
            var expectedBytes = BitConverter.GetBytes(values[i]);
            var actualBytes = result.AsSpan(i * 4, 4);
            Assert.True(expectedBytes.AsSpan().SequenceEqual(actualBytes));
        }
    }

    [Fact]
    public void WriteValues_EmptyArray_WritesNothing()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int32Type.Instance;
        var values = Array.Empty<int>();

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        Assert.Equal(0, writer.WrittenCount);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    [InlineData(1234567890)]
    [InlineData(-1234567890)]
    public void RoundTrip_SingleValue_PreservesValue(int value)
    {
        // Arrange
        var typeHandler = Int32Type.Instance;
        var writer = new ArrayBufferWriter<byte>();

        // Act - Write
        typeHandler.WriteValue(writer, value);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var result = typeHandler.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(value, result);
        Assert.Equal(4, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void RoundTrip_LargeArray_PreservesAllValues()
    {
        // Arrange
        var typeHandler = Int32Type.Instance;
        var values = new int[1000];
        var random = new Random(42);
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = random.Next(int.MinValue, int.MaxValue);
        }

        // Act - Write
        var writer = new ArrayBufferWriter<byte>();
        typeHandler.WriteValues(writer, values);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new int[values.Length];
        var itemsRead = typeHandler.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(values.Length, itemsRead);
        Assert.Equal(values.Length * 4, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void ReadValue_InsufficientData_ThrowsException()
    {
        // Arrange
        var bytes = new byte[] { 0x2A, 0x00, 0x00 }; // Only 3 bytes instead of 4
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int32Type.Instance;

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(
            () => reader.ReadValue(ref sequence, out _));
        Assert.Contains("Insufficient data", exception.Message);
    }

    [Fact]
    public void Properties_ReturnCorrectValues()
    {
        // Arrange
        var typeHandler = Int32Type.Instance;

        // Assert
        Assert.Equal(ClickHouseDataType.Int32, typeHandler.DataType);
        Assert.Equal("Int32", typeHandler.TypeName);
        Assert.True(typeHandler.IsFixedLength);
        Assert.Equal(4, typeHandler.FixedByteLength);
        Assert.Equal(typeof(int), typeHandler.ClrType);
    }

    [Fact]
    public void SimdCapabilities_DefaultInstance_UsesDefaultCapabilities()
    {
        // Arrange & Act
        var typeHandler = new Int32Type();

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
        var typeHandler = new Int32Type(customCapabilities);

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
        var typeHandler = new Int32Type(capabilities);
        
        var values = Enumerable.Range(1, 100).ToArray();
        var writer = new ArrayBufferWriter<byte>();

        // Act - Write
        typeHandler.WriteValues(writer, values);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new int[values.Length];
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