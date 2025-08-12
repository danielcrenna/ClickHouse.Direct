using System.Buffers;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Tests.Types;

public class Int16TypeTests
{
    [Fact]
    public void ReadValue_SingleValue_ReturnsCorrectValue()
    {
        // Arrange: 42 in little-endian bytes
        var bytes = new byte[] { 0x2A, 0x00 };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int16Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal((short)42, result);
        Assert.Equal(2, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void ReadValue_NegativeValue_ReturnsCorrectValue()
    {
        // Arrange: -1 in little-endian bytes (0xFFFF)
        var bytes = new byte[] { 0xFF, 0xFF };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int16Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal((short)-1, result);
        Assert.Equal(2, bytesConsumed);
    }

    [Fact]
    public void ReadValue_MaxValue_ReturnsCorrectValue()
    {
        // Arrange: short.MaxValue (32767) in little-endian bytes
        var bytes = new byte[] { 0xFF, 0x7F };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int16Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(short.MaxValue, result);
        Assert.Equal(2, bytesConsumed);
    }

    [Fact]
    public void ReadValue_MinValue_ReturnsCorrectValue()
    {
        // Arrange: short.MinValue (-32768) in little-endian bytes
        var bytes = new byte[] { 0x00, 0x80 };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int16Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(short.MinValue, result);
        Assert.Equal(2, bytesConsumed);
    }

    [Fact]
    public void ReadValues_MultipleValues_ReturnsCorrectArray()
    {
        // Arrange: [1, 2, 3] in little-endian bytes
        var bytes = new byte[] 
        {
            0x01, 0x00, // 1
            0x02, 0x00, // 2
            0x03, 0x00  // 3
        };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int16Type.Instance;
        var destination = new short[3];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(3, itemsRead);
        Assert.Equal(6, bytesConsumed);
        Assert.Equal([1, 2, 3], destination);
        Assert.Equal(0, sequence.Length);
    }

    [Theory]
    [InlineData(8)]   // SSE2 boundary
    [InlineData(16)]  // AVX2 boundary
    [InlineData(32)]  // AVX512 boundary
    [InlineData(9)]   // Not aligned to vector size
    [InlineData(33)]  // Just over AVX512 boundary
    [InlineData(64)]  // Multiple AVX512 vectors
    [InlineData(100)] // Large batch
    public void ReadValues_VariousSizes_ReturnsCorrectArray(int count)
    {
        // Arrange
        var values = Enumerable.Range(1, count).Select(x => (short)x).ToArray();
        var bytes = new byte[count * 2];
        for (var i = 0; i < count; i++)
        {
            BitConverter.GetBytes(values[i]).CopyTo(bytes, i * 2);
        }
        
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int16Type.Instance;
        var destination = new short[count];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(count, itemsRead);
        Assert.Equal(count * 2, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void ReadValues_FragmentedSequence_ReturnsCorrectArray()
    {
        // Arrange: Create fragmented sequence
        var segment1 = new byte[] { 0x01, 0x00 };
        var segment2 = new byte[] { 0x02 };
        var segment3 = new byte[] { 0x00 };
        
        var first = new TestSequenceSegment<byte>(segment1, 0, segment1.Length);
        var second = first.Append(segment2);
        var third = second.Append(segment3);
        
        var sequence = new ReadOnlySequence<byte>(first, 0, third, segment3.Length);
        var reader = Int16Type.Instance;
        var destination = new short[2];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(2, itemsRead);
        Assert.Equal(4, bytesConsumed);
        Assert.Equal([1, 2], destination);
    }

    [Fact]
    public void WriteValue_SingleValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int16Type.Instance;

        // Act
        typeHandler.WriteValue(writer, 42);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0x2A, 0x00], result);
    }

    [Fact]
    public void WriteValue_NegativeValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int16Type.Instance;

        // Act
        typeHandler.WriteValue(writer, -1);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0xFF, 0xFF], result);
    }

    [Fact]
    public void WriteValue_MaxValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int16Type.Instance;

        // Act
        typeHandler.WriteValue(writer, short.MaxValue);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0xFF, 0x7F], result);
    }

    [Fact]
    public void WriteValue_MinValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int16Type.Instance;

        // Act
        typeHandler.WriteValue(writer, short.MinValue);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0x00, 0x80], result);
    }

    [Fact]
    public void WriteValues_MultipleValues_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int16Type.Instance;
        var values = new short[] { 1, 2, 3 };

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        var expected = new byte[] 
        {
            0x01, 0x00, // 1
            0x02, 0x00, // 2
            0x03, 0x00  // 3
        };
        Assert.Equal(expected, writer.WrittenSpan.ToArray());
    }

    [Theory]
    [InlineData(8)]   // SSE2 boundary
    [InlineData(16)]  // AVX2 boundary
    [InlineData(32)]  // AVX512 boundary
    [InlineData(9)]   // Not aligned to vector size
    [InlineData(33)]  // Just over AVX512 boundary
    [InlineData(64)]  // Multiple AVX512 vectors
    [InlineData(100)] // Large batch
    public void WriteValues_VariousSizes_ProducesCorrectBytes(int count)
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int16Type.Instance;
        var values = Enumerable.Range(1, count).Select(x => (short)x).ToArray();

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal(count * 2, result.Length);
        
        // Verify each value
        for (var i = 0; i < count; i++)
        {
            var expectedBytes = BitConverter.GetBytes(values[i]);
            var actualBytes = result.AsSpan(i * 2, 2);
            Assert.True(expectedBytes.AsSpan().SequenceEqual(actualBytes));
        }
    }

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)-1)]
    [InlineData((short)1)]
    [InlineData(short.MaxValue)]
    [InlineData(short.MinValue)]
    [InlineData((short)12345)]
    [InlineData((short)-12345)]
    public void RoundTrip_SingleValue_PreservesValue(short value)
    {
        // Arrange
        var typeHandler = Int16Type.Instance;
        var writer = new ArrayBufferWriter<byte>();

        // Act - Write
        typeHandler.WriteValue(writer, value);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var result = typeHandler.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(value, result);
        Assert.Equal(2, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void RoundTrip_LargeArray_PreservesAllValues()
    {
        // Arrange
        var typeHandler = Int16Type.Instance;
        var values = new short[1000];
        var random = new Random(42);
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = (short)random.Next(short.MinValue, short.MaxValue + 1);
        }

        // Act - Write
        var writer = new ArrayBufferWriter<byte>();
        typeHandler.WriteValues(writer, values);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new short[values.Length];
        var itemsRead = typeHandler.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(values.Length, itemsRead);
        Assert.Equal(values.Length * 2, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void ReadValue_InsufficientData_ThrowsException()
    {
        // Arrange
        var bytes = new byte[] { 0x2A }; // Only 1 byte instead of 2
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int16Type.Instance;

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(
            () => reader.ReadValue(ref sequence, out _));
        Assert.Contains("Insufficient data", exception.Message);
    }

    [Fact]
    public void Properties_ReturnCorrectValues()
    {
        // Arrange
        var typeHandler = Int16Type.Instance;

        // Assert
        Assert.Equal(0x08, typeHandler.ProtocolCode);
        Assert.Equal("Int16", typeHandler.TypeName);
        Assert.True(typeHandler.IsFixedLength);
        Assert.Equal(2, typeHandler.FixedByteLength);
        Assert.Equal(typeof(short), typeHandler.ClrType);
    }

    [Fact]
    public void SimdCapabilities_DefaultInstance_UsesDefaultCapabilities()
    {
        // Arrange & Act
        var typeHandler = new Int16Type();

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
        var typeHandler = new Int16Type(customCapabilities);

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
        var typeHandler = new Int16Type(capabilities);
        
        var values = Enumerable.Range(1, 100).Select(x => (short)x).ToArray();
        var writer = new ArrayBufferWriter<byte>();

        // Act - Write
        typeHandler.WriteValues(writer, values);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new short[values.Length];
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