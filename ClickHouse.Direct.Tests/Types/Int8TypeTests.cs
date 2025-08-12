using System.Buffers;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Tests.Types;

public class Int8TypeTests
{
    [Fact]
    public void ReadValue_SingleValue_ReturnsCorrectValue()
    {
        // Arrange: 42 as single byte
        var bytes = new byte[] { 0x2A };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int8Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal((sbyte)42, result);
        Assert.Equal(1, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void ReadValue_NegativeValue_ReturnsCorrectValue()
    {
        // Arrange: -1 as single byte (0xFF)
        var bytes = new byte[] { 0xFF };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int8Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal((sbyte)-1, result);
        Assert.Equal(1, bytesConsumed);
    }

    [Fact]
    public void ReadValue_MaxValue_ReturnsCorrectValue()
    {
        // Arrange: sbyte.MaxValue (127) as single byte
        var bytes = new byte[] { 0x7F };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int8Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(sbyte.MaxValue, result);
        Assert.Equal(1, bytesConsumed);
    }

    [Fact]
    public void ReadValue_MinValue_ReturnsCorrectValue()
    {
        // Arrange: sbyte.MinValue (-128) as single byte
        var bytes = new byte[] { 0x80 };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int8Type.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(sbyte.MinValue, result);
        Assert.Equal(1, bytesConsumed);
    }

    [Fact]
    public void ReadValues_MultipleValues_ReturnsCorrectArray()
    {
        // Arrange: [1, 2, 3] as bytes
        var bytes = new byte[] { 0x01, 0x02, 0x03 };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int8Type.Instance;
        var destination = new sbyte[3];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(3, itemsRead);
        Assert.Equal(3, bytesConsumed);
        Assert.Equal([1, 2, 3], destination);
        Assert.Equal(0, sequence.Length);
    }

    [Theory]
    [InlineData(16)]  // SSE2 boundary
    [InlineData(32)]  // AVX2 boundary
    [InlineData(64)]  // AVX512 boundary
    [InlineData(17)]  // Not aligned to vector size
    [InlineData(65)]  // Just over AVX512 boundary
    [InlineData(128)] // Multiple AVX512 vectors
    [InlineData(100)] // Large batch
    public void ReadValues_VariousSizes_ReturnsCorrectArray(int count)
    {
        // Arrange
        var values = Enumerable.Range(1, count).Select(x => (sbyte)(x % 128)).ToArray();
        var bytes = values.Select(x => (byte)x).ToArray();
        
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int8Type.Instance;
        var destination = new sbyte[count];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(count, itemsRead);
        Assert.Equal(count, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void ReadValues_FragmentedSequence_ReturnsCorrectArray()
    {
        // Arrange: Create fragmented sequence
        var segment1 = new byte[] { 0x01 };
        var segment2 = new byte[] { 0x02, 0x03 };
        
        var first = new TestSequenceSegment<byte>(segment1, 0, segment1.Length);
        var second = first.Append(segment2);
        
        var sequence = new ReadOnlySequence<byte>(first, 0, second, segment2.Length);
        var reader = Int8Type.Instance;
        var destination = new sbyte[3];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(3, itemsRead);
        Assert.Equal(3, bytesConsumed);
        Assert.Equal([1, 2, 3], destination);
    }

    [Fact]
    public void WriteValue_SingleValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int8Type.Instance;

        // Act
        typeHandler.WriteValue(writer, 42);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0x2A], result);
    }

    [Fact]
    public void WriteValue_NegativeValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int8Type.Instance;

        // Act
        typeHandler.WriteValue(writer, -1);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0xFF], result);
    }

    [Fact]
    public void WriteValue_MaxValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int8Type.Instance;

        // Act
        typeHandler.WriteValue(writer, sbyte.MaxValue);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0x7F], result);
    }

    [Fact]
    public void WriteValue_MinValue_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int8Type.Instance;

        // Act
        typeHandler.WriteValue(writer, sbyte.MinValue);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0x80], result);
    }

    [Fact]
    public void WriteValues_MultipleValues_ProducesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int8Type.Instance;
        var values = new sbyte[] { 1, 2, 3 };

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        var expected = new byte[] { 0x01, 0x02, 0x03 };
        Assert.Equal(expected, writer.WrittenSpan.ToArray());
    }

    [Theory]
    [InlineData(16)]  // SSE2 boundary
    [InlineData(32)]  // AVX2 boundary
    [InlineData(64)]  // AVX512 boundary
    [InlineData(17)]  // Not aligned to vector size
    [InlineData(65)]  // Just over AVX512 boundary
    [InlineData(128)] // Multiple AVX512 vectors
    [InlineData(100)] // Large batch
    public void WriteValues_VariousSizes_ProducesCorrectBytes(int count)
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = Int8Type.Instance;
        var values = Enumerable.Range(1, count).Select(x => (sbyte)(x % 128)).ToArray();

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal(count, result.Length);
        
        // Verify each value
        for (var i = 0; i < count; i++)
        {
            Assert.Equal((byte)values[i], result[i]);
        }
    }

    [Theory]
    [InlineData((sbyte)0)]
    [InlineData((sbyte)-1)]
    [InlineData((sbyte)1)]
    [InlineData(sbyte.MaxValue)]
    [InlineData(sbyte.MinValue)]
    [InlineData((sbyte)100)]
    [InlineData((sbyte)-100)]
    public void RoundTrip_SingleValue_PreservesValue(sbyte value)
    {
        // Arrange
        var typeHandler = Int8Type.Instance;
        var writer = new ArrayBufferWriter<byte>();

        // Act - Write
        typeHandler.WriteValue(writer, value);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var result = typeHandler.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(value, result);
        Assert.Equal(1, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void RoundTrip_LargeArray_PreservesAllValues()
    {
        // Arrange
        var typeHandler = Int8Type.Instance;
        var values = new sbyte[1000];
        var random = new Random(42);
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = (sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue + 1);
        }

        // Act - Write
        var writer = new ArrayBufferWriter<byte>();
        typeHandler.WriteValues(writer, values);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new sbyte[values.Length];
        var itemsRead = typeHandler.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(values.Length, itemsRead);
        Assert.Equal(values.Length, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void ReadValue_InsufficientData_ThrowsException()
    {
        // Arrange
        var bytes = Array.Empty<byte>();
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = Int8Type.Instance;

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(
            () => reader.ReadValue(ref sequence, out _));
        Assert.Contains("Insufficient data", exception.Message);
    }

    [Fact]
    public void Properties_ReturnCorrectValues()
    {
        // Arrange
        var typeHandler = Int8Type.Instance;

        // Assert
        Assert.Equal(0x07, typeHandler.ProtocolCode);
        Assert.Equal("Int8", typeHandler.TypeName);
        Assert.True(typeHandler.IsFixedLength);
        Assert.Equal(1, typeHandler.FixedByteLength);
        Assert.Equal(typeof(sbyte), typeHandler.ClrType);
    }

    [Fact]
    public void SimdCapabilities_DefaultInstance_UsesDefaultCapabilities()
    {
        // Arrange & Act
        var typeHandler = new Int8Type();

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
        var typeHandler = new Int8Type(customCapabilities);

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
        var typeHandler = new Int8Type(capabilities);
        
        var values = Enumerable.Range(1, 100).Select(x => (sbyte)(x % 128)).ToArray();
        var writer = new ArrayBufferWriter<byte>();

        // Act - Write
        typeHandler.WriteValues(writer, values);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new sbyte[values.Length];
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