using System.Buffers;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Tests.Types.Simd;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Tests.Types;

public class Float32TypeTests
{
    private readonly Float32Type _type = Float32Type.Instance;

    [Fact]
    public void ReadValue_SingleValue_ReturnsCorrectValue()
    {
        var buffer = new byte[] { 0x00, 0x00, 0x20, 0x41 }; // 10.0f in little-endian
        var sequence = new ReadOnlySequence<byte>(buffer);

        var result = _type.ReadValue(ref sequence, out var bytesConsumed);

        Assert.Equal(10.0f, result);
        Assert.Equal(4, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void ReadValue_NegativeValue_ReturnsCorrectValue()
    {
        var buffer = new byte[] { 0x00, 0x00, 0x20, 0xC1 }; // -10.0f in little-endian
        var sequence = new ReadOnlySequence<byte>(buffer);

        var result = _type.ReadValue(ref sequence, out var bytesConsumed);

        Assert.Equal(-10.0f, result);
        Assert.Equal(4, bytesConsumed);
    }

    [Fact]
    public void ReadValue_MaxValue_ReturnsCorrectValue()
    {
        var buffer = new byte[4];
        BitConverter.TryWriteBytes(buffer, float.MaxValue);
        var sequence = new ReadOnlySequence<byte>(buffer);

        var result = _type.ReadValue(ref sequence, out var bytesConsumed);

        Assert.Equal(float.MaxValue, result);
        Assert.Equal(4, bytesConsumed);
    }

    [Fact]
    public void ReadValue_MinValue_ReturnsCorrectValue()
    {
        var buffer = new byte[4];
        BitConverter.TryWriteBytes(buffer, float.MinValue);
        var sequence = new ReadOnlySequence<byte>(buffer);

        var result = _type.ReadValue(ref sequence, out var bytesConsumed);

        Assert.Equal(float.MinValue, result);
        Assert.Equal(4, bytesConsumed);
    }

    [Fact]
    public void ReadValue_NaN_ReturnsNaN()
    {
        var buffer = new byte[4];
        BitConverter.TryWriteBytes(buffer, float.NaN);
        var sequence = new ReadOnlySequence<byte>(buffer);

        var result = _type.ReadValue(ref sequence, out var bytesConsumed);

        Assert.True(float.IsNaN(result));
        Assert.Equal(4, bytesConsumed);
    }

    [Fact]
    public void ReadValue_PositiveInfinity_ReturnsPositiveInfinity()
    {
        var buffer = new byte[4];
        BitConverter.TryWriteBytes(buffer, float.PositiveInfinity);
        var sequence = new ReadOnlySequence<byte>(buffer);

        var result = _type.ReadValue(ref sequence, out var bytesConsumed);

        Assert.Equal(float.PositiveInfinity, result);
        Assert.Equal(4, bytesConsumed);
    }

    [Fact]
    public void ReadValue_NegativeInfinity_ReturnsNegativeInfinity()
    {
        var buffer = new byte[4];
        BitConverter.TryWriteBytes(buffer, float.NegativeInfinity);
        var sequence = new ReadOnlySequence<byte>(buffer);

        var result = _type.ReadValue(ref sequence, out var bytesConsumed);

        Assert.Equal(float.NegativeInfinity, result);
        Assert.Equal(4, bytesConsumed);
    }

    [Fact]
    public void ReadValues_MultipleValues_ReturnsCorrectArray()
    {
        var values = new[] { 1.0f, -2.5f, 3.14159f, 0.0f, -0.0f };
        var buffer = new byte[values.Length * sizeof(float)];
        Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
        var sequence = new ReadOnlySequence<byte>(buffer);

        var destination = new float[values.Length];
        var valuesRead = _type.ReadValues(ref sequence, destination, out var bytesConsumed);

        Assert.Equal(values.Length, valuesRead);
        Assert.Equal(buffer.Length, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Theory]
    [InlineData(1)]   // Single value
    [InlineData(3)]   // Just below SSE2 boundary
    [InlineData(4)]   // SSE2 boundary
    [InlineData(7)]   // Just below AVX2 boundary
    [InlineData(8)]   // AVX2 boundary
    [InlineData(15)]  // Just below AVX512 boundary
    [InlineData(16)]  // AVX512 boundary
    [InlineData(17)]  // Just above AVX512 boundary
    [InlineData(100)] // Large array
    public void ReadValues_VariousSizes_ReturnsCorrectArray(int count)
    {
        var values = new float[count];
        var random = new Random(42);
        for (var i = 0; i < count; i++)
            values[i] = (float)(random.NextDouble() * 200.0 - 100.0);

        var buffer = new byte[count * sizeof(float)];
        Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
        var sequence = new ReadOnlySequence<byte>(buffer);

        var destination = new float[count];
        var valuesRead = _type.ReadValues(ref sequence, destination, out var bytesConsumed);

        Assert.Equal(count, valuesRead);
        Assert.Equal(buffer.Length, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void ReadValues_FragmentedSequence_ReturnsCorrectArray()
    {
        var values = new[] { 1.1f, 2.2f, 3.3f, 4.4f, 5.5f };
        var fullBuffer = new byte[values.Length * sizeof(float)];
        Buffer.BlockCopy(values, 0, fullBuffer, 0, fullBuffer.Length);

        var segment1 = new BufferSegment(new Memory<byte>(fullBuffer, 0, 8));
        var segment2 = new BufferSegment(new Memory<byte>(fullBuffer, 8, 8));
        var segment3 = new BufferSegment(new Memory<byte>(fullBuffer, 16, 4));
        segment1.Append(segment2);
        segment2.Append(segment3);
        var sequence = new ReadOnlySequence<byte>(segment1, 0, segment3, 4);

        var destination = new float[values.Length];
        var valuesRead = _type.ReadValues(ref sequence, destination, out var bytesConsumed);

        Assert.Equal(values.Length, valuesRead);
        Assert.Equal(fullBuffer.Length, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void WriteValue_SingleValue_ProducesCorrectBytes()
    {
        var writer = new ArrayBufferWriter<byte>();
        var value = 42.5f;

        _type.WriteValue(writer, value);

        var expectedBytes = new byte[4];
        BitConverter.TryWriteBytes(expectedBytes, value);
        Assert.Equal(expectedBytes, writer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteValue_NegativeValue_ProducesCorrectBytes()
    {
        var writer = new ArrayBufferWriter<byte>();
        var value = -123.456f;

        _type.WriteValue(writer, value);

        var expectedBytes = new byte[4];
        BitConverter.TryWriteBytes(expectedBytes, value);
        Assert.Equal(expectedBytes, writer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteValue_MaxValue_ProducesCorrectBytes()
    {
        var writer = new ArrayBufferWriter<byte>();

        _type.WriteValue(writer, float.MaxValue);

        var expectedBytes = new byte[4];
        BitConverter.TryWriteBytes(expectedBytes, float.MaxValue);
        Assert.Equal(expectedBytes, writer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteValue_MinValue_ProducesCorrectBytes()
    {
        var writer = new ArrayBufferWriter<byte>();

        _type.WriteValue(writer, float.MinValue);

        var expectedBytes = new byte[4];
        BitConverter.TryWriteBytes(expectedBytes, float.MinValue);
        Assert.Equal(expectedBytes, writer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteValue_NaN_ProducesCorrectBytes()
    {
        var writer = new ArrayBufferWriter<byte>();

        _type.WriteValue(writer, float.NaN);

        var expectedBytes = new byte[4];
        BitConverter.TryWriteBytes(expectedBytes, float.NaN);
        Assert.Equal(expectedBytes, writer.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteValues_MultipleValues_ProducesCorrectBytes()
    {
        var writer = new ArrayBufferWriter<byte>();
        var values = new[] { 1.0f, -2.5f, 3.14159f, 0.0f, float.PositiveInfinity };

        _type.WriteValues(writer, values);

        var expectedBytes = new byte[values.Length * sizeof(float)];
        Buffer.BlockCopy(values, 0, expectedBytes, 0, expectedBytes.Length);
        Assert.Equal(expectedBytes, writer.WrittenSpan.ToArray());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(100)]
    public void WriteValues_VariousSizes_ProducesCorrectBytes(int count)
    {
        var writer = new ArrayBufferWriter<byte>();
        var values = new float[count];
        var random = new Random(42);
        for (var i = 0; i < count; i++)
            values[i] = (float)(random.NextDouble() * 200.0 - 100.0);

        _type.WriteValues(writer, values);

        var expectedBytes = new byte[count * sizeof(float)];
        Buffer.BlockCopy(values, 0, expectedBytes, 0, expectedBytes.Length);
        Assert.Equal(expectedBytes, writer.WrittenSpan.ToArray());
    }

    [Fact]
    public void RoundTrip_SingleValue_PreservesValue()
    {
        var writer = new ArrayBufferWriter<byte>();
        var originalValue = 987.654f;

        _type.WriteValue(writer, originalValue);
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var readValue = _type.ReadValue(ref sequence, out _);

        Assert.Equal(originalValue, readValue);
    }

    [Fact]
    public void RoundTrip_LargeArray_PreservesAllValues()
    {
        var writer = new ArrayBufferWriter<byte>();
        var originalValues = new float[1000];
        var random = new Random(42);
        for (var i = 0; i < originalValues.Length; i++)
            originalValues[i] = (float)(random.NextDouble() * 1000.0 - 500.0);

        _type.WriteValues(writer, originalValues);
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var readValues = new float[originalValues.Length];
        var valuesRead = _type.ReadValues(ref sequence, readValues, out _);

        Assert.Equal(originalValues.Length, valuesRead);
        Assert.Equal(originalValues, readValues);
    }

    [Fact]
    public void RoundTrip_SpecialValues_PreservesAllValues()
    {
        var writer = new ArrayBufferWriter<byte>();
        var originalValues = new[] 
        { 
            float.MinValue, 
            float.MaxValue, 
            float.Epsilon, 
            float.NaN, 
            float.NegativeInfinity, 
            float.PositiveInfinity,
            0.0f,
            -0.0f
        };

        _type.WriteValues(writer, originalValues);
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var readValues = new float[originalValues.Length];
        var valuesRead = _type.ReadValues(ref sequence, readValues, out _);

        Assert.Equal(originalValues.Length, valuesRead);
        for (var i = 0; i < originalValues.Length; i++)
        {
            if (float.IsNaN(originalValues[i]))
                Assert.True(float.IsNaN(readValues[i]));
            else
                Assert.Equal(originalValues[i], readValues[i]);
        }
    }

    [Fact]
    public void ReadValue_InsufficientData_ThrowsException()
    {
        var buffer = new byte[] { 0x01, 0x02 }; // Only 2 bytes instead of 4
        var sequence = new ReadOnlySequence<byte>(buffer);

        var ex = Assert.Throws<InvalidOperationException>(() => _type.ReadValue(ref sequence, out _));
        Assert.Contains("Insufficient data", ex.Message);
    }

    [Fact]
    public void Properties_ReturnCorrectValues()
    {
        Assert.Equal(0x43, _type.ProtocolCode);
        Assert.Equal("Float32", _type.TypeName);
        Assert.True(_type.IsFixedLength);
        Assert.Equal(4, _type.FixedByteLength);
        Assert.Equal(4, _type.GetFixedByteLength());
        Assert.Equal(typeof(float), _type.ClrType);
    }

    [Fact]
    public void SimdCapabilities_DefaultInstance_UsesDefaultCapabilities()
    {
        var type = Float32Type.Instance;
        Assert.Same(DefaultSimdCapabilities.Instance, type.SimdCapabilities);
    }

    [Fact]
    public void SimdCapabilities_CustomInstance_UsesProvidedCapabilities()
    {
        var customCapabilities = ConstrainedSimdCapabilities.ScalarOnly(DefaultSimdCapabilities.Instance);
        var type = new Float32Type(customCapabilities);
        Assert.Same(customCapabilities, type.SimdCapabilities);
    }

    [Fact]
    public void SimdPaths_DifferentCapabilities_ProduceSameResults()
    {
        var values = new float[100];
        var random = new Random(42);
        for (var i = 0; i < values.Length; i++)
            values[i] = (float)(random.NextDouble() * 1000.0 - 500.0);

        var scalarType = new Float32Type(ConstrainedSimdCapabilities.ScalarOnly(DefaultSimdCapabilities.Instance));
        var simdType = new Float32Type(DefaultSimdCapabilities.Instance);

        var scalarWriter = new ArrayBufferWriter<byte>();
        var simdWriter = new ArrayBufferWriter<byte>();

        scalarType.WriteValues(scalarWriter, values);
        simdType.WriteValues(simdWriter, values);

        Assert.Equal(scalarWriter.WrittenSpan.ToArray(), simdWriter.WrittenSpan.ToArray());

        var scalarSequence = new ReadOnlySequence<byte>(scalarWriter.WrittenMemory);
        var simdSequence = new ReadOnlySequence<byte>(simdWriter.WrittenMemory);

        var scalarResult = new float[values.Length];
        var simdResult = new float[values.Length];

        scalarType.ReadValues(ref scalarSequence, scalarResult, out _);
        simdType.ReadValues(ref simdSequence, simdResult, out _);

        Assert.Equal(scalarResult, simdResult);
        Assert.Equal(values, scalarResult);
    }
}