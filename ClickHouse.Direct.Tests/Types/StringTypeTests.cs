using System.Buffers;
using System.Text;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types.Tests;

public class StringTypeTests
{
    [Fact]
    public void ReadValue_EmptyString_ReturnsEmpty()
    {
        // Arrange: varint 0 for empty string
        var bytes = new byte[] { 0x00 };
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = StringType.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(string.Empty, result);
        Assert.Equal(1, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void ReadValue_SimpleAsciiString_ReturnsCorrectString()
    {
        // Arrange: "Hello" as length(5) + UTF-8 bytes
        var helloBytes = "Hello"u8.ToArray();
        var bytes = new List<byte> { 0x05 }; // varint 5
        bytes.AddRange(helloBytes);
        var sequence = new ReadOnlySequence<byte>(bytes.ToArray());
        var reader = StringType.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal("Hello", result);
        Assert.Equal(6, bytesConsumed); // 1 byte varint + 5 bytes UTF-8
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void ReadValue_UnicodeString_ReturnsCorrectString()
    {
        // Arrange: "こんにちは" (Japanese "Hello")
        var text = "こんにちは";
        var utf8Bytes = Encoding.UTF8.GetBytes(text);
        var bytes = new List<byte>();
        
        // Write varint length
        WriteVarint(bytes, (ulong)utf8Bytes.Length);
        bytes.AddRange(utf8Bytes);
        
        var sequence = new ReadOnlySequence<byte>(bytes.ToArray());
        var reader = StringType.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(text, result);
        Assert.Equal(bytes.Count, bytesConsumed);
    }

    [Theory]
    [InlineData("")]
    [InlineData("a")]
    [InlineData("Hello")]
    [InlineData("Hello, World!")]
    [InlineData("Mixed ASCII + こんにちは + 🚀")]
    [InlineData("Ĩñţérñåţîöñåŀ")]
    public void ReadValue_VariousStrings_ReturnsCorrectString(string input)
    {
        // Arrange
        var utf8Bytes = Encoding.UTF8.GetBytes(input);
        var bytes = new List<byte>();
        WriteVarint(bytes, (ulong)utf8Bytes.Length);
        bytes.AddRange(utf8Bytes);
        
        var sequence = new ReadOnlySequence<byte>(bytes.ToArray());
        var reader = StringType.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(input, result);
        Assert.Equal(bytes.Count, bytesConsumed);
    }

    [Fact]
    public void ReadValues_MultipleStrings_ReturnsCorrectArray()
    {
        // Arrange: ["", "Hello", "World"]
        var strings = new[] { "", "Hello", "World" };
        var bytes = new List<byte>();
        
        foreach (var str in strings)
        {
            var utf8Bytes = Encoding.UTF8.GetBytes(str);
            WriteVarint(bytes, (ulong)utf8Bytes.Length);
            bytes.AddRange(utf8Bytes);
        }
        
        var sequence = new ReadOnlySequence<byte>(bytes.ToArray());
        var reader = StringType.Instance;
        var destination = new string[strings.Length];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(strings.Length, itemsRead);
        Assert.Equal(bytes.Count, bytesConsumed);
        Assert.Equal(strings, destination);
        Assert.Equal(0, sequence.Length);
    }

    [Theory]
    [InlineData(4)]   // Small batch
    [InlineData(8)]   // Potential SIMD optimization boundary
    [InlineData(16)]  // SIMD-friendly batch size
    [InlineData(32)]  // Larger batch
    [InlineData(100)] // Large batch for bulk optimization testing
    public void ReadValues_VariousBatchSizes_ReturnsCorrectArray(int count)
    {
        // Arrange - Create mix of ASCII and Unicode strings
        var strings = new string[count];
        var random = new Random(42);
        
        for (var i = 0; i < count; i++)
        {
            strings[i] = random.Next(4) switch
            {
                0 => "", // Empty
                1 => GenerateAsciiString(random, random.Next(1, 20)), // ASCII
                2 => GenerateUnicodeString(random, random.Next(1, 10)), // Unicode  
                _ => GenerateLongAsciiString(random, random.Next(50, 200)) // Long ASCII for SIMD
            };
        }
        
        // Encode all strings
        var bytes = new List<byte>();
        foreach (var str in strings)
        {
            var utf8Bytes = Encoding.UTF8.GetBytes(str);
            WriteVarint(bytes, (ulong)utf8Bytes.Length);
            bytes.AddRange(utf8Bytes);
        }
        
        var sequence = new ReadOnlySequence<byte>(bytes.ToArray());
        var reader = StringType.Instance;
        var destination = new string[count];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(count, itemsRead);
        Assert.Equal(bytes.Count, bytesConsumed);
        Assert.Equal(strings, destination);
    }

    [Fact]
    public void ReadValues_FragmentedSequence_ReturnsCorrectArray()
    {
        // Arrange: Create fragmented sequence with "Hello" and "World"
        var hello = "Hello"u8.ToArray();
        var world = "World"u8.ToArray();
        
        var segment1 = new byte[] { 0x05 }; // varint 5 for "Hello"
        var segment2 = hello[..3]; // "Hel"
        var segment3 = hello[3..]; // "lo"
        var segment4 = new byte[] { 0x05 }; // varint 5 for "World"
        var segment5 = world;
        
        var first = new TestSequenceSegment<byte>(segment1, 0, segment1.Length);
        var second = first.Append(segment2);
        var third = second.Append(segment3);
        var fourth = third.Append(segment4);
        var fifth = fourth.Append(segment5);
        
        var sequence = new ReadOnlySequence<byte>(first, 0, fifth, segment5.Length);
        var reader = StringType.Instance;
        var destination = new string[2];

        // Act
        var itemsRead = reader.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(2, itemsRead);
        Assert.Equal(12, bytesConsumed); // 1+5+1+5 bytes
        Assert.Equal(new[] { "Hello", "World" }, destination);
    }

    [Fact]
    public void WriteValue_EmptyString_WritesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = StringType.Instance;

        // Act
        typeHandler.WriteValue(writer, string.Empty);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        Assert.Equal([0x00], result);
    }

    [Fact]
    public void WriteValue_SimpleAsciiString_WritesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = StringType.Instance;

        // Act
        typeHandler.WriteValue(writer, "Hello");

        // Assert
        var result = writer.WrittenSpan.ToArray();
        var expected = new List<byte> { 0x05 }; // varint 5
        expected.AddRange("Hello"u8.ToArray());
        Assert.Equal(expected.ToArray(), result);
    }

    [Theory]
    [InlineData(127)]  // Single byte varint boundary
    [InlineData(128)]  // Two byte varint boundary
    [InlineData(200)]  // Multi-byte varint
    [InlineData(16383)] // Three byte varint boundary
    [InlineData(16384)] // Three byte varint
    public void WriteValue_LargeVarint_WritesCorrectBytes(int stringLength)
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = StringType.Instance;
        var longString = new string('A', stringLength);

        // Act
        typeHandler.WriteValue(writer, longString);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        
        // Verify varint encoding manually
        var expectedVarint = new List<byte>();
        WriteVarint(expectedVarint, (ulong)stringLength);
        
        // Check varint matches
        for (var i = 0; i < expectedVarint.Count; i++)
        {
            Assert.Equal(expectedVarint[i], result[i]);
        }
        
        // Verify the string content
        var actualString = Encoding.UTF8.GetString(result.AsSpan(expectedVarint.Count));
        Assert.Equal(longString, actualString);
    }

    [Fact]
    public void WriteValues_MultipleStrings_WritesCorrectBytes()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = StringType.Instance;
        var values = new[] { "Hello", "World", "" };

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        
        // Build expected bytes
        var expected = new List<byte>();
        foreach (var str in values)
        {
            var utf8Bytes = Encoding.UTF8.GetBytes(str);
            WriteVarint(expected, (ulong)utf8Bytes.Length);
            expected.AddRange(utf8Bytes);
        }
        
        Assert.Equal(expected.ToArray(), result);
    }

    [Theory]
    [InlineData(4)]   // Small batch
    [InlineData(8)]   // Batch optimization boundary
    [InlineData(16)]  // SIMD-friendly size
    [InlineData(32)]  // Larger batch
    [InlineData(100)] // Large batch for bulk testing
    public void WriteValues_VariousBatchSizes_WritesCorrectBytes(int count)
    {
        // Arrange - Create mix of string types to test different optimization paths
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = StringType.Instance;
        var values = new string[count];
        var random = new Random(42);
        
        for (var i = 0; i < count; i++)
        {
            values[i] = random.Next(4) switch
            {
                0 => "", // Empty
                1 => GenerateAsciiString(random, random.Next(1, 30)), // Short ASCII
                2 => GenerateUnicodeString(random, random.Next(1, 10)), // Unicode
                _ => GenerateLongAsciiString(random, random.Next(50, 150)) // Long ASCII
            };
        }

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert - Verify by round-trip
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var destination = new string[count];
        var itemsRead = typeHandler.ReadValues(ref sequence, destination, out _);
        
        Assert.Equal(count, itemsRead);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void WriteValues_EmptyArray_WritesNothing()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = StringType.Instance;
        var values = Array.Empty<string>();

        // Act
        typeHandler.WriteValues(writer, values);

        // Assert
        Assert.Equal(0, writer.WrittenCount);
    }

    [Theory]
    [InlineData("")]
    [InlineData("Hello")]
    [InlineData("World 🌍")]
    [InlineData("こんにちは")]
    [InlineData("Mixed: ASCII + こんにちは + 🚀")]
    [InlineData("Ĩñţérñåţîöñåŀ")]
    public void RoundTrip_SingleValue_PreservesValue(string value)
    {
        // Arrange
        var typeHandler = StringType.Instance;
        var writer = new ArrayBufferWriter<byte>();

        // Act - Write
        typeHandler.WriteValue(writer, value);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var result = typeHandler.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(value, result);
        Assert.Equal(buffer.Length, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void RoundTrip_LargeArray_PreservesAllValues()
    {
        // Arrange - Mix of string types to test all optimization paths
        var typeHandler = StringType.Instance;
        var values = new string[1000];
        var random = new Random(42);
        
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = random.Next(5) switch
            {
                0 => "",
                1 => GenerateAsciiString(random, random.Next(1, 50)),
                2 => GenerateUnicodeString(random, random.Next(1, 20)),
                3 => GenerateLongAsciiString(random, random.Next(100, 500)),
                _ => GenerateMixedString(random, random.Next(10, 100))
            };
        }

        // Act - Write
        var writer = new ArrayBufferWriter<byte>();
        typeHandler.WriteValues(writer, values);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new string[values.Length];
        var itemsRead = typeHandler.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(values.Length, itemsRead);
        Assert.Equal(buffer.Length, bytesConsumed);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void Properties_ReturnCorrectValues()
    {
        // Arrange
        var typeHandler = StringType.Instance;

        // Assert
        Assert.Equal(0x15, typeHandler.ProtocolCode);
        Assert.Equal("String", typeHandler.TypeName);
        Assert.False(typeHandler.IsFixedLength);
        Assert.Equal(-1, typeHandler.FixedByteLength);
        Assert.Equal(typeof(string), typeHandler.ClrType);
    }

    [Fact]
    public void SimdCapabilities_DefaultInstance_UsesDefaultCapabilities()
    {
        // Arrange & Act
        var typeHandler = new StringType();

        // Assert
        Assert.NotNull(typeHandler.SimdCapabilities);
        Assert.IsType<DefaultSimdCapabilities>(typeHandler.SimdCapabilities);
    }

    [Fact]
    public void SimdCapabilities_CustomInstance_UsesProvidedCapabilities()
    {
        // Arrange
        var customCapabilities = ConstrainedSimdCapabilities.ScalarOnly(DefaultSimdCapabilities.Instance);

        // Act
        var typeHandler = new StringType(customCapabilities);

        // Assert
        Assert.Same(customCapabilities, typeHandler.SimdCapabilities);
    }

    [Theory]
    [InlineData(true, true)]   // AVX2 + SSSE3 (full SIMD)
    [InlineData(true, false)]  // AVX2 only
    [InlineData(false, true)]  // SSSE3 only 
    [InlineData(false, false)] // No SIMD (scalar only)
    public void SimdPaths_DifferentCapabilities_ProduceSameResults(bool avx2, bool ssse3)
    {
        // Arrange
        var capabilities = new ConstrainedSimdCapabilities(
            DefaultSimdCapabilities.Instance,
            allowAvx2: avx2,
            allowSsse3: ssse3,
            allowSse2: avx2 || ssse3 // Need SSE2 as baseline
        );
        var typeHandler = new StringType(capabilities);
        
        // Test with mix of string types that exercise different SIMD paths
        var values = new[]
        {
            "", // Empty
            "a", // Single char
            "Hello", // Short ASCII
            GenerateLongAsciiString(new Random(42), 100), // Long ASCII for SIMD
            "こんにちは", // Unicode
            GenerateMixedString(new Random(42), 50) // Mixed content
        };

        var writer = new ArrayBufferWriter<byte>();

        // Act - Write
        typeHandler.WriteValues(writer, values);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new string[values.Length];
        var itemsRead = typeHandler.ReadValues(ref sequence, destination, out _);

        // Assert
        Assert.Equal(values.Length, itemsRead);
        Assert.Equal(values, destination);
    }

    [Fact]
    public void AsciiOptimization_LargeAsciiStrings_UsesSimdPath()
    {
        // Arrange - Large ASCII strings should trigger SIMD optimizations
        var typeHandler = StringType.Instance;
        var largeAsciiStrings = Enumerable.Range(0, 10)
            .Select(i => GenerateLongAsciiString(new Random(i), 200))
            .ToArray();

        // Act - This should use SIMD paths for ASCII detection and conversion
        var writer = new ArrayBufferWriter<byte>();
        typeHandler.WriteValues(writer, largeAsciiStrings);
        
        // Act - Read back
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var destination = new string[largeAsciiStrings.Length];
        var itemsRead = typeHandler.ReadValues(ref sequence, destination, out _);

        // Assert
        Assert.Equal(largeAsciiStrings.Length, itemsRead);
        Assert.Equal(largeAsciiStrings, destination);
    }

    [Fact]
    public void BatchEncoding_AsciiStrings_UsesFastPath()
    {
        // Arrange - Small ASCII strings should trigger batch encoding
        var typeHandler = StringType.Instance;
        var asciiStrings = Enumerable.Range(0, 20)
            .Select(i => GenerateAsciiString(new Random(i), 10))
            .ToArray();

        // Act
        var writer = new ArrayBufferWriter<byte>();
        typeHandler.WriteValues(writer, asciiStrings);

        // Assert - Verify by round-trip
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var destination = new string[asciiStrings.Length];
        var itemsRead = typeHandler.ReadValues(ref sequence, destination, out _);

        Assert.Equal(asciiStrings.Length, itemsRead);
        Assert.Equal(asciiStrings, destination);
    }

    // Helper methods
    private static void WriteVarint(List<byte> bytes, ulong value)
    {
        while (value >= 0x80)
        {
            bytes.Add((byte)(value | 0x80));
            value >>= 7;
        }
        bytes.Add((byte)value);
    }

    private static string GenerateAsciiString(Random random, int length)
    {
        var chars = new char[length];
        for (var i = 0; i < length; i++)
        {
            chars[i] = (char)('a' + random.Next(26));
        }
        return new string(chars);
    }

    private static string GenerateLongAsciiString(Random random, int length)
    {
        var chars = new char[length];
        for (var i = 0; i < length; i++)
        {
            chars[i] = (char)('A' + random.Next(26));
        }
        return new string(chars);
    }

    private static string GenerateUnicodeString(Random random, int length)
    {
        var chars = new char[length];
        for (var i = 0; i < length; i++)
        {
            chars[i] = (char)('あ' + random.Next(100)); // Japanese characters
        }
        return new string(chars);
    }

    private static string GenerateMixedString(Random random, int length)
    {
        var chars = new char[length];
        for (var i = 0; i < length; i++)
        {
            chars[i] = random.Next(3) switch
            {
                0 => (char)('a' + random.Next(26)), // ASCII
                1 => (char)('А' + random.Next(32)), // Cyrillic
                _ => (char)('あ' + random.Next(50))  // Japanese
            };
        }
        return new string(chars);
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