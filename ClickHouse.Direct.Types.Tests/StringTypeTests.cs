using System.Buffers;
using System.Text;

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
    public void ReadValue_SimpleString_ReturnsCorrectString()
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
        var length = utf8Bytes.Length;
        bytes.Add((byte)length); // Assuming length < 128 for simplicity
        bytes.AddRange(utf8Bytes);
        
        var sequence = new ReadOnlySequence<byte>(bytes.ToArray());
        var reader = StringType.Instance;

        // Act
        var result = reader.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(text, result);
        Assert.Equal(1 + utf8Bytes.Length, bytesConsumed);
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
    public void WriteValue_SimpleString_WritesCorrectBytes()
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

    [Fact]
    public void WriteValue_LargeVarint_WritesCorrectBytes()
    {
        // Arrange: Test with a string that requires multi-byte varint
        var writer = new ArrayBufferWriter<byte>();
        var typeHandler = StringType.Instance;
        var longString = new string('A', 200); // 200 characters

        // Act
        typeHandler.WriteValue(writer, longString);

        // Assert
        var result = writer.WrittenSpan.ToArray();
        
        // 200 in varint is: 0xC8, 0x01 (200 = 128 + 72 = 0x80 | 72, 0x01)
        Assert.Equal(0xC8, result[0]); // 200 & 0x7F | 0x80
        Assert.Equal(0x01, result[1]); // 200 >> 7
        
        // Verify the string content
        var actualString = Encoding.UTF8.GetString(result.AsSpan(2));
        Assert.Equal(longString, actualString);
    }

    [Fact]
    public void RoundTrip_VariousStrings_PreservesData()
    {
        // Arrange
        var strings = new[] 
        { 
            "",
            "Hello",
            "World 🌍",
            "こんにちは",
            new string('X', 1000),
            "Mixed: ASCII + こんにちは + 🚀"
        };

        foreach (var originalString in strings)
        {
            // Write
            var writer = new ArrayBufferWriter<byte>();
            StringType.Instance.WriteValue(writer, originalString);
            
            // Read
            var sequence = new ReadOnlySequence<byte>(writer.WrittenSpan.ToArray());
            var result = StringType.Instance.ReadValue(ref sequence, out _);
            
            // Assert
            Assert.Equal(originalString, result);
        }
    }

    [Fact]
    public void Properties_ReturnCorrectValues()
    {
        // Arrange
        var typeHandler = StringType.Instance;

        // Assert
        Assert.Equal(Abstractions.ClickHouseDataType.String, typeHandler.DataType);
        Assert.Equal("String", typeHandler.TypeName);
        Assert.False(typeHandler.IsFixedLength);
        Assert.Equal(-1, typeHandler.FixedByteLength);
        Assert.Equal(typeof(string), typeHandler.ClrType);
    }
}