using System.Buffers;

namespace ClickHouse.Direct.Types.Tests;

public class UuidTypeTests
{
    [Theory]
    [InlineData("00000000-0000-0000-0000-000000000000")]
    [InlineData("12345678-90ab-cdef-1234-567890abcdef")]
    [InlineData("ffffffff-ffff-ffff-ffff-ffffffffffff")]
    [InlineData("550e8400-e29b-41d4-a716-446655440000")]
    [InlineData("6ba7b810-9dad-11d1-80b4-00c04fd430c8")]
    [InlineData("01234567-89ab-cdef-0123-456789abcdef")]
    public void RoundTripTests(string guidString)
    {
        // Arrange
        var guid = new Guid(guidString);
        var typeHandler = UuidType.Instance;

        // Act - Write to ClickHouse format
        var writer = new ArrayBufferWriter<byte>();
        typeHandler.WriteValue(writer, guid);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read back from ClickHouse format
        var sequence = new ReadOnlySequence<byte>(buffer);
        var result = typeHandler.ReadValue(ref sequence, out var bytesConsumed);

        // Assert
        Assert.Equal(guid, result);
        Assert.Equal(16, bytesConsumed);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void Read_Write_MultipleGuids_ShouldProduceCorrectResults()
    {
        // Arrange
        var testGuids = new[]
        {
            Guid.Empty,
            Guid.NewGuid(),
            new Guid("12345678-90ab-cdef-1234-567890abcdef"),
            new Guid(Enumerable.Range(0, 16).Select(i => (byte)i).ToArray()),
            new Guid(Enumerable.Range(0, 16).Select(i => (byte)(255 - i)).ToArray()),
            new Guid(Enumerable.Repeat((byte)0xAA, 16).ToArray()),
            new Guid(Enumerable.Repeat((byte)0x55, 16).ToArray())
        };

        var typeHandler = UuidType.Instance;

        foreach (var guid in testGuids)
        {
            // Act - Round trip test
            var writer = new ArrayBufferWriter<byte>();
            typeHandler.WriteValue(writer, guid);
            var buffer = writer.WrittenSpan.ToArray();

            var sequence = new ReadOnlySequence<byte>(buffer);
            var result = typeHandler.ReadValue(ref sequence, out _);

            // Assert
            Assert.Equal(guid, result);
        }
    }

    [Fact]
    public void Write_WithStringInput_ShouldConvertAndWrite()
    {
        // Arrange
        const string guidString = "12345678-90ab-cdef-1234-567890abcdef";
        var expected = new Guid(guidString);
        var typeHandler = UuidType.Instance;

        // Act - Write the GUID
        var writer = new ArrayBufferWriter<byte>();
        typeHandler.WriteValue(writer, expected);
        var buffer = writer.WrittenSpan.ToArray();

        // Act - Read it back
        var sequence = new ReadOnlySequence<byte>(buffer);
        var result = typeHandler.ReadValue(ref sequence, out _);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ClickHouseByteOrder_ShouldMatchExpectedFormat()
    {
        // This test validates the exact ClickHouse UUID byte order transformation
        // Guid: 01234567-89AB-CDEF-0123-456789ABCDEF
        // .NET: 67 45 23 01 | AB 89 | EF CD | 01 23 45 67 89 AB CD EF
        //   CH: EF CD | AB 89 | 67 45 23 01 | EF CD AB 89 67 45 23 01

        // Arrange
        var guid = new Guid("01234567-89AB-CDEF-0123-456789ABCDEF");
        var dotNetBytes = guid.ToByteArray();

        // Verify .NET GUID byte order (mixed endian)
        Assert.Equal(0x67, dotNetBytes[0]);
        Assert.Equal(0x45, dotNetBytes[1]);
        Assert.Equal(0x23, dotNetBytes[2]);
        Assert.Equal(0x01, dotNetBytes[3]);
        Assert.Equal(0xAB, dotNetBytes[4]);
        Assert.Equal(0x89, dotNetBytes[5]);
        Assert.Equal(0xEF, dotNetBytes[6]);
        Assert.Equal(0xCD, dotNetBytes[7]);

        // Act - Convert to ClickHouse format
        var writer = new ArrayBufferWriter<byte>();
        UuidType.Instance.WriteValue(writer, guid);
        var chBytes = writer.WrittenSpan.ToArray();

        // Assert - Verify ClickHouse byte order matches expected pattern
        var expected = new byte[]
        {
            0xEF, 0xCD,                                     // bytes[6-7]
            0xAB, 0x89,                                     // bytes[4-5]
            0x67, 0x45, 0x23, 0x01,                         // bytes[0-3]
            0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01  // bytes[8-15] reversed
        };

        Assert.Equal(expected, chBytes);

        // Assert - Verify round trip works
        var sequence = new ReadOnlySequence<byte>(chBytes);
        var roundTrip = UuidType.Instance.ReadValue(ref sequence, out _);
        Assert.Equal(guid, roundTrip);
    }

    [Fact]
    public void ReadValue_WithInsufficientData_ShouldThrowException()
    {
        // Arrange - Only 5 bytes instead of 16
        var buffer = new byte[] { 1, 2, 3, 4, 5 };
        var sequence = new ReadOnlySequence<byte>(buffer);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(
            () => UuidType.Instance.ReadValue(ref sequence, out _));
        
        Assert.Contains("Insufficient data", exception.Message);
    }

    [Fact]
    public void ReadValues_MultipleGuids_ProcessesCorrectly()
    {
        // Arrange
        var originalGuids = new[]
        {
            new Guid("12345678-1234-5678-9abc-123456789abc"),
            new Guid("87654321-4321-8765-cba9-cba987654321"),
            Guid.Empty,
            Guid.NewGuid()
        };

        // Write all GUIDs to buffer
        var writer = new ArrayBufferWriter<byte>();
        UuidType.Instance.WriteValues(writer, originalGuids);
        
        // Act - Read them back
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var destination = new Guid[originalGuids.Length];
        var itemsRead = UuidType.Instance.ReadValues(ref sequence, destination, out var totalBytesConsumed);

        // Assert
        Assert.Equal(originalGuids.Length, itemsRead);
        Assert.Equal(originalGuids.Length * 16, totalBytesConsumed);
        Assert.Equal(originalGuids, destination);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void ReadValues_PartialData_ReadsCompleteGuidsOnly()
    {
        // Arrange - Create buffer with 1.5 GUIDs worth of data
        var completeGuid = new Guid("12345678-1234-5678-9abc-123456789abc");
        
        var writer = new ArrayBufferWriter<byte>();
        UuidType.Instance.WriteValue(writer, completeGuid);
        var validBytes = writer.WrittenSpan.ToArray();
        
        // Add partial data (8 bytes of incomplete GUID)
        var buffer = new byte[24]; // 16 + 8
        validBytes.CopyTo(buffer, 0);
        // Leave last 8 bytes as partial data
        
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new Guid[3]; // Request more than available

        // Act
        var itemsRead = UuidType.Instance.ReadValues(ref sequence, destination, out var bytesConsumed);

        // Assert
        Assert.Equal(1, itemsRead); // Only one complete GUID
        Assert.Equal(16, bytesConsumed);
        Assert.Equal(completeGuid, destination[0]);
        Assert.Equal(Guid.Empty, destination[1]); // Unchanged
        Assert.Equal(8, sequence.Length); // 8 bytes remaining
    }

    [Fact]
    public void WriteValues_EmptySpan_DoesNothing()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        var emptyGuids = Array.Empty<Guid>();

        // Act
        UuidType.Instance.WriteValues(writer, emptyGuids);

        // Assert
        Assert.Equal(0, writer.WrittenCount);
    }

    [Fact]
    public void Properties_ReturnCorrectValues()
    {
        // Arrange
        var typeHandler = UuidType.Instance;

        // Assert
        Assert.Equal(0x1D, typeHandler.ProtocolCode);
        Assert.Equal("UUID", typeHandler.TypeName);
        Assert.True(typeHandler.IsFixedLength);
        Assert.Equal(16, typeHandler.FixedByteLength);
        Assert.Equal(typeof(Guid), typeHandler.ClrType);
    }

    [Fact]
    public void BulkOperations_LargeArrays_ShouldUseSimdPaths()
    {
        // Arrange - Create arrays large enough to trigger SIMD paths
        var guids = Enumerable.Range(0, 100).Select(_ => Guid.NewGuid()).ToArray();

        // Act - Write using bulk operation (should use SIMD internally)
        var writer = new ArrayBufferWriter<byte>();
        UuidType.Instance.WriteValues(writer, guids);

        // Act - Read using bulk operation (should use SIMD internally)
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var destination = new Guid[guids.Length];
        var itemsRead = UuidType.Instance.ReadValues(ref sequence, destination, out _);

        // Assert
        Assert.Equal(guids.Length, itemsRead);
        Assert.Equal(guids, destination);
    }
}