using System.Buffers;
using Xunit.Abstractions;

namespace ClickHouse.Direct.Types.Tests;

public class UuidRoundTripTests(ITestOutputHelper output)
{
    [Fact]
    public void RoundTrip_KnownGuid_PreservesValue()
    {
        // Test case from the GitHub issue
        var originalGuid = new Guid("dca0e161-9503-41a1-9de2-18528bfffe88");
        
        // Write to ClickHouse format
        var writer = new ArrayBufferWriter<byte>();
        UuidType.Instance.WriteValue(writer, originalGuid);
        var clickHouseBytes = writer.WrittenSpan.ToArray();
        
        // Read back from ClickHouse format
        var sequence = new ReadOnlySequence<byte>(clickHouseBytes);
        var result = UuidType.Instance.ReadValue(ref sequence, out var bytesConsumed);
        
        output.WriteLine($"Original GUID: {originalGuid}");
        output.WriteLine($"ClickHouse bytes: {Convert.ToHexString(clickHouseBytes)}");
        output.WriteLine($"Round-trip result: {result}");
        output.WriteLine($"Bytes consumed: {bytesConsumed}");
        
        // The key test: round-trip should preserve the original GUID
        Assert.Equal(originalGuid, result);
        Assert.Equal(16, bytesConsumed);
    }

    [Fact]
    public void RoundTrip_MultipleGuids_PreservesAllValues()
    {
        var originalGuids = new[]
        {
            new Guid("dca0e161-9503-41a1-9de2-18528bfffe88"),
            Guid.NewGuid(),
            Guid.Empty,
            new Guid("12345678-1234-5678-9abc-123456789abc")
        };
        
        // Write all to ClickHouse format
        var writer = new ArrayBufferWriter<byte>();
        UuidType.Instance.WriteValues(writer, originalGuids);
        
        // Read all back
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var results = new Guid[originalGuids.Length];
        var itemsRead = UuidType.Instance.ReadValues(ref sequence, results, out var totalBytesConsumed);
        
        Assert.Equal(originalGuids.Length, itemsRead);
        Assert.Equal(originalGuids.Length * 16, totalBytesConsumed);
        Assert.Equal(originalGuids, results);
    }
}