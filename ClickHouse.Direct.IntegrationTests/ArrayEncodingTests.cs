using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Transports;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests;

[Collection("ClickHouse")]
public class ArrayEncodingTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : IClassFixture<ClickHouseContainerFixture>, IDisposable
{
    private readonly IClickHouseTransport _transport = new HttpTransport(
        fixture.Hostname,
        fixture.HttpPort,
        ClickHouseContainerFixture.Username,
        ClickHouseContainerFixture.Password
    );

    public void Dispose()
    {
        _transport?.Dispose();
    }
    
    [Fact]
    public async Task CompareArrayEncodings()
    {
        var tableName = TableNameExtensions.GenerateTableName();
        
        // Create simple test case
        await _transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await _transport.ExecuteNonQueryAsync($"""
                                              CREATE TABLE {tableName} (
                                                  arr Array(Int32)
                                              ) ENGINE = Memory
                                              """);
        await _transport.ExecuteNonQueryAsync($"INSERT INTO {tableName} VALUES ([1, 2, 3])");
        
        // Get Native format
        var nativeQuery = $"SELECT arr FROM {tableName} FORMAT Native";
        var nativeData = await CaptureRawBytes(nativeQuery);
        
        // Get RowBinary format
        var rowBinaryQuery = $"SELECT arr FROM {tableName} FORMAT RowBinary";
        var rowBinaryData = await CaptureRawBytes(rowBinaryQuery);
        
        output.WriteLine("=== Comparison for [1, 2, 3] ===");
        output.WriteLine("\nNative Format:");
        DumpHexBytes(nativeData, "Native", detailed: true);
        
        output.WriteLine("\nRowBinary Format:");
        DumpHexBytes(rowBinaryData, "RowBinary", detailed: true);
        
        // Analyze the differences
        AnalyzeArrayEncoding(nativeData, rowBinaryData);
        
        // Cleanup
        await _transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Fact]
    public async Task InvestigateArrayWithEmptyValues()
    {
        var tableName = TableNameExtensions.GenerateTableName();
        
        // Test empty arrays
        await _transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await _transport.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                arr Array(Int32)
            ) ENGINE = Memory");
        await _transport.ExecuteNonQueryAsync($"INSERT INTO {tableName} VALUES ([]), ([1]), ([1, 2]), ([])");
        
        var nativeData = await CaptureRawBytes($"SELECT arr FROM {tableName} FORMAT Native");
        var rowBinaryData = await CaptureRawBytes($"SELECT arr FROM {tableName} FORMAT RowBinary");
        
        output.WriteLine("=== Empty Arrays Test ===");
        output.WriteLine("Native:");
        DumpHexBytes(nativeData, "Native with empty arrays");
        
        output.WriteLine("\nRowBinary:");
        DumpHexBytes(rowBinaryData, "RowBinary with empty arrays");
        
        await _transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    private async Task<byte[]> CaptureRawBytes(string query)
    {
        // Use the HTTP transport directly to get raw bytes
        var httpTransport = _transport as HttpTransport;
        if (httpTransport == null)
            throw new InvalidOperationException("This test requires HttpTransport");
            
        using var httpClient = new HttpClient();
        httpClient.BaseAddress = new Uri($"http://{fixture.Hostname}:{fixture.HttpPort}");
        
        var content = new StringContent(query);
        content.Headers.Clear();
        content.Headers.Add("X-ClickHouse-User", ClickHouseContainerFixture.Username);
        content.Headers.Add("X-ClickHouse-Key", ClickHouseContainerFixture.Password);
        
        var response = await httpClient.PostAsync("/", content);
        response.EnsureSuccessStatusCode();
        
        return await response.Content.ReadAsByteArrayAsync();
    }
    
    private void DumpHexBytes(byte[] data, string label, bool detailed = false)
    {
        output.WriteLine($"{label} ({data.Length} bytes):");
        
        if (detailed)
        {
            // Detailed dump with offset and ASCII
            for (var i = 0; i < data.Length; i += 16)
            {
                var line = $"{i:X4}: ";
                
                // Hex bytes
                for (var j = 0; j < 16; j++)
                {
                    if (i + j < data.Length)
                        line += $"{data[i + j]:X2} ";
                    else
                        line += "   ";
                }
                
                line += " | ";
                
                // ASCII representation
                for (var j = 0; j < 16 && i + j < data.Length; j++)
                {
                    var b = data[i + j];
                    line += (b >= 32 && b < 127) ? (char)b : '.';
                }
                
                output.WriteLine(line);
            }
        }
        else
        {
            // Simple hex dump
            var line = "";
            for (var i = 0; i < Math.Min(data.Length, 256); i++)
            {
                line += $"{data[i]:X2} ";
                if ((i + 1) % 16 == 0)
                {
                    output.WriteLine(line.TrimEnd());
                    line = "";
                }
            }
            if (!string.IsNullOrEmpty(line))
                output.WriteLine(line.TrimEnd());
                
            if (data.Length > 256)
                output.WriteLine($"... ({data.Length - 256} more bytes)");
        }
    }
    
    private void AnalyzeArrayEncoding(byte[] nativeData, byte[] rowBinaryData)
    {
        output.WriteLine("");
        output.WriteLine("=== Analysis ===");
        
        // Expected array: [1, 2, 3] as Array(Int32)
        // Each Int32 = 4 bytes little-endian
        
        output.WriteLine("Expected Int32 values in little-endian:");
        output.WriteLine("  1 = 01 00 00 00");
        output.WriteLine("  2 = 02 00 00 00");
        output.WriteLine("  3 = 03 00 00 00");
        
        // Try to find these patterns in both formats
        var int1 = new byte[] { 0x01, 0x00, 0x00, 0x00 };
        var int2 = new byte[] { 0x02, 0x00, 0x00, 0x00 };
        var int3 = new byte[] { 0x03, 0x00, 0x00, 0x00 };
        
        FindPattern(nativeData, int1, "Int32(1) in Native");
        FindPattern(nativeData, int2, "Int32(2) in Native");
        FindPattern(nativeData, int3, "Int32(3) in Native");
        
        FindPattern(rowBinaryData, int1, "Int32(1) in RowBinary");
        FindPattern(rowBinaryData, int2, "Int32(2) in RowBinary");
        FindPattern(rowBinaryData, int3, "Int32(3) in RowBinary");
        
        // Look for array length encoding (likely VarInt or UInt64)
        output.WriteLine("");
        output.WriteLine("Possible array length encodings:");
        if (rowBinaryData.Length > 0)
        {
            output.WriteLine($"  First byte in RowBinary: 0x{rowBinaryData[0]:X2} (decimal: {rowBinaryData[0]})");
            if (rowBinaryData[0] == 3)
                output.WriteLine("    -> Likely array length as VarInt!");
        }
        
        if (nativeData.Length > 8)
        {
            // Native format typically has metadata
            output.WriteLine("  Native format header (first 16 bytes):");
            var header = "";
            for (var i = 0; i < Math.Min(16, nativeData.Length); i++)
                header += $"{nativeData[i]:X2} ";
            output.WriteLine($"    {header}");
        }
    }
    
    private void FindPattern(byte[] data, byte[] pattern, string description)
    {
        for (var i = 0; i <= data.Length - pattern.Length; i++)
        {
            var match = true;
            for (var j = 0; j < pattern.Length; j++)
            {
                if (data[i + j] != pattern[j])
                {
                    match = false;
                    break;
                }
            }
            if (match)
            {
                output.WriteLine($"  Found {description} at offset 0x{i:X4}");
            }
        }
    }
}