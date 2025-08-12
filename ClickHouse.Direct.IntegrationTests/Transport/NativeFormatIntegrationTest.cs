using ClickHouse.Direct.Transports;
using ClickHouse.Direct.Transports.Protocol;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Transport;

[Collection("ClickHouse")]
public class NativeFormatIntegrationTest(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : IDisposable
{
    private TcpTransport? _transport;
    private readonly NativeFormatReader _reader = new();

    [Fact]
    public async Task TestNativeFormatInt32()
    {
        _transport = new TcpTransport(
            fixture.Hostname, 
            fixture.NativeTcpPort,
            ClickHouseContainerFixture.Username,
            ClickHouseContainerFixture.Password);
        
        const string query = "SELECT 42 AS answer FORMAT Native";
        output.WriteLine($"Executing query: {query}");
        
        var rawData = await _transport.ExecuteQueryAsync(query);
        output.WriteLine($"Received {rawData.Length} bytes");
        
        // Parse Native format
        var block = _reader.ParseNativeData(rawData);
        
        output.WriteLine("Parsed result:");
        output.WriteLine($"  Columns: {block.ColumnCount}");
        output.WriteLine($"  Rows: {block.RowCount}");
        
        Assert.Equal(1, block.ColumnCount);
        Assert.Equal(1, block.RowCount);
        
        var column = block.Columns[0];
        output.WriteLine($"  Column name: {column.Name}");
        output.WriteLine($"  Column type: {column.Type.TypeName}");
        output.WriteLine($"  CLR type: {column.Type.ClrType}");
        
        Assert.Equal("answer", column.Name);
        Assert.Equal(typeof(int), column.Type.ClrType);
        
        // Check the value
        var value = block[0, 0];
        output.WriteLine($"  Value: {value}");
        Assert.Equal(42, value);
    }
    
    [Fact]
    public async Task TestNativeFormatString()
    {
        _transport = new TcpTransport(
            fixture.Hostname, 
            fixture.NativeTcpPort,
            ClickHouseContainerFixture.Username,
            ClickHouseContainerFixture.Password);
        
        const string query = "SELECT 'hello' AS greeting FORMAT Native";
        output.WriteLine($"Executing query: {query}");
        
        var rawData = await _transport.ExecuteQueryAsync(query);
        var block = _reader.ParseNativeData(rawData);
        
        Assert.Equal(1, block.ColumnCount);
        Assert.Equal(1, block.RowCount);
        Assert.Equal("greeting", block.Columns[0].Name);
        Assert.Equal(typeof(string), block.Columns[0].Type.ClrType);
        Assert.Equal("hello", block[0, 0]);
    }
    
    [Fact]
    public async Task TestNativeFormatMultipleRows()
    {
        _transport = new TcpTransport(
            fixture.Hostname, 
            fixture.NativeTcpPort,
            ClickHouseContainerFixture.Username,
            ClickHouseContainerFixture.Password);
        
        const string query = """
                             SELECT number AS id, toString(number) AS name
                             FROM system.numbers
                             LIMIT 5
                             FORMAT Native
                             """;
        
        output.WriteLine($"Executing query: {query}");
        
        var rawData = await _transport.ExecuteQueryAsync(query);
        var block = _reader.ParseNativeData(rawData);
        
        output.WriteLine($"Result: {block.ColumnCount} columns, {block.RowCount} rows");
        
        Assert.Equal(2, block.ColumnCount);
        Assert.Equal(5, block.RowCount);
        
        // Check column names and types
        Assert.Equal("id", block.Columns[0].Name);
        Assert.Equal("name", block.Columns[1].Name);
        Assert.Equal(typeof(ulong), block.Columns[0].Type.ClrType);
        Assert.Equal(typeof(string), block.Columns[1].Type.ClrType);
        
        // Check values
        for (var i = 0; i < 5; i++)
        {
            Assert.Equal((ulong)i, block[i, 0]);
            Assert.Equal(i.ToString(), block[i, 1]);
            output.WriteLine($"  Row {i}: id={block[i, 0]}, name='{block[i, 1]}'");
        }
    }
    
    [Fact]
    public async Task TestNativeFormatMixedTypes()
    {
        _transport = new TcpTransport(
            fixture.Hostname, 
            fixture.NativeTcpPort,
            ClickHouseContainerFixture.Username,
            ClickHouseContainerFixture.Password);
        
        var query = @"
            SELECT 
                toInt32(1) AS int_val,
                toFloat32(3.14) AS float_val,
                'test' AS str_val,
                toUInt8(255) AS byte_val
            FORMAT Native";
        
        output.WriteLine($"Executing query: {query}");
        
        var rawData = await _transport.ExecuteQueryAsync(query);
        var block = _reader.ParseNativeData(rawData);
        
        Assert.Equal(4, block.ColumnCount);
        Assert.Equal(1, block.RowCount);
        
        Assert.Equal(1, block[0, 0]);
        Assert.Equal(3.14f, block[0, 1]);
        Assert.Equal("test", block[0, 2]);
        Assert.Equal((byte)255, block[0, 3]);
        
        output.WriteLine("Mixed types parsed successfully:");
        output.WriteLine($"  int_val: {block[0, 0]} ({block.Columns[0].Type.TypeName})");
        output.WriteLine($"  float_val: {block[0, 1]} ({block.Columns[1].Type.TypeName})");
        output.WriteLine($"  str_val: {block[0, 2]} ({block.Columns[2].Type.TypeName})");
        output.WriteLine($"  byte_val: {block[0, 3]} ({block.Columns[3].Type.TypeName})");
    }
    
    public void Dispose()
    {
        _transport?.Dispose();
    }
}