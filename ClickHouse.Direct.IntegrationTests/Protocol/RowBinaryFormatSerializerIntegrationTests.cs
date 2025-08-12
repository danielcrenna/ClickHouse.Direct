using ClickHouse.Direct.Formats;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Protocol;

public class RowBinaryFormatSerializerIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : FormatSerializerIntegrationTestsBase(fixture, output)
{
    protected override IFormatSerializer CreateSerializer() => new RowBinaryFormatSerializer();
    protected override string FormatName => "RowBinary";
}