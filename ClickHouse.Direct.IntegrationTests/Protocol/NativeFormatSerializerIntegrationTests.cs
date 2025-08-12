using ClickHouse.Direct.Formats;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Protocol;

public class NativeFormatSerializerIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : FormatSerializerIntegrationTestsBase(fixture, output)
{
    protected override IFormatSerializer CreateSerializer() => new NativeFormatSerializer();
    protected override string FormatName => "Native";
}