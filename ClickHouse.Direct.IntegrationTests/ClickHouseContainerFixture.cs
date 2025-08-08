using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;

namespace ClickHouse.Direct.IntegrationTests;

public sealed class ClickHouseContainerFixture : IAsyncLifetime
{
    private readonly ClickHouseContainer _container;
    public const string Username = "default";
    public const string Password = "clickhouse";

    public ClickHouseContainerFixture()
    {
        _container = new ClickHouseBuilder()
            .WithImage("clickhouse/clickhouse-server:25.7-alpine")
            .WithUsername(Username)
            .WithPassword(Password)
            .WithExposedPort(8123)
            .WithExposedPort(9000)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(8123).ForPath("/ping")))
            .Build();
    }

    public string HttpConnectionString => _container.GetConnectionString();
    public string NativeConnectionString => $"Host={_container.Hostname};Port={_container.GetMappedPublicPort(9000)};Database=default;";
    public int NativeTcpPort => _container.GetMappedPublicPort(9000);
    public int HttpPort => _container.GetMappedPublicPort(8123);
    public string Hostname => _container.Hostname;

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();
}