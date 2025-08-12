namespace ClickHouse.Direct.Abstractions;

public interface IClickHouseTransport : IDisposable
{
    Task<byte[]> ExecuteQueryAsync(string query, CancellationToken cancellationToken = default);
    Task SendDataAsync(string query, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);
    Task<ReadOnlyMemory<byte>> QueryDataAsync(string query, CancellationToken cancellationToken = default);
    Task ExecuteNonQueryAsync(string query, CancellationToken cancellationToken = default);
    Task<bool> PingAsync(CancellationToken cancellationToken = default);
}