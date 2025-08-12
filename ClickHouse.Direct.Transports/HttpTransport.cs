using System.Net.Http.Headers;
using System.Text;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Transports;

public sealed class HttpTransport : IClickHouseTransport
{
    private readonly HttpClient _httpClient;
    private readonly string _baseUrl;
    private readonly bool _ownsHttpClient;
    
    public HttpTransport(string host, int port = 8123, string username = "default", string password = "", string? database = null)
        : this($"http://{host}:{port}", username, password, database)
    {
    }
    
    public HttpTransport(string baseUrl, string username = "default", string password = "", string? database = null)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _httpClient = new HttpClient();
        _ownsHttpClient = true;
        
        if (!string.IsNullOrEmpty(username))
        {
            var authValue = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authValue);
        }
        
        if (!string.IsNullOrEmpty(database))
        {
            _httpClient.DefaultRequestHeaders.Add("X-ClickHouse-Database", database);
        }
    }
    
    public HttpTransport(HttpClient httpClient, string baseUrl)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _baseUrl = baseUrl.TrimEnd('/');
        _ownsHttpClient = false;
    }
    
    public async Task<byte[]> ExecuteQueryAsync(string query, CancellationToken cancellationToken = default)
    {
        var url = $"{_baseUrl}/?query={Uri.EscapeDataString(query)}";
        var response = await _httpClient.GetAsync(url, cancellationToken);
        
        await EnsureSuccessStatusCodeAsync(response);
        return await response.Content.ReadAsByteArrayAsync(cancellationToken);
    }
    
    public async Task SendDataAsync(string query, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var url = $"{_baseUrl}/?query={Uri.EscapeDataString(query)}";
        using var content = new ByteArrayContent(data.ToArray());
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        
        var response = await _httpClient.PostAsync(url, content, cancellationToken);
        await EnsureSuccessStatusCodeAsync(response);
    }
    
    public async Task<ReadOnlyMemory<byte>> QueryDataAsync(string query, CancellationToken cancellationToken = default)
    {
        var url = $"{_baseUrl}/?query={Uri.EscapeDataString(query)}";
        var response = await _httpClient.GetAsync(url, cancellationToken);
        
        await EnsureSuccessStatusCodeAsync(response);
        var bytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);
        return new ReadOnlyMemory<byte>(bytes);
    }
    
    public async Task ExecuteNonQueryAsync(string query, CancellationToken cancellationToken = default)
    {
        var url = $"{_baseUrl}/?query={Uri.EscapeDataString(query)}";
        var response = await _httpClient.PostAsync(url, null, cancellationToken);
        
        await EnsureSuccessStatusCodeAsync(response);
    }
    
    public async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetAsync($"{_baseUrl}/ping", cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }
    
    public void Dispose()
    {
        if (_ownsHttpClient)
        {
            _httpClient.Dispose();
        }
    }
    
    private static async Task EnsureSuccessStatusCodeAsync(HttpResponseMessage response)
    {
        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync();
            throw new HttpRequestException(
                $"ClickHouse returned error {response.StatusCode}: {errorContent}", 
                null, 
                response.StatusCode);
        }
    }
}