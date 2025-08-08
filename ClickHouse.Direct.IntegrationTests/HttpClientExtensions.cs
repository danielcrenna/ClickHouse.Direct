using System.Net.Http.Headers;
using System.Text;

namespace ClickHouse.Direct.IntegrationTests;

internal static class HttpClientExtensions
{
    public static async Task<HttpResponseMessage> ExecuteQuery(
        this HttpClient httpClient,
        string hostname,
        int httpPort,
        string query)
    {
        var content = new StringContent(query, Encoding.UTF8, "text/plain");
        return await httpClient.PostAsync($"http://{hostname}:{httpPort}/", content);
    }
    
    public static async Task<HttpResponseMessage> ExecuteBinaryQuery(
        this HttpClient httpClient,
        string hostname,
        int httpPort,
        string query,
        byte[] binaryData)
    {
        // ClickHouse expects: [SQL Query with FORMAT RowBinary] [Binary Data]
        
        using var ms = new MemoryStream();
        
        var queryBytes = Encoding.UTF8.GetBytes(query);
        await ms.WriteAsync(queryBytes, 0, queryBytes.Length);
        await ms.WriteAsync("\n"u8.ToArray(), 0, 1);
        await ms.WriteAsync(binaryData, 0, binaryData.Length);
        
        ms.Position = 0;
        var content = new StreamContent(ms);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        
        return await httpClient.PostAsync($"http://{hostname}:{httpPort}/", content);
    }
}