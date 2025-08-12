using System.Net;
using System.Net.Sockets;
using System.Text;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Transports.Protocol;

namespace ClickHouse.Direct.Transports;

public sealed class TcpTransport(
    string host,
    int port = 9000,
    string username = "default",
    string password = "",
    string? database = null)
    : IClickHouseTransport
{
    private const string ClientName = nameof(TcpTransport);

    private readonly string _host = host ?? throw new ArgumentNullException(nameof(host));
    
    private Socket? _socket;
    private NetworkStream? _stream;
    private bool _connected;
    private bool _disposed;
    
    // Server info received during handshake
    private string? _serverName;
    private ulong _serverVersionMajor;
    private ulong _serverVersionMinor;
    private ulong _serverVersionPatch;
    private ulong _serverRevision;
    private string? _serverTimezone;
    private string? _serverDisplayName;

    private async Task EnsureConnectedAsync(CancellationToken cancellationToken = default)
    {
        if (_connected && _socket?.Connected == true)
        {
            return;
        }
        
        await ConnectAsync(cancellationToken);
    }
    
    private async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            // Clean up any existing connection
            Disconnect();
            
            // Create new socket
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            
            // Connect to server
            var ipAddress = await ResolveHostAsync(_host, cancellationToken);
            await _socket.ConnectAsync(new IPEndPoint(ipAddress, port), cancellationToken);
            
            // Setup stream
            _stream = new NetworkStream(_socket, ownsSocket: false);
            
            // Perform handshake
            await PerformHandshakeAsync(cancellationToken);
            
            _connected = true;
        }
        catch
        {
            Disconnect();
            throw;
        }
    }
    
    private async Task PerformHandshakeAsync(CancellationToken cancellationToken = default)
    {
        // Send Hello packet
        await SendHelloPacketAsync(cancellationToken);
        
        // Read server's Hello response
        await ReadHelloResponseAsync(cancellationToken);
    }
    
    private async Task SendHelloPacketAsync(CancellationToken cancellationToken = default)
    {
        using var ms = new MemoryStream();
        await using var writer = new BinaryWriter(ms);
        
        // Packet type
        writer.Write((byte)ClientPacketType.Hello);
        
        // Client name and version
        WriteString(writer, ClientName);
        WriteVarInt(writer, 1); // Version major
        WriteVarInt(writer, 0); // Version minor
        WriteVarInt(writer, ProtocolRevision.CURRENT_PROTOCOL_VERSION);
        
        // Database
        WriteString(writer, database ?? "default");
        
        // User and password
        WriteString(writer, username);
        WriteString(writer, password);
        
        writer.Flush();
        ms.Flush();
        var buffer = ms.ToArray();
        await _stream!.WriteAsync(buffer, cancellationToken);
        await _stream.FlushAsync(cancellationToken);
    }
    
    private async Task ReadHelloResponseAsync(CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask; // Satisfy async requirement
        using var reader = new BinaryReader(_stream!, Encoding.UTF8, leaveOpen: true);
        
        var packetType = reader.ReadByte();
        
        if (packetType == (byte)ServerPacketType.Exception)
        {
            var exception = ReadException(reader);
            throw new InvalidOperationException($"Server returned exception: {exception}");
        }
        
        if (packetType != (byte)ServerPacketType.Hello)
        {
            throw new InvalidOperationException($"Expected Hello packet (0), got {packetType}");
        }
        
        // Read server info
        _serverName = ReadString(reader);
        _serverVersionMajor = ReadVarInt(reader);
        _serverVersionMinor = ReadVarInt(reader);
        _serverRevision = ReadVarInt(reader);
        
        if (_serverRevision >= ProtocolRevision.DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH)
        {
            _serverTimezone = ReadString(reader);
        }
        
        _serverDisplayName = ReadString(reader);
        _serverVersionPatch = ReadVarInt(reader);
    }
    
    private string ReadException(BinaryReader reader)
    {
        var code = reader.ReadUInt32();
        var name = ReadString(reader);
        var message = ReadString(reader);
        var stackTrace = ReadString(reader);
        var hasNested = reader.ReadByte() != 0;
        
        var result = $"{name} (code {code}): {message}";
        
        if (hasNested)
        {
            // Recursively read nested exceptions
            var nested = ReadException(reader);
            result += $" [Nested: {nested}]";
        }
        
        return result;
    }
    
    private async Task SendQueryAsync(string query, CancellationToken cancellationToken = default)
    {
        await EnsureConnectedAsync(cancellationToken);
        
        // Trim the query to remove leading/trailing whitespace that could cause "Empty query" errors
        query = query?.Trim() ?? string.Empty;
        
        using var ms = new MemoryStream();
        await using var writer = new BinaryWriter(ms);
        
        // Packet type
        writer.Write((byte)ClientPacketType.Query);
        
        // Query ID (empty for auto-generation)
        WriteString(writer, "");
        
        // SIMPLIFIED PROTOCOL - Skip client info, go straight to settings
        // Based on testing, the server accepts this simpler format
        
        // Settings
        WriteString(writer, ""); // Empty settings
        
        // Interserver secret
        WriteString(writer, ""); // Empty
        
        // Query processing stage (Complete = 2)
        WriteVarInt(writer, 2);
        
        // Compression (0 = disabled)
        WriteVarInt(writer, 0);
        
        // Query itself
        WriteString(writer, query);
        
        writer.Flush();
        ms.Flush();
        var buffer = ms.ToArray();
        
        await _stream!.WriteAsync(buffer, cancellationToken);
        await _stream.FlushAsync(cancellationToken);
    }
    
    private async Task<List<byte[]>> ReadDataPacketsAsync(CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask; // Satisfy async requirement
        var results = new List<byte[]>();
        using var reader = new BinaryReader(_stream!, Encoding.UTF8, leaveOpen: true);
        
        while (true)
        {
            var packetType = reader.ReadByte();
            
            switch ((ServerPacketType)packetType)
            {
                case ServerPacketType.Data:
                    // Read table name (can be empty)
                    var tableName = ReadString(reader);
                    
                    // For TabSeparated format, after the table name, the data follows directly
                    // The actual format depends on what was requested (TabSeparated, Native, etc.)
                    // For TabSeparated with "SELECT 1", we expect "1\n"
                    
                    // Read some data - we'll read up to 4KB for now
                    var buffer = new byte[4096];
                    var bytesRead = _stream!.Read(buffer, 0, buffer.Length);
                    
                    if (bytesRead > 0)
                    {
                        // Only add the bytes we actually read
                        var actualData = new byte[bytesRead];
                        Array.Copy(buffer, actualData, bytesRead);
                        results.Add(actualData);
                    }
                    break;
                    
                case ServerPacketType.Progress:
                    // Skip progress info
                    ReadVarInt(reader); // rows
                    ReadVarInt(reader); // bytes
                    ReadVarInt(reader); // total rows
                    ReadVarInt(reader); // total bytes
                    ReadVarInt(reader); // written rows
                    ReadVarInt(reader); // written bytes
                    ReadVarInt(reader); // elapsed ns
                    break;
                    
                case ServerPacketType.ProfileInfo:
                    // Skip profile info
                    ReadVarInt(reader); // rows
                    ReadVarInt(reader); // blocks
                    ReadVarInt(reader); // bytes
                    reader.ReadByte(); // applied limit
                    ReadVarInt(reader); // rows before limit
                    reader.ReadByte(); // calculated rows before limit
                    break;
                    
                case ServerPacketType.Exception:
                    var exception = ReadException(reader);
                    throw new InvalidOperationException($"Query failed: {exception}");
                    
                case ServerPacketType.EndOfStream:
                    return results;
                    
                default:
                    // Skip unknown packet types
                    break;
            }
        }
    }
    
    private static void WriteString(BinaryWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value ?? "");
        WriteVarInt(writer, (ulong)bytes.Length);
        if (bytes.Length > 0)
        {
            writer.Write(bytes);
        }
    }
    
    private static string ReadString(BinaryReader reader)
    {
        var length = (int)ReadVarInt(reader);
        if (length == 0)
        {
            return string.Empty;
        }
        
        var bytes = reader.ReadBytes(length);
        return Encoding.UTF8.GetString(bytes);
    }
    
    private static void WriteVarInt(BinaryWriter writer, ulong value)
    {
        while (value >= 0x80)
        {
            writer.Write((byte)(value | 0x80));
            value >>= 7;
        }
        writer.Write((byte)value);
    }
    
    private static ulong ReadVarInt(BinaryReader reader)
    {
        ulong value = 0;
        var shift = 0;
        
        while (true)
        {
            var b = reader.ReadByte();
            value |= (ulong)(b & 0x7F) << shift;
            
            if ((b & 0x80) == 0)
            {
                return value;
            }
            
            shift += 7;
            if (shift >= 64)
            {
                throw new InvalidOperationException("VarInt too large");
            }
        }
    }
    
    private async Task<IPAddress> ResolveHostAsync(string host, CancellationToken cancellationToken)
    {
        if (IPAddress.TryParse(host, out var address))
        {
            return address;
        }
        
        var addresses = await Dns.GetHostAddressesAsync(host, cancellationToken);
        return addresses.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork)
            ?? addresses.FirstOrDefault()
            ?? throw new InvalidOperationException($"Could not resolve host {host}");
    }
    
    private void Disconnect()
    {
        _connected = false;
        
        _stream?.Dispose();
        _stream = null;
        
        if (_socket != null)
        {
            if (_socket.Connected)
            {
                _socket.Shutdown(SocketShutdown.Both);
            }
            _socket.Close();
            _socket.Dispose();
            _socket = null;
        }
    }
    
    public async Task<byte[]> ExecuteQueryAsync(string query, CancellationToken cancellationToken = default)
    {
        await SendQueryAsync(query, cancellationToken);
        
        // Send empty data block to signal no input data
        // Based on ClickHouse protocol, empty block needs block info structure
        using var ms = new MemoryStream();
        await using var writer = new BinaryWriter(ms);
        writer.Write((byte)ClientPacketType.Data);
        WriteString(writer, ""); // Table name (empty)
        
        // Block info for empty block
        writer.Write((byte)0); // is_overflows
        writer.Write((byte)0); // bucket_num
        WriteVarInt(writer, 0); // num_columns
        WriteVarInt(writer, 0); // num_rows
        
        writer.Flush();
        await _stream!.WriteAsync(ms.ToArray(), cancellationToken);
        await _stream.FlushAsync(cancellationToken);
        
        var dataPackets = await ReadDataPacketsAsync(cancellationToken);
        
        // Combine all data packets
        var totalLength = dataPackets.Sum(p => p.Length);
        var result = new byte[totalLength];
        var offset = 0;
        
        foreach (var packet in dataPackets)
        {
            Buffer.BlockCopy(packet, 0, result, offset, packet.Length);
            offset += packet.Length;
        }
        
        return result;
    }
    
    public async Task SendDataAsync(string query, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        await SendQueryAsync(query, cancellationToken);
        
        // Send Data packet
        using var ms = new MemoryStream();
        await using var writer = new BinaryWriter(ms);
        writer.Write((byte)ClientPacketType.Data);
        writer.Write(data.ToArray());
        writer.Flush();
        await _stream!.WriteAsync(ms.ToArray(), cancellationToken);
        
        // Send empty data block to signal end with proper block info
        ms.SetLength(0);
        writer.Write((byte)ClientPacketType.Data);
        WriteString(writer, ""); // Table name (empty)
        
        // Block info for empty block
        writer.Write((byte)0); // is_overflows
        writer.Write((byte)0); // bucket_num
        WriteVarInt(writer, 0); // num_columns
        WriteVarInt(writer, 0); // num_rows
        
        writer.Flush();
        await _stream.WriteAsync(ms.ToArray(), cancellationToken);
        await _stream.FlushAsync(cancellationToken);
        
        // Wait for EndOfStream
        using var reader = new BinaryReader(_stream, Encoding.UTF8, leaveOpen: true);
        while (true)
        {
            var packetType = reader.ReadByte();
            
            if (packetType == (byte)ServerPacketType.EndOfStream)
            {
                break;
            }
            
            if (packetType == (byte)ServerPacketType.Exception)
            {
                var exception = ReadException(reader);
                throw new InvalidOperationException($"Insert failed: {exception}");
            }
            
            if (packetType == (byte)ServerPacketType.Progress)
            {
                // Skip progress info
                ReadVarInt(reader); // rows
                ReadVarInt(reader); // bytes
                ReadVarInt(reader); // total rows
                ReadVarInt(reader); // total bytes
                ReadVarInt(reader); // written rows
                ReadVarInt(reader); // written bytes
                ReadVarInt(reader); // elapsed ns
            }
            else if (packetType == (byte)ServerPacketType.ProfileInfo)
            {
                // Skip profile info
                ReadVarInt(reader); // rows
                ReadVarInt(reader); // blocks
                ReadVarInt(reader); // bytes
                reader.ReadByte(); // applied limit
                ReadVarInt(reader); // rows before limit
                reader.ReadByte(); // calculated rows before limit
            }
        }
    }
    
    public async Task<ReadOnlyMemory<byte>> QueryDataAsync(string query, CancellationToken cancellationToken = default)
    {
        var result = await ExecuteQueryAsync(query, cancellationToken);
        return new ReadOnlyMemory<byte>(result);
    }
    
    public async Task ExecuteNonQueryAsync(string query, CancellationToken cancellationToken = default)
    {
        await SendQueryAsync(query, cancellationToken);
        
        // Send empty data block with proper block info structure
        using var ms = new MemoryStream();
        await using var writer = new BinaryWriter(ms);
        writer.Write((byte)ClientPacketType.Data);
        WriteString(writer, ""); // Table name (empty)
        
        // Block info for empty block
        writer.Write((byte)0); // is_overflows
        writer.Write((byte)0); // bucket_num
        WriteVarInt(writer, 0); // num_columns
        WriteVarInt(writer, 0); // num_rows
        
        writer.Flush();
        await _stream!.WriteAsync(ms.ToArray(), cancellationToken);
        await _stream.FlushAsync(cancellationToken);
        
        // Wait for EndOfStream
        using var reader = new BinaryReader(_stream, Encoding.UTF8, leaveOpen: true);
        while (true)
        {
            var packetType = reader.ReadByte();
            
            if (packetType == (byte)ServerPacketType.EndOfStream)
            {
                break;
            }
            
            if (packetType == (byte)ServerPacketType.Exception)
            {
                var exception = ReadException(reader);
                throw new InvalidOperationException($"Query failed: {exception}");
            }
            
            if (packetType == (byte)ServerPacketType.Progress)
            {
                // Skip progress info
                ReadVarInt(reader); // rows
                ReadVarInt(reader); // bytes
                ReadVarInt(reader); // total rows
                ReadVarInt(reader); // total bytes
                ReadVarInt(reader); // written rows
                ReadVarInt(reader); // written bytes
                ReadVarInt(reader); // elapsed ns
            }
            else if (packetType == (byte)ServerPacketType.ProfileInfo)
            {
                // Skip profile info
                ReadVarInt(reader); // rows
                ReadVarInt(reader); // blocks
                ReadVarInt(reader); // bytes
                reader.ReadByte(); // applied limit
                ReadVarInt(reader); // rows before limit
                reader.ReadByte(); // calculated rows before limit
            }
            
            // Continue for any other packet types
        }
    }
    
    public async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureConnectedAsync(cancellationToken);
            // Connection successful means ping is successful
            // The TCP protocol doesn't really have a standalone ping outside of queries
            return true;
        }
        catch
        {
            return false;
        }
    }
    
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        
        Disconnect();
        _disposed = true;
    }
}