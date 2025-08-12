namespace ClickHouse.Direct.Transports.Protocol;

internal enum ClientPacketType : byte
{
    Hello = 0,
    Query = 1,
    Data = 2,
    Cancel = 3,
    Ping = 4,
    TablesStatusRequest = 5,
    KeepAlive = 6
}