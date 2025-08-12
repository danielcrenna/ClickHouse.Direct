namespace ClickHouse.Direct.Transports.Protocol;

internal static class ProtocolRevision
{
    public const ulong DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH = 54448;
    public const ulong DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS = 54459;
    public const ulong DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME = 54449;
    public const ulong DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT = 54456;
    public const ulong DBMS_MIN_PROTOCOL_VERSION_WITH_TABLE_UUID = 54461;
    
    // Current protocol version that we support
    public const ulong CURRENT_PROTOCOL_VERSION = 54466;
}