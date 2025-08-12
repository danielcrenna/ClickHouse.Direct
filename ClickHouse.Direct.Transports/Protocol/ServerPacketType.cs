namespace ClickHouse.Direct.Transports.Protocol;

internal enum ServerPacketType : byte
{
    Hello = 0,
    Data = 1,
    Exception = 2,
    Progress = 3,
    Pong = 4,
    EndOfStream = 5,
    ProfileInfo = 6,
    Totals = 7,
    Extremes = 8,
    TablesStatusResponse = 9,
    Log = 10,
    TableColumns = 11,
    PartUUIDs = 12,
    ReadTaskRequest = 13,
    ProfileEvents = 14,
    MergeTreeReadTaskRequest = 15,
    MergeTreeAllRangesAnnouncement = 16,
    MergeTreeReadTaskRequest2 = 17,
    ProfileCounters = 18,
    ParallelReplicasProtocolVersion = 19
}