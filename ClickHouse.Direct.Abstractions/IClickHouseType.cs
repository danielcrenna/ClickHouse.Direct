namespace ClickHouse.Direct.Abstractions;

public interface IClickHouseType
{
    byte ProtocolCode { get; }
    Type ClrType { get; }
    string TypeName { get; }
    bool IsFixedLength { get; }
    int FixedByteLength { get; }
}