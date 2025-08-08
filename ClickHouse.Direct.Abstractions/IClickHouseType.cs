namespace ClickHouse.Direct.Abstractions;

public interface IClickHouseType
{
    ClickHouseDataType DataType { get; }
    Type ClrType { get; }
    string TypeName { get; }
    bool IsFixedLength { get; }
    int FixedByteLength { get; }
}