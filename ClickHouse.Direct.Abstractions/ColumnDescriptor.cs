namespace ClickHouse.Direct.Abstractions;

public sealed class ColumnDescriptor
{
    public required string Name { get; init; }
    public required IClickHouseType Type { get; init; }
    
    public static ColumnDescriptor Create(string name, IClickHouseType type)
    {
        return new ColumnDescriptor
        {
            Name = name,
            Type = type
        };
    }
}