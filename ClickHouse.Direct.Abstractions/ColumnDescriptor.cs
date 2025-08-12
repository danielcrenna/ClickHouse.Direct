namespace ClickHouse.Direct.Abstractions;

public sealed class ColumnDescriptor
{
    public required string Name { get; init; }
    public required IClickHouseType Type { get; init; }
    public int ArrayDepth { get; init; } = 0; // 0 for scalar, 1 for Array(T), 2 for Array(Array(T)), etc.
    
    public bool IsArray => ArrayDepth > 0;
    
    public static ColumnDescriptor Create(string name, IClickHouseType type, bool isArray = false)
    {
        return new ColumnDescriptor
        {
            Name = name,
            Type = type,
            ArrayDepth = isArray ? 1 : 0
        };
    }
    
    public static ColumnDescriptor CreateNestedArray(string name, IClickHouseType type, int arrayDepth)
    {
        return new ColumnDescriptor
        {
            Name = name,
            Type = type,
            ArrayDepth = arrayDepth
        };
    }
    
    public string GetClickHouseTypeName()
    {
        if (ArrayDepth == 0)
            return Type.TypeName;
        
        var typeName = Type.TypeName;
        for (var i = 0; i < ArrayDepth; i++)
        {
            typeName = $"Array({typeName})";
        }
        return typeName;
    }
    
    public Type GetClrType()
    {
        var clrType = Type.ClrType;
        for (var i = 0; i < ArrayDepth; i++)
        {
            clrType = clrType.MakeArrayType();
        }
        return clrType;
    }
}