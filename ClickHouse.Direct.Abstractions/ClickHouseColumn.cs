using System.Collections;

namespace ClickHouse.Direct.Abstractions;

public readonly struct ClickHouseColumn<T>(string name, ClickHouseDataType dataType, ReadOnlyMemory<T> data)
    : IEnumerable<T>
{
    public string Name { get; } = name ?? throw new ArgumentNullException(nameof(name));
    public ClickHouseDataType DataType { get; } = dataType;
    public ReadOnlyMemory<T> Data { get; } = data;
    public int RowCount => Data.Length;

    public T this[int index] => Data.Span[index];

    public IEnumerator<T> GetEnumerator()
    {
        var array = Data.ToArray();
        foreach (var item in array)
            yield return item;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}