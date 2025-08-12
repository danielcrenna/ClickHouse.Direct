using System.Collections;

namespace ClickHouse.Direct.Abstractions;

public sealed class Block : IEnumerable<IReadOnlyList<object?>>
{
    private readonly IReadOnlyList<ColumnDescriptor> _columns;
    private readonly List<IList> _columnData;
    private readonly int _rowCount;

    public Block(IReadOnlyList<ColumnDescriptor> columns, int capacity = 0)
    {
        _columns = columns;
        _columnData = new List<IList>(columns.Count);
        _rowCount = 0;
        
        foreach (var column in columns)
        {
            var listType = typeof(List<>).MakeGenericType(column.GetClrType());
            var list = (IList)Activator.CreateInstance(listType, capacity)!;
            _columnData.Add(list);
        }
    }

    private Block(IReadOnlyList<ColumnDescriptor> columns, List<IList> columnData, int rowCount)
    {
        _columns = columns;
        _columnData = columnData;
        _rowCount = rowCount;
    }

    public IReadOnlyList<ColumnDescriptor> Columns => _columns;
    public int RowCount => _rowCount;
    public int ColumnCount => _columns.Count;

    public object? this[int row, int column]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(row, _rowCount);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(column, _columnData.Count);
            return _columnData[column][row];
        }
    }

    public object? this[int row, string columnName]
    {
        get
        {
            var columnIndex = GetColumnIndex(columnName);
            if (columnIndex < 0)
                throw new ArgumentException($"Column '{columnName}' not found", nameof(columnName));
            return this[row, columnIndex];
        }
    }

    public IList GetColumnData(int columnIndex)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(columnIndex, _columnData.Count);
        return _columnData[columnIndex];
    }

    public IList GetColumnData(string columnName)
    {
        var columnIndex = GetColumnIndex(columnName);
        if (columnIndex < 0)
            throw new ArgumentException($"Column '{columnName}' not found", nameof(columnName));
        return GetColumnData(columnIndex);
    }

    public int GetColumnIndex(string columnName)
    {
        for (var i = 0; i < _columns.Count; i++)
        {
            if (_columns[i].Name == columnName)
                return i;
        }
        return -1;
    }

    public static Block CreateFromColumnData(IReadOnlyList<ColumnDescriptor> columns, List<IList> columnData, int rowCount)
    {
        if (columns.Count != columnData.Count)
            throw new ArgumentException("Column count mismatch");
        
        return new Block(columns, columnData, rowCount);
    }


    public IEnumerator<IReadOnlyList<object?>> GetEnumerator()
    {
        for (var row = 0; row < _rowCount; row++)
        {
            var rowData = new object?[_columnData.Count];
            for (var col = 0; col < _columnData.Count; col++)
            {
                rowData[col] = _columnData[col][row];
            }
            yield return rowData;
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}