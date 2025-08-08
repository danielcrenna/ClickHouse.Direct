using System.Collections;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Protocol;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Tests.Protocol;

public class BlockTests
{
    [Fact]
    public void Constructor_WithColumns_CreatesEmptyBlock()
    {
        // Arrange
        var columns = new[]
        {
            ColumnDescriptor.Create("id", ClickHouseTypes.Int32),
            ColumnDescriptor.Create("name", ClickHouseTypes.String)
        };

        // Act
        var block = new Block(columns);

        // Assert
        Assert.Equal(2, block.ColumnCount);
        Assert.Equal(0, block.RowCount);
        Assert.Equal("id", block.Columns[0].Name);
        Assert.Equal("name", block.Columns[1].Name);
        Assert.Same(ClickHouseTypes.Int32, block.Columns[0].Type);
        Assert.Same(ClickHouseTypes.String, block.Columns[1].Type);
    }

    [Fact]
    public void CreateFromColumnData_WithValidData_CreatesBlock()
    {
        // Arrange
        var columns = new[]
        {
            ColumnDescriptor.Create("id", ClickHouseTypes.Int32),
            ColumnDescriptor.Create("name", ClickHouseTypes.String)
        };

        var columnData = new List<IList>
        {
            new List<int> { 1, 2, 3 },
            new List<string> { "Alice", "Bob", "Charlie" }
        };

        // Act
        var block = Block.CreateFromColumnData(columns, columnData, 3);

        // Assert
        Assert.Equal(2, block.ColumnCount);
        Assert.Equal(3, block.RowCount);
        
        // Check data access
        Assert.Equal(1, block[0, 0]);
        Assert.Equal("Alice", block[0, 1]);
        Assert.Equal(2, block[1, 0]);
        Assert.Equal("Bob", block[1, 1]);
        Assert.Equal(3, block[2, 0]);
        Assert.Equal("Charlie", block[2, 1]);
    }

    [Fact]
    public void GetColumnData_ByIndex_ReturnsCorrectData()
    {
        // Arrange
        var columns = new[]
        {
            ColumnDescriptor.Create("id", ClickHouseTypes.Int32)
        };

        var columnData = new List<IList>
        {
            new List<int> { 10, 20, 30 }
        };

        var block = Block.CreateFromColumnData(columns, columnData, 3);

        // Act
        var data = block.GetColumnData(0);

        // Assert
        Assert.Equal(3, data.Count);
        Assert.Equal(10, data[0]);
        Assert.Equal(20, data[1]);
        Assert.Equal(30, data[2]);
    }

    [Fact]
    public void GetColumnData_ByName_ReturnsCorrectData()
    {
        // Arrange
        var columns = new[]
        {
            ColumnDescriptor.Create("name", ClickHouseTypes.String)
        };

        var columnData = new List<IList>
        {
            new List<string> { "Test1", "Test2" }
        };

        var block = Block.CreateFromColumnData(columns, columnData, 2);

        // Act
        var data = block.GetColumnData("name");

        // Assert
        Assert.Equal(2, data.Count);
        Assert.Equal("Test1", data[0]);
        Assert.Equal("Test2", data[1]);
    }

    [Fact]
    public void GetColumnIndex_ExistingColumn_ReturnsCorrectIndex()
    {
        // Arrange
        var columns = new[]
        {
            ColumnDescriptor.Create("first", ClickHouseTypes.Int32),
            ColumnDescriptor.Create("second", ClickHouseTypes.String),
            ColumnDescriptor.Create("third", ClickHouseTypes.UUID)
        };

        var block = new Block(columns);

        // Act & Assert
        Assert.Equal(0, block.GetColumnIndex("first"));
        Assert.Equal(1, block.GetColumnIndex("second"));
        Assert.Equal(2, block.GetColumnIndex("third"));
        Assert.Equal(-1, block.GetColumnIndex("nonexistent"));
    }

    [Fact]
    public void Enumeration_WithData_IteratesRows()
    {
        // Arrange
        var columns = new[]
        {
            ColumnDescriptor.Create("id", ClickHouseTypes.Int32),
            ColumnDescriptor.Create("name", ClickHouseTypes.String)
        };

        var columnData = new List<IList>
        {
            new List<int> { 1, 2 },
            new List<string> { "Alice", "Bob" }
        };

        var block = Block.CreateFromColumnData(columns, columnData, 2);

        // Act
        var rows = block.ToList();

        // Assert
        Assert.Equal(2, rows.Count);
        
        var firstRow = rows[0];
        Assert.Equal(1, firstRow[0]);
        Assert.Equal("Alice", firstRow[1]);
        
        var secondRow = rows[1];
        Assert.Equal(2, secondRow[0]);
        Assert.Equal("Bob", secondRow[1]);
    }
}