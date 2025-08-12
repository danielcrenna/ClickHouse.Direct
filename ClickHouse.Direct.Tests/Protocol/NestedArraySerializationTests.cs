using System.Buffers;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Formats;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Tests.Protocol;

public class NestedArraySerializationTests
{
    [Fact]
    public void WriteReadNestedArray_Native_RoundTrip()
    {
        // Arrange
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new UInt32Type()),
            ColumnDescriptor.CreateNestedArray("matrix", new Int32Type(), arrayDepth: 2)
        };
        
        var idData = new List<uint> { 1, 2, 3 };
        var matrixData = new List<int[][]>
        {
            new int[][]
            {
                [1, 2, 3],
                [4, 5, 6]
            },
            new int[][]
            {
                [10]
            },
            new int[][]
            {
            }
        };
        
        var originalBlock = Block.CreateFromColumnData(columns, [idData, matrixData], 3);
        
        // Act - Serialize
        var serializer = new NativeFormatSerializer();
        var buffer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(originalBlock, buffer);
        
        // Act - Deserialize
        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var deserializedBlock = serializer.ReadBlock(3, columns, ref sequence, out var bytesConsumed);
        
        // Assert
        Assert.Equal(buffer.WrittenCount, bytesConsumed);
        Assert.Equal(3, deserializedBlock.RowCount);
        Assert.Equal(2, deserializedBlock.ColumnCount);
        
        for (var i = 0; i < 3; i++)
        {
            var id = (uint)deserializedBlock[i, 0]!;
            var matrix = (int[][])deserializedBlock[i, 1]!;
            
            Assert.Equal(idData[i], id);
            Assert.Equal(matrixData[i].Length, matrix.Length);
            
            for (var j = 0; j < matrix.Length; j++)
            {
                Assert.Equal(matrixData[i][j], matrix[j]);
            }
        }
    }
    
    [Fact]
    public void WriteReadTripleNestedArray_Native_RoundTrip()
    {
        // Arrange
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.CreateNestedArray("cube", new Int32Type(), arrayDepth: 3)
        };
        
        var cubeData = new List<int[][][]>
        {
            new[]
            {
                new int[][]
                {
                    [1, 2],
                    [3, 4]
                },
                new int[][]
                {
                    [5, 6],
                    [7, 8]
                }
            }
        };
        
        var originalBlock = Block.CreateFromColumnData(columns, [cubeData], 1);
        
        // Act - Serialize
        var serializer = new NativeFormatSerializer();
        var buffer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(originalBlock, buffer);
        
        // Act - Deserialize
        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var deserializedBlock = serializer.ReadBlock(1, columns, ref sequence, out var bytesConsumed);
        
        // Assert
        Assert.Equal(buffer.WrittenCount, bytesConsumed);
        Assert.Equal(1, deserializedBlock.RowCount);
        Assert.Equal(1, deserializedBlock.ColumnCount);
        
        var cube = (int[][][])deserializedBlock[0, 0]!;
        Assert.Equal(2, cube.Length);
        Assert.Equal(2, cube[0].Length);
        Assert.Equal(2, cube[0][0].Length);
        
        Assert.Equal([1, 2], cube[0][0]);
        Assert.Equal([3, 4], cube[0][1]);
        Assert.Equal([5, 6], cube[1][0]);
        Assert.Equal([7, 8], cube[1][1]);
    }
    
    [Fact]
    public void ColumnDescriptor_GetClickHouseTypeName_ReturnsCorrectNestedType()
    {
        // Single array
        var arrayColumn = ColumnDescriptor.Create("arr", new Int32Type(), isArray: true);
        Assert.Equal("Array(Int32)", arrayColumn.GetClickHouseTypeName());
        
        // Double nested array
        var nestedColumn2 = ColumnDescriptor.CreateNestedArray("matrix", new Int32Type(), arrayDepth: 2);
        Assert.Equal("Array(Array(Int32))", nestedColumn2.GetClickHouseTypeName());
        
        // Triple nested array
        var nestedColumn3 = ColumnDescriptor.CreateNestedArray("cube", new Int32Type(), arrayDepth: 3);
        Assert.Equal("Array(Array(Array(Int32)))", nestedColumn3.GetClickHouseTypeName());
        
        // Scalar (no array)
        var scalarColumn = ColumnDescriptor.Create("scalar", new Int32Type());
        Assert.Equal("Int32", scalarColumn.GetClickHouseTypeName());
    }
    
    [Fact]
    public void ColumnDescriptor_GetClrType_ReturnsCorrectNestedType()
    {
        // Single array
        var arrayColumn = ColumnDescriptor.Create("arr", new Int32Type(), isArray: true);
        Assert.Equal(typeof(int[]), arrayColumn.GetClrType());
        
        // Double nested array
        var nestedColumn2 = ColumnDescriptor.CreateNestedArray("matrix", new Int32Type(), arrayDepth: 2);
        Assert.Equal(typeof(int[][]), nestedColumn2.GetClrType());
        
        // Triple nested array
        var nestedColumn3 = ColumnDescriptor.CreateNestedArray("cube", new Int32Type(), arrayDepth: 3);
        Assert.Equal(typeof(int[][][]), nestedColumn3.GetClrType());
        
        // Scalar (no array)
        var scalarColumn = ColumnDescriptor.Create("scalar", new Int32Type());
        Assert.Equal(typeof(int), scalarColumn.GetClrType());
    }
}