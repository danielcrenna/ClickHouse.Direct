using System.Collections;
using System.Text;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Transports.Protocol;

/// <summary>
/// Reads Native format data from TCP protocol responses
/// </summary>
public class NativeFormatReader
{
    /// <summary>
    /// Parses Native format data from a raw byte array into a Block
    /// </summary>
    public Block ParseNativeData(byte[] data)
    {
        if (data == null || data.Length == 0)
        {
            return Block.CreateFromColumnData(new List<ColumnDescriptor>(), [], 0);
        }
        
        var position = 0;
        
        // Read header
        var columnCount = (int)ReadVarInt(data, ref position);
        var rowCount = (int)ReadVarInt(data, ref position);
        
        if (columnCount == 0 || rowCount == 0)
        {
            return Block.CreateFromColumnData(new List<ColumnDescriptor>(), [], 0);
        }
        
        var columns = new List<ColumnDescriptor>(columnCount);
        var columnDataList = new List<IList>(columnCount);
        
        // Read column metadata and data
        for (var i = 0; i < columnCount; i++)
        {
            // Read column name
            var nameLength = (int)ReadVarInt(data, ref position);
            var name = Encoding.UTF8.GetString(data, position, nameLength);
            position += nameLength;
            
            // Read column type
            var typeLength = (int)ReadVarInt(data, ref position);
            var typeName = Encoding.UTF8.GetString(data, position, typeLength);
            position += typeLength;
            
            // Map type name to IClickHouseType and create column descriptor
            var clickHouseType = MapTypeNameToClickHouseType(typeName);
            var column = ColumnDescriptor.Create(name, clickHouseType);
            columns.Add(column);
            
            // Read column data based on type
            var columnData = ReadColumnData(data, ref position, typeName, rowCount);
            columnDataList.Add(columnData);
        }
        
        return Block.CreateFromColumnData(columns, columnDataList, rowCount);
    }
    
    private IClickHouseType MapTypeNameToClickHouseType(string typeName)
    {
        // Remove any parameters from type name (e.g., "Nullable(Int32)" -> "Nullable")
        var baseType = typeName.Contains('(') ? typeName.Substring(0, typeName.IndexOf('(')) : typeName;
        
        return baseType switch
        {
            "Int8" => new Int8Type(),
            "UInt8" => new UInt8Type(),
            "Int16" => new Int16Type(),
            "UInt16" => new UInt16Type(),
            "Int32" => new Int32Type(),
            "UInt32" => new UInt32Type(),
            "Int64" => new Int64Type(),
            "UInt64" => new UInt64Type(),
            "Float32" => new Float32Type(),
            "Float64" => new Float64Type(),
            "String" => new StringType(),
            "FixedString" => new FixedStringType(0), // Size will be parsed from typeName if needed
            "Date" => new DateType(),
            "DateTime" => new DateTimeType(),
            "DateTime64" => new DateTime64Type(),
            "Bool" => new BoolType(),
            "UUID" => new UuidType(),
            _ => new StringType() // Fallback to string for unknown types
        };
    }
    
    private IList ReadColumnData(byte[] data, ref int position, string typeName, int rowCount)
    {
        // Remove parameters from type name
        var baseType = typeName.Contains('(') ? typeName.Substring(0, typeName.IndexOf('(')) : typeName;
        
        switch (baseType)
        {
            case "Int32":
                var int32Values = new int[rowCount];
                for (var i = 0; i < rowCount; i++)
                {
                    int32Values[i] = BitConverter.ToInt32(data, position);
                    position += 4;
                }
                return int32Values;
                
            case "UInt32":
                var uint32Values = new uint[rowCount];
                for (var i = 0; i < rowCount; i++)
                {
                    uint32Values[i] = BitConverter.ToUInt32(data, position);
                    position += 4;
                }
                return uint32Values;
                
            case "Int64":
                var int64Values = new long[rowCount];
                for (var i = 0; i < rowCount; i++)
                {
                    int64Values[i] = BitConverter.ToInt64(data, position);
                    position += 8;
                }
                return int64Values;
                
            case "UInt64":
                var uint64Values = new ulong[rowCount];
                for (var i = 0; i < rowCount; i++)
                {
                    uint64Values[i] = BitConverter.ToUInt64(data, position);
                    position += 8;
                }
                return uint64Values;
                
            case "Float32":
                var floatValues = new float[rowCount];
                for (var i = 0; i < rowCount; i++)
                {
                    floatValues[i] = BitConverter.ToSingle(data, position);
                    position += 4;
                }
                return floatValues;
                
            case "Float64":
                var doubleValues = new double[rowCount];
                for (var i = 0; i < rowCount; i++)
                {
                    doubleValues[i] = BitConverter.ToDouble(data, position);
                    position += 8;
                }
                return doubleValues;
                
            case "String":
                var stringValues = new string[rowCount];
                for (var i = 0; i < rowCount; i++)
                {
                    var strLength = (int)ReadVarInt(data, ref position);
                    stringValues[i] = Encoding.UTF8.GetString(data, position, strLength);
                    position += strLength;
                }
                return stringValues;
                
            case "UInt8":
            case "Bool":
                var byteValues = new byte[rowCount];
                Array.Copy(data, position, byteValues, 0, rowCount);
                position += rowCount;
                return byteValues;
                
            case "Int8":
                var sbyteValues = new sbyte[rowCount];
                for (var i = 0; i < rowCount; i++)
                {
                    sbyteValues[i] = (sbyte)data[position++];
                }
                return sbyteValues;
                
            case "Int16":
                var int16Values = new short[rowCount];
                for (var i = 0; i < rowCount; i++)
                {
                    int16Values[i] = BitConverter.ToInt16(data, position);
                    position += 2;
                }
                return int16Values;
                
            case "UInt16":
                var uint16Values = new ushort[rowCount];
                for (var i = 0; i < rowCount; i++)
                {
                    uint16Values[i] = BitConverter.ToUInt16(data, position);
                    position += 2;
                }
                return uint16Values;
                
            default:
                throw new NotSupportedException($"Type '{typeName}' is not yet supported for Native format reading");
        }
    }
    
    private static ulong ReadVarInt(byte[] buffer, ref int position)
    {
        ulong value = 0;
        var shift = 0;
        
        while (position < buffer.Length)
        {
            var b = buffer[position++];
            value |= (ulong)(b & 0x7F) << shift;
            
            if ((b & 0x80) == 0)
                return value;
            
            shift += 7;
            if (shift >= 64)
                throw new InvalidOperationException("VarInt too large");
        }
        
        throw new InvalidOperationException("Unexpected end of buffer while reading VarInt");
    }
}