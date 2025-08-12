using System.Buffers;

namespace ClickHouse.Direct.Tests.Types.Simd;

internal sealed class BufferSegment : ReadOnlySequenceSegment<byte>
{
    public BufferSegment(Memory<byte> memory)
    {
        Memory = memory;
    }

    public void Append(BufferSegment segment)
    {
        segment.RunningIndex = RunningIndex + Memory.Length;
        Next = segment;
    }
}