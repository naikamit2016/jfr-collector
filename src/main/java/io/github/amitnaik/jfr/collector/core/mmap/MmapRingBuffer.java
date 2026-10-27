package io.github.amitnaik.jfr.collector.core.mmap;

import io.github.amitnaik.jfr.collector.core.RingBuffer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

public final class MmapRingBuffer implements RingBuffer {

    static final int HEAD_OFFSET = 0;
    static final int TAIL_OFFSET = 64;
    static final int DATA_OFFSET = 128;
    static final int REC_HEADER  = 8;     // int length + int type

    static final VarHandle LONG = MethodHandles.byteBufferViewVarHandle(long[].class, Contract.ORDER);

    private final MappedByteBuffer buffer;
    private final int capacity, mask;
    private final AtomicLong dropped = new AtomicLong();

    public MmapRingBuffer(Path path, int capacity) throws IOException {
        if (Integer.bitCount(capacity) != 1)
            throw new IllegalArgumentException("capacity must be a power of two");
        this.capacity = capacity;
        this.mask = capacity - 1;
        try (RandomAccessFile f = new RandomAccessFile(path.toFile(), "rw")) {
            f.setLength(DATA_OFFSET + (long) capacity);
            buffer = f.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, DATA_OFFSET + capacity);
        }
        buffer.order(Contract.ORDER);   // MUST match the consumer
        LONG.setRelease(buffer, HEAD_OFFSET, 0L);   // fresh start; avoids stale positions from a previous run
        LONG.setRelease(buffer, TAIL_OFFSET, 0L);
    }

    /** Single producer */
    @Override
    public boolean write(int type, byte[] payload, int off, int len) {
        long head = (long) LONG.get(buffer, HEAD_OFFSET);
        long tail = (long) LONG.getAcquire(buffer, TAIL_OFFSET);
        int total = align8(REC_HEADER + payload.length);

        if (head - tail + total > capacity) {
            dropped.incrementAndGet();
            return false;
        }

        int index = DATA_OFFSET + (int) (head & mask);
        buffer.putInt(index, payload.length);
        buffer.putInt(index + 4, type);
        copyIn(head + REC_HEADER, payload, off, len);

        LONG.setRelease(buffer, HEAD_OFFSET, head + total);
        return true;
    }

    private void copyIn(long position, byte[] src, int off, int len) {
        int index = (int) (position & mask);
        int first = Math.min(len, capacity - index);
        buffer.put(DATA_OFFSET + index, src, off, first);
        if (first < src.length) {
            buffer.put(DATA_OFFSET, src, first + off, len - first);
        }
    }

    static int align8(int n) {
        return (n + 7) & ~7;
    }
    public long dropped()    {
        return dropped.get();
    }

    @Override
    public void close() {
        buffer.force();
    }
}