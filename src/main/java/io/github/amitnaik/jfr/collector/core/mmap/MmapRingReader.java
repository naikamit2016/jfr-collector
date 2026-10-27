package io.github.amitnaik.jfr.collector.core.mmap;

import io.github.amitnaik.jfr.collector.core.JfrEventHandler;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public final class MmapRingReader implements AutoCloseable {

    private final MappedByteBuffer mappedByteBuffer;
    private final int capacity, mask;

    private MmapRingReader(MappedByteBuffer mappedByteBuffer, int capacity) {
        this.mappedByteBuffer = mappedByteBuffer;
        this.capacity = capacity;
        this.mask = capacity - 1;
    }

    /** Waits until the producer has created the file, then maps it. */
    public static MmapRingReader awaitOpen(Path path) throws IOException, InterruptedException {
        while (true) {
            if (Files.exists(path)) {
                long size = Files.size(path);
                long capacity = size - MmapRingBuffer.DATA_OFFSET;
                if (capacity > 0 && Long.bitCount(capacity) == 1) {            // producer finished sizing it
                    try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
                         FileChannel channel = file.getChannel()) {
                        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
                        mappedByteBuffer.order(Contract.ORDER);                         // same as producer
                        return new MmapRingReader(mappedByteBuffer, (int) capacity);
                    }
                }
            }
            Thread.sleep(200);
        }
    }

    /** Reads at most one record. Returns false if nothing is available. */
    public boolean poll(JfrEventHandler jfrEventHandler) {
        long tail = (long) MmapRingBuffer.LONG.get(mappedByteBuffer, MmapRingBuffer.TAIL_OFFSET);
        long head = (long) MmapRingBuffer.LONG.getAcquire(mappedByteBuffer, MmapRingBuffer.HEAD_OFFSET);

        if (head < tail) {                       // producer restarted and reset the positions
            MmapRingBuffer.LONG.setRelease(mappedByteBuffer, MmapRingBuffer.TAIL_OFFSET, head);
            return false;
        }
        if (tail == head) return false;

        int index  = MmapRingBuffer.DATA_OFFSET + (int) (tail & mask);
        int len  = mappedByteBuffer.getInt(index);
        int type = mappedByteBuffer.getInt(index + 4);
        if (len < 0 || len > capacity - MmapRingBuffer.REC_HEADER)
            throw new IllegalStateException("Found invalid record at " + tail + " len=" + len);

        byte[] payload = new byte[len];
        int pos = (int) ((tail + MmapRingBuffer.REC_HEADER) & mask);
        int first = Math.min(len, capacity - pos);
        mappedByteBuffer.get(MmapRingBuffer.DATA_OFFSET + pos, payload, 0, first);
        if (first < len) mappedByteBuffer.get(MmapRingBuffer.DATA_OFFSET, payload, first, len - first);

        // Advance the tail only after copying the payload, so the producer can't overwrite it
        MmapRingBuffer.LONG.setRelease(mappedByteBuffer, MmapRingBuffer.TAIL_OFFSET,
                tail + MmapRingBuffer.align8(MmapRingBuffer.REC_HEADER + len));

        jfrEventHandler.onEvent(type, ByteBuffer.wrap(payload).order(Contract.ORDER));
        return true;
    }

    @Override
    public void close() {
        /* mapping is released on GC; nothing to flush for a reader */
    }
}