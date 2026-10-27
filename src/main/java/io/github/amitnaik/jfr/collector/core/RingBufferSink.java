package io.github.amitnaik.jfr.collector.core;

import io.github.amitnaik.jfr.collector.core.mmap.Contract;
import jdk.jfr.consumer.RecordedEvent;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class RingBufferSink implements JfrEventSink {
    private final RingBuffer ring;
    private final EventSerializer serializer;
    private final ByteBuffer scratch =
            ByteBuffer.allocate(64 * 1024).order(Contract.ORDER);   // heap, reused, one stream thread
    private long dropped;

    public RingBufferSink(RingBuffer ring, EventSerializer serializer) {
        this.ring = ring;
        this.serializer = serializer;
    }

    @Override
    public void consume(RecordedEvent event) {
        scratch.clear();
        int type;
        try {
            type = serializer.serialize(event, scratch);
        } catch (BufferOverflowException ex) {   // oversized message
            dropped++;
            return;
        }
        if (type == 0) {
            // unsupported event
            return;
        }
        if (!ring.write(type, scratch.array(), 0, scratch.position())) {
            dropped++;
        }
    }

    @Override
    public void close() {
        ring.close();
    }
}