package io.github.amitnaik.jfr.collector.core;

import jdk.jfr.consumer.RecordedEvent;

import java.nio.ByteBuffer;

/**
 * Converts a RecordedEvent into a compact byte payload for IPC / memory-mapped transport.
 */
public interface EventSerializer {
    int serialize(RecordedEvent recordedEvent, ByteBuffer buf);
}