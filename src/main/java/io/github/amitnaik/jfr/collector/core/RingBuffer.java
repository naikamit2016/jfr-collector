package io.github.amitnaik.jfr.collector.core;

/**
 * Universal contract for high-performance IPC buffers transferring raw JFR binary frames.
 */
public interface RingBuffer extends AutoCloseable {

    /**
     * Non-blocking write of a serialized byte frame into the buffer.
     *
     * @param payload Encoded event bytes
     * @return true if successfully written, false if the buffer is full or unavailable
     */
    boolean write(int type, byte[] payload, int off, int len);


    @Override
    void close();
}
