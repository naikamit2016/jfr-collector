package io.github.amitnaik.jfr.collector.core.mmap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MmapConsumer {

    private static final Logger logger = Logger.getLogger(MmapConsumer.class.getName());

    private static final Path BUFFER_PATH = Path.of("/tmp/jfr_ring.buf");

    static void main(String[] args) throws Exception {
        logger.info(">>> [MMAP CONSUMER] Waiting for ring buffer " + BUFFER_PATH);

        try (MmapRingReader reader = MmapRingReader.awaitOpen(BUFFER_PATH)) {
            logger.info(">>> [MMAP CONSUMER] Attached.");
            int idle = 0;

            while (!Thread.currentThread().isInterrupted()) {
                boolean exists;
                try {
                    exists = reader.poll(MmapConsumer::handle);
                } catch (IllegalStateException ex) {
                    // Should never happen with the new protocol.
                    throw ex;
                }
                if (exists) {
                    idle = 0;
                } else {
                    backoff(idle++);
                }
            }
        }
    }

    /** Spin briefly, then yield, then park, so idle CPU stays near zero. */
    private static void backoff(int idle) {
        if (idle < 100)       Thread.onSpinWait();
        else if (idle < 200)  Thread.yield();
        else                  LockSupport.parkNanos(50_000);   // ~50 µs
    }

    private static void handle(int type, ByteBuffer byteBuffer) {
        switch (type) {
            case Contract.CPU_LOAD -> logger.info(String.format("[EVENT: jdk.CPULoad] User: %.2f%% | System: %.2f%% | Total: %.2f%%%n",
                    byteBuffer.getFloat() * 100, byteBuffer.getFloat() * 100, byteBuffer.getFloat() * 100));

            case Contract.GC -> logger.info(
                    String.format("[EVENT: jdk.GarbageCollection] GC ID: %d | Duration: %.3f ms%n",
                            byteBuffer.getLong(), byteBuffer.getLong() / 1_000_000.0));

            case Contract.GC_HEAP -> {
                // Order must match the serializer: gcId, start, used, committed, reserved
                long gcId = byteBuffer.getLong(), start = byteBuffer.getLong();
                long used = byteBuffer.getLong(), committed = byteBuffer.getLong(), reserved = byteBuffer.getLong();
                logger.info(String.format(
                        "[EVENT: jdk.GCHeapSummary] GC ID: %d | Start: 0x%X | Used: %d MB | Committed: %d MB | Reserved: %d MB%n",
                        gcId, start, used >> 20, committed >> 20, reserved >> 20));
            }

            case Contract.THREAD_ALLOC -> logger.info(String.format(
                    "[EVENT: jdk.ThreadAllocationStatistics] Allocated: %d KB%n", byteBuffer.getLong() / 1024));

            case Contract.APP_TELEMETRY -> {
                long ts = byteBuffer.getLong();
                byte[] bytes = new byte[byteBuffer.getInt()];
                byteBuffer.get(bytes);
                logger.info(String.format("[EVENT: com.example.AppTelemetry] Epoch: %d | Msg: %s%n",
                        ts, new String(bytes, StandardCharsets.UTF_8)));
            }

            default -> logger.log(Level.SEVERE, String.format("[MMAP CONSUMER] Unknown Event Type ID: %d%n", type));
        }
    }
}