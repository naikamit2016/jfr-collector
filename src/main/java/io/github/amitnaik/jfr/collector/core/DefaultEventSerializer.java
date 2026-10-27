package io.github.amitnaik.jfr.collector.core;

import io.github.amitnaik.jfr.collector.core.mmap.Contract;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedObject;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DefaultEventSerializer implements EventSerializer {
    @Override
    public int serialize(RecordedEvent recordedEvent, ByteBuffer byteBuffer) {
        switch (recordedEvent.getEventType().getName()) {
            case "jdk.CPULoad" -> {
                byteBuffer.putFloat(recordedEvent.getFloat("jvmUser"));
                byteBuffer.putFloat(recordedEvent.getFloat("jvmSystem"));
                byteBuffer.putFloat(recordedEvent.getFloat("machineTotal"));
                return Contract.CPU_LOAD;
            }
            case "jdk.GarbageCollection" -> {
                byteBuffer.putLong(recordedEvent.getLong("gcId"));
                byteBuffer.putLong(recordedEvent.getDuration().toNanos());
                return Contract.GC;
            }
            case "jdk.GCHeapSummary" -> {
                RecordedObject hs = recordedEvent.getValue("heapSpace");
                byteBuffer.putLong(recordedEvent.getLong("gcId"));
                byteBuffer.putLong(hs.getLong("start"));
                byteBuffer.putLong(recordedEvent.getLong("heapUsed"));        // same order the consumer reads
                byteBuffer.putLong(hs.getLong("committedSize"));
                byteBuffer.putLong(hs.getLong("reservedSize"));
                return Contract.GC_HEAP;
            }
            case "jdk.ThreadAllocationStatistics" -> {
                byteBuffer.putLong(recordedEvent.getLong("allocated"));
                return Contract.THREAD_ALLOC;
            }
            case "com.example.AppTelemetry" -> {
                String msg = recordedEvent.getString("message");
                byte[] bytes = msg != null ? msg.getBytes(StandardCharsets.UTF_8) : new byte[0];
                byteBuffer.putLong(recordedEvent.getStartTime().toEpochMilli());
                byteBuffer.putInt(bytes.length);
                byteBuffer.put(bytes);
                return Contract.APP_TELEMETRY;
            }
            default -> {
                return 0;
            }
        }
    }
}