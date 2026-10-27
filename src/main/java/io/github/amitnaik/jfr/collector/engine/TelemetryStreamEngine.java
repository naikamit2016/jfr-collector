package io.github.amitnaik.jfr.collector.engine;

import io.github.amitnaik.jfr.collector.core.JfrEventSink;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

public final class TelemetryStreamEngine implements AutoCloseable {

    private RecordingStream rs;
    private final Map<String, List<Consumer<RecordedEvent>>> pendingHandlers = new ConcurrentHashMap<>();
    private ScheduledExecutorService flusher;

    public void subscribe(String eventName, JfrEventSink sink) {
        pendingHandlers.computeIfAbsent(eventName, k -> new ArrayList<>())
                .add(sink::consume);
    }

    public synchronized void startAsync() {
        rs = new RecordingStream();
        rs.enable("jdk.CPULoad").withPeriod(Duration.ofSeconds(1));
        rs.enable("jdk.ThreadAllocationStatistics").withPeriod(Duration.ofSeconds(1));
        rs.enable("jdk.GarbageCollection");
        rs.enable("jdk.GCHeapSummary");
        rs.enable("com.example.AppTelemetry");
        rs.setReuse(true);
        rs.setOrdered(false);

        pendingHandlers.forEach((name, handlers) ->
                rs.onEvent(name, ev -> handlers.forEach(consumer -> {
                    try {
                        consumer.accept(ev);
                    } catch (Exception e) {
                        /* count/log rarely */
                    }
                })));
        rs.startAsync();
    }

    @Override
    public synchronized void close() {
        if (this.rs != null) {
            this.rs.close();
        }
    }
}