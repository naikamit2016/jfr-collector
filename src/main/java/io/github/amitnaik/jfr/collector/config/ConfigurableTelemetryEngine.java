package io.github.amitnaik.jfr.collector.config;

import io.github.amitnaik.jfr.collector.core.DefaultEventSerializer;
import io.github.amitnaik.jfr.collector.core.RingBuffer;
import io.github.amitnaik.jfr.collector.core.RingBufferSink;
import io.github.amitnaik.jfr.collector.core.mmap.MmapRingBuffer;
import io.github.amitnaik.jfr.collector.engine.TelemetryStreamEngine;
import jdk.jfr.Recording;
import jdk.jfr.RecordingState;

import java.io.IOException;

// Configure in the application
public class ConfigurableTelemetryEngine implements AutoCloseable {

    private final TelemetryStreamEngine engine;
    private Recording activeRecording;

    public ConfigurableTelemetryEngine(JfrStreamConfig config) throws IOException {
        if (!config.isEnabled()) {
            this.engine = null;
            return;
        }

        // 2. Instantiate the engine and subscriptions
        this.engine = new TelemetryStreamEngine();
        for (String eventName : config.getAllowList()) {
            this.engine.subscribe(eventName, new RingBufferSink(createRingBuffer(config), new DefaultEventSerializer()));
        }
    }

    public void start() throws IOException {
        if (engine != null) {
            engine.startAsync();
        }
    }

    @Override
    public void close() {
        if (engine != null) {
            engine.close();
        }
        if (activeRecording != null && activeRecording.getState() == RecordingState.RUNNING) {
            activeRecording.stop();
            activeRecording.close();
        }
    }

    private static RingBuffer createRingBuffer(JfrStreamConfig config) throws IOException {
        return switch (config.getTransport()) {
            case MMAP -> new MmapRingBuffer(config.getBufferPath(), config.getBufferCapacityBytes());
        };
    }
}