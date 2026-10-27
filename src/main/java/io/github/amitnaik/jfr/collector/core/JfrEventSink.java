package io.github.amitnaik.jfr.collector.core;

import jdk.jfr.consumer.RecordedEvent;

/**
 * Extension point for consuming low-overhead JFR events.
 */
@FunctionalInterface
public interface JfrEventSink extends AutoCloseable {

    /**
     * Invoked asynchronously when a subscribed JFR event is emitted by the JVM.
     *
     * @param event Native JDK JFR event
     */
    void consume(RecordedEvent event);

    @Override
    default void close() throws Exception {
        // Optional lifecycle cleanup hook
    }
}
