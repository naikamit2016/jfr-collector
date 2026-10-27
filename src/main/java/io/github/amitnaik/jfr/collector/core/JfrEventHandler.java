package io.github.amitnaik.jfr.collector.core;

import java.nio.ByteBuffer;

public interface JfrEventHandler {
    void onEvent(int type, ByteBuffer payload);
}
