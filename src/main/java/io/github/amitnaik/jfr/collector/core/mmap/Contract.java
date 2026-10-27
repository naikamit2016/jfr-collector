package io.github.amitnaik.jfr.collector.core.mmap;

import java.nio.ByteOrder;

public final class Contract {
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    public static final int CPU_LOAD = 1, GC = 2, GC_HEAP = 3, THREAD_ALLOC = 4, APP_TELEMETRY = 5;
    private Contract() {}
}
