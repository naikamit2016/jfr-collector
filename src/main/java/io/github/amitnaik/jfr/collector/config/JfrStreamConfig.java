package io.github.amitnaik.jfr.collector.config;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class JfrStreamConfig {

    public enum TransportType {
        MMAP
    }

    private boolean enabled = true;
    private TransportType transport = TransportType.MMAP;
    private Path bufferPath = Path.of("/tmp/jfr_ring.buf");
    private int bufferCapacityBytes = 1024 * 1024; // 1 MB
    private List<String> allowList = new ArrayList<>();

    public TransportType getTransport() {
        return transport;
    }

    public void setTransport(TransportType transport) {
        this.transport = transport;
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public Path getBufferPath() { return bufferPath; }
    public void setBufferPath(Path bufferPath) { this.bufferPath = bufferPath; }

    public int getBufferCapacityBytes() { return bufferCapacityBytes; }
    public void setBufferCapacityBytes(int bufferCapacityBytes) { this.bufferCapacityBytes = bufferCapacityBytes; }

    public List<String> getAllowList() { return allowList; }
    public void setAllowList(List<String> allowList) { this.allowList = allowList; }
}