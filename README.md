# Mmap based JFR Telemetry Streaming event collector

This architecture streams JFR events from a Spring application to an external process using a shared memory-mapped 
ring buffer (MmapRingBuffer). It’s completely framework-agnostic and relies on zero third-party dependencies to keep 
the footprint as small as possible.

---

## Overview

```text
[ Spring application Producer ] 
      │ (JFR EventStream)
      ▼
[ RingBufferSink ] ──(Serialize) ──>[ MmapRingBuffer ] ──(write) ──> [ /tmp/jfr_ring.buf (Mmap) ] ──(Read)──> [ Off-Process Consumer ]
```

1. **Producer Side:** Listens to live JFR events (`jdk.CPULoad`, `jdk.GarbageCollection`, `jdk.GCHeapSummary`, `jdk.ThreadAllocationStatistics`, and `com.example.AppTelemetry`) via `EventStream`.
2. **Serialization:** The serializer writes a payload and returns the event type ID. MmapRingBuffer adds the [int length][int type], pads to 8 bytes, and then publishes.
3. **Shared Memory (`MmapRingBuffer`):** Writes serialized payloads into a memory-mapped file (`/tmp/jfr_ring.buf`) and keep track of head and tail pointers.
4. **Consumer Side (`MmapConsumer`):** An independent process polls the buffer with acquire/release ordering.

---

## Binary Contract Protocol & Memory Layout

Memory layout of the mapped file (default path /tmp/jfr_ring.buf; prefer /dev/shm to avoid disk writeback):

* **Offset `0` (8 bytes):** write head, a monotonic byte counter written only by the producer 
* **Offset `64` (8 bytes):** read tail, a monotonic byte counter written only by the consumer 
* **Offset `128+` (Data Ring Region):** the data ring, of capacity bytes (a power of two). The file size is 128 + capacity.
* Record format, starting at 128 + (pos & mask):
  * `[4 bytes]` — payload length (int)
  *  `[4 bytes]` — event type ID (int: 1 = CPULoad, 2 = GarbageCollection, 3 = GCHeapSummary, 4 = ThreadAllocationStatistics, 5 = AppTelemetry)
  * `[N bytes]` — payload, which may wrap around the end of the ring
  * Aligns to 8-byte boundaries to keep headers from splitting across wrap points
* The next position is pos + align8(8 + N). All multi-byte values are little-endian.
  * Points to note:
    * The producer writes the full record, then publishes by advancing the head.
    * The consumer copies the payload out, then advances the tail.
    * A full buffer means the producer drops the event and counts it.


---

## Event Type ID Reference

| ID | Event Name | Payload Structure |
| :--- | :--- | :--- |
| **1** | `jdk.CPULoad` | 3 × `float` (`jvmUser`, `jvmSystem`, `machineTotal`) |
| **2** | `jdk.GarbageCollection` | 2 × `long` (`gcId`, `durationNanos`) |
| **3** | `jdk.GCHeapSummary` | 5 × `long` (`gcId`, `start`, `heapUsed`, `committedSize`, `reservedSize`) |
| **4** | `jdk.ThreadAllocationStatistics` | 1 × `long` (`allocated` bytes) |
| **5** | `com.example.AppTelemetry` | `long` (Timestamp) + `int` (Str Len) + `byte[]` (UTF-8 Message) |

---

## Getting Started

### Requisites
* JDK 17 minimum is required for producer and consumer.
* Permissions: Both processes need read/write access to the file, and the consumer needs write access because it writes the tail.

### Step 1: Start the Spring Boot Producer Application
Run your Spring Boot application with HotSpot JFR enabled and persistent disk chunking 
active so the stream engine can ingest background events -

```bash
java -jar target/demo.jar
```

### Step 2: Run the Off-Process Consumer
In a separate window, compile and launch the consumer application pointing to the shared memory-mapped 
ring buffer file -

```bash
java io.github.amitnaik.jfr.collector.consumer.mmap.MmapConsumer
```

### Step 3: Trigger Telemetry Events
Force an explicit garbage collection or system load event to verify end-to-end streaming -

```bash
jcmd $(pgrep -f demo) GC.run
```

#### Expected Consumer Output
```text
INFO: >>> [MMAP CONSUMER] Waiting for ring buffer /tmp/jfr_ring.buf
Oct 25, 2026 4:56:40 PM io.github.amitnaik.jfr.collector.core.mmap.MmapConsumer main
INFO: >>> [MMAP CONSUMER] Attached.
Oct 25, 2026 4:56:40 PM io.github.amitnaik.jfr.collector.core.mmap.MmapConsumer handle
INFO: [EVENT: jdk.ThreadAllocationStatistics] Allocated: 0 KB
Oct 25, 2026 4:56:40 PM io.github.amitnaik.jfr.collector.core.mmap.MmapConsumer handle
INFO: [EVENT: jdk.ThreadAllocationStatistics] Allocated: 67 KB
Oct 25, 2026 4:56:41 PM io.github.amitnaik.jfr.collector.core.mmap.MmapConsumer handle
INFO: [EVENT: jdk.CPULoad] User: 0.00% | System: 0.00% | Total: 29.93%

```

---

## Resiliency & Safety Features

* The producer first writes the full record, then advances the write head with a release store. 
The consumer reads the head with an acquire load, so any record at or below the head is complete and visible. If the producer shuts down mid-write, 
the head was never advanced, so the consumer never sees a half-written record.
* It fails with error on corruption. Records are [int len][int type][payload], padded to 8 bytes. 
An invalid length outside the valid range throws IllegalStateException.
* **Producer Restart handling:** 
  * On startup the producer initializes both the head and tail positions to zero.
  * The reader re-reads the tail from the file on every poll().
  * If a restart races with a read in progress, the reader can write an old tail over the new initialized one. The head < tail check catches that, 
and the reader resets it's tail to the head.
  * Known gap - if the producer restarts and writes past the old tail before the reader polls, the reset isn’t detected. 
A header with a magic number, version, capacity and a counter (bumped by the producer on startup) closes this. 
The reader would remap when the changes.
* **Startup ordering.:**
  * awaitOpen waits until the file exists and its size is 128 + a power of two, so a reader that starts first never maps a half-created file.
* **Backpressure and isolation.**
  * The producer never gets blocked. When the ring is full, it drops the event and increments a counter.
  * A dead or slow consumer only causes drops, never have impact an application.
  * Each side writes only its own pointer (the producer writes the head, the consumer writes the tail), so there is no cross-process locking.

## Limits
* It is single-producer, single-consumer only.
* It must run on the same host. Shared memory such as /dev/shm or a memory-backed emptyDir works. 
NFS and similar network or virtualized filesystems don’t guarantee mmap coherence.
* Both sides must use Contract.ORDER (little-endian) and the same type IDs and field order.
