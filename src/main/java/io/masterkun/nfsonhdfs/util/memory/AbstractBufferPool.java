package io.masterkun.nfsonhdfs.util.memory;

import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

public abstract class AbstractBufferPool implements AutoCloseable {

    private static final int HEURISTIC = 64;
    private final NavigableMap<Integer, Queue<ByteBuffer>> buffers = new ConcurrentSkipListMap<>();
    private final Function<Integer, Queue<ByteBuffer>> loader =
            capacity -> new ManyToManyConcurrentArrayQueue<>(HEURISTIC);

    protected abstract ByteBuffer allocate(int capacity);

    public ByteBuffer take(int capacity) {
        var firstEntry = buffers.tailMap(capacity, true).firstEntry();
        if (firstEntry == null) {
            return allocate(capacity);
        } else {
            ByteBuffer poll = firstEntry.getValue().poll();
            if (poll == null) {
                return allocate(capacity);
            } else if (poll.limit() != poll.capacity() || poll.position() != 0) {
                throw new IllegalStateException("buffer reused");
            } else {
                return poll;
            }
        }
    }

    public void give(ByteBuffer buffer) {
        buffers.computeIfAbsent(buffer.capacity(), loader).offer(buffer.clear());
    }

    @Override
    public void close() {
        buffers.clear();
    }
}
