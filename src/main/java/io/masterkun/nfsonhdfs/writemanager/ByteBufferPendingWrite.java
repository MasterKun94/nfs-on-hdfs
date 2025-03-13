package io.masterkun.nfsonhdfs.writemanager;

import io.masterkun.nfsonhdfs.util.Utils;
import io.masterkun.nfsonhdfs.util.memory.CustomPooledMemoryManagerFactory;
import io.masterkun.nfsonhdfs.util.memory.DirectByteBufferPool;
import org.apache.hadoop.hdfs.DFSInputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;

public final class ByteBufferPendingWrite implements PendingWrite {
    private static final DirectByteBufferPool BYTE_BUFFER_POOL = CustomPooledMemoryManagerFactory.BYTE_BUFFER_POOL;
    private static final int BYTES_TL_LEN = Utils.getServerConfig()
            .getVfs()
            .getWriteManager()
            .getWriteBufferLength();
    private static final ThreadLocal<byte[]> BYTES_TL = ThreadLocal.withInitial(() -> new byte[BYTES_TL_LEN]);
    private static final VarHandle VALUE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            VALUE = l.findVarHandle(ByteBufferPendingWrite.class, "released", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final long offset;
    private final int count;
    private final ByteBuffer data;
    @SuppressWarnings("all")
    private volatile boolean released = false;

    private ByteBufferPendingWrite(long offset, int count, ByteBuffer data) {
        this.offset = offset;
        this.count = count;
        this.data = data;
    }

    private ByteBufferPendingWrite(long offset, int count, ByteBuffer data, int arrayOffset) {
        this.offset = offset + arrayOffset;
        this.count = count - arrayOffset;
        this.data = data.position(data.position() + arrayOffset);
    }

    public static PendingWrite get(long offset, int count, ByteBuffer data) {
        ByteBuffer buffer = BYTE_BUFFER_POOL.take(count).limit(count);
        buffer.put(data);
        buffer.flip();
        return new ByteBufferPendingWrite(offset, count, buffer);
    }

    @Override
    public void release() {
        if (VALUE.compareAndSet(this, false, true)) {
            BYTE_BUFFER_POOL.give(data);
        }
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        int remaining = count;
        if (data.hasArray()) {
            out.write(data.array(), data.arrayOffset(), remaining);
        } else {
            final byte[] bytes = BYTES_TL.get();
            while (remaining > 0) {
                int read = Math.min(BYTES_TL_LEN, remaining);
                data.get(bytes, 0, read);
                out.write(bytes, 0, read);
                remaining -= read;
            }
        }
    }

    @Override
    public boolean dataEquals(DFSInputStream in, int count) throws IOException {
        ByteBuffer buffer = BYTE_BUFFER_POOL.take(count);
        int read = in.read(offset, buffer);
        buffer.flip();
        if (read < count) {
            throw new IllegalArgumentException("bytes read " + read + " is less than write count " + count);
        }
        final int mismatch = data.mismatch(buffer);
        return mismatch == -1 || mismatch >= count;
    }

    @Override
    public PendingWrite slice(int arrayOffset) {
        return new ByteBufferPendingWrite(offset, count, data, arrayOffset);
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public String toString() {
        return "ByteBufferPendingWrite{" +
                "offset=" + offset +
                ", count=" + count +
                ", data=" + data +
                ", released=" + released +
                '}';
    }
}
