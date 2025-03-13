package io.masterkun.nfsonhdfs.writemanager;

import org.apache.hadoop.hdfs.DFSInputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

public final class ByteArrayPendingWrite implements PendingWrite {

    private final long offset;
    private final int count;
    private final byte[] bytes;
    private final int arrayOffset;

    private ByteArrayPendingWrite(long offset, int count, byte[] bytes) {
        this.offset = offset;
        this.count = count;
        this.bytes = bytes;
        this.arrayOffset = 0;
    }

    private ByteArrayPendingWrite(long offset, int count, byte[] bytes, int arrayOffset) {
        this.offset = offset + arrayOffset;
        this.count = count - arrayOffset;
        this.bytes = bytes.clone();
        this.arrayOffset = arrayOffset;
    }

    public static PendingWrite get(long offset, int count, byte[] bytes) {
        return new ByteArrayPendingWrite(offset, count, bytes.clone());
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(bytes, arrayOffset, count);
    }

    @Override
    public boolean dataEquals(DFSInputStream in, int count) throws IOException {
        byte[] readData = new byte[count];
        int read = in.read(offset, readData, 0, count);
        if (read < count) {
            throw new IllegalArgumentException("bytes read " + read + " is less than write count " + count);
        }
        return Arrays.equals(bytes, 0, count, readData, 0, count);
    }

    @Override
    public PendingWrite slice(int arrayOffset) {
        return new ByteArrayPendingWrite(offset, count, bytes, arrayOffset);
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
        return "ByteArrayPendingWrite{" +
                "offset=" + offset +
                ", count=" + count +
                ", arrayOffset=" + arrayOffset +
                '}';
    }
}
