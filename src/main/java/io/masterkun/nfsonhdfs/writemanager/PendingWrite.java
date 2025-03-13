package io.masterkun.nfsonhdfs.writemanager;

import org.apache.hadoop.hdfs.DFSInputStream;

import java.io.IOException;
import java.io.OutputStream;

sealed interface PendingWrite extends PendingAction permits ByteArrayPendingWrite, ByteBufferPendingWrite {
    @Override
    default int self() {
        return 0;
    }

    default void release() {
        // do nothing
    }

    void writeTo(OutputStream out) throws IOException;

    boolean dataEquals(DFSInputStream in, int count) throws IOException;

    PendingWrite slice(int arrayOffset) throws IOException;
}
