package io.masterkun.nfsonhdfs.writemanager;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface WriteContext {

    long handleWrite(long offset, byte[] data, int count) throws IOException;

    long handleWrite(long offset, ByteBuffer buffer) throws IOException;

    void handleCommit(long offset, int count) throws IOException;
}
