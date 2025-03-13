package io.masterkun.nfsonhdfs.writemanager;

import io.masterkun.nfsonhdfs.CallContext;
import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.Utils;
import io.masterkun.nfsonhdfs.vfs.DfsClientCache;
import io.masterkun.nfsonhdfs.vfs.FileHandle;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.nio.ByteBuffer;

public class WriteManagerImpl implements WriteManager {
    private final DfsClientCache dfsClientCache;
    private final long writeHighWatermark;
    private final long writeLowWatermark;
    private final WriteContextFactory factory = new WriteContextFactoryImpl();

    public WriteManagerImpl(DfsClientCache dfsClientCache) {
        this.dfsClientCache = dfsClientCache;
        AppConfig.WriteManagerConfig writeManager = Utils.getServerConfig().getVfs().getWriteManager();
        this.writeHighWatermark = writeManager.getWriteHighWatermark();
        this.writeLowWatermark = writeManager.getWriteLowWatermark();
    }

    @Override
    public long create(long parentFileId, String name, FsPermission permission) throws IOException {
        return factory.create(parentFileId, name, permission, dfsClientCache);
    }

    @Override
    public void handleWrite(long fileId, byte[] data, long offset, int count) throws IOException {
        FileHandle fileHandle = CallContext.getFileHandle(fileId);
        WriteContext context = factory.cacheGet(fileHandle, dfsClientCache);
        long pendingWriteBytes = context.handleWrite(offset, data, count);
        if (pendingWriteBytes > writeHighWatermark) {
            handleCommit(fileId, Math.max(1, offset - writeLowWatermark), 0);
        }
    }

    @Override
    public void handleWrite(long fileId, ByteBuffer data, long offset) throws IOException {
        FileHandle fileHandle = CallContext.getFileHandle(fileId);
        WriteContext context = factory.cacheGet(fileHandle, dfsClientCache);
        int remaining = data.remaining();
        if (remaining > 0) {
            long pendingWriteBytes = context.handleWrite(offset, data);
            if (pendingWriteBytes > writeHighWatermark) {
                handleCommit(fileId, Math.max(1, offset - writeLowWatermark), 0);
            }
        }
    }

    @Override
    public void handleCommit(long fileId, long offset, int count) throws IOException {
        FileHandle fileHandle = CallContext.getFileHandle(fileId);
        WriteContext context = factory.cacheGetOption(fileHandle);
        if (context != null) {
            context.handleCommit(offset, count);
        }
    }
}
