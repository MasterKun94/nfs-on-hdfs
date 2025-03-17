package io.masterkun.nfsonhdfs.writemanager;

import io.masterkun.nfsonhdfs.CallContext;
import io.masterkun.nfsonhdfs.util.Utils;
import io.masterkun.nfsonhdfs.vfs.DfsClientCache;
import io.masterkun.nfsonhdfs.vfs.FileHandle;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WriteContextFactoryImpl implements WriteContextFactory {
    private static final Logger LOG = LoggerFactory.getLogger(WriteContextFactoryImpl.class);

    private final Map<FileHandle, WriteContext> CACHE = new ConcurrentHashMap<>(
            Utils.getServerConfig().getVfs().getWriteManager().getWriteThreadMaxNum()
    );

    void cleanUp(FileHandle fileHandle, WriteContext writeContext) {
        CACHE.remove(fileHandle, writeContext);
    }

    @Override
    public long create(long parentFileId, String name, FsPermission permission,
                       DfsClientCache dfsClientCache) throws IOException {
        DFSClient dfsClient = dfsClientCache.getDFSClient();
        DfsClientConf conf = dfsClient.getConf();
        int bufferSize = conf.getIoBufferSize();
        short replication = conf.getDefaultReplication();
        long blockSize = conf.getDefaultBlockSize();
        EnumSet<CreateFlag> createFlags = EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
        String newFileIdPath = Utils.getFileIdPath(parentFileId, name);
        DFSOutputStream out = null;
        try {
            out = dfsClient.create(newFileIdPath,
                    permission,
                    createFlags,
                    false,
                    replication,
                    blockSize,
                    null,
                    bufferSize,
                    null);
            FileHandle fileHandle = CallContext.getFileHandle(out.getFileId());
            WriteContext context = WriteContextImpl.get(fileHandle, dfsClient,
                    dfsClient.createWrappedOutputStream(out, null), this);
            LOG.info("{} created", context);
            CACHE.put(fileHandle, context);
            return fileHandle.fileId();
        } catch (Exception e) {
            if (out != null) {
                out.close();
            }
            throw e;
        }
    }

    /**
     * 从缓存中获取写入上下文，如果没有则返回空
     */
    @Override
    public WriteContext cacheGetOption(FileHandle handle) {
        return CACHE.get(handle);
    }

    /**
     * 从缓存中获取写入上下文，如果没有则创建
     */
    @Override
    public WriteContext cacheGet(FileHandle handle, DfsClientCache clientCache) throws IOException {
        try {
            return CACHE.computeIfAbsent(handle, key -> {
                try {
                    String fileIdPath = Utils.getFileIdPath(key.fileId());
                    DFSClient dfsClient = clientCache.getDFSClient(key.principal());
                    HdfsDataOutputStream out = dfsClient.append(
                            fileIdPath,
                            dfsClient.getConf().getIoBufferSize(),
                            EnumSet.of(CreateFlag.APPEND),
                            null,
                            null
                    );
                    WriteContextImpl ctx = WriteContextImpl.get(key,
                            clientCache.getDFSClient(key.principal()), out, this);
                    LOG.info("{} created for append", ctx);
                    return ctx;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException(e);
            }
        }
    }
}
