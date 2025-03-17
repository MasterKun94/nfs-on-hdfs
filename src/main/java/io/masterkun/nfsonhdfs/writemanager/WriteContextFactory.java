package io.masterkun.nfsonhdfs.writemanager;

import io.masterkun.nfsonhdfs.vfs.DfsClientCache;
import io.masterkun.nfsonhdfs.vfs.FileHandle;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

public interface WriteContextFactory {
    long create(long parentFileId, String name, FsPermission permission,
                DfsClientCache dfsClientCache) throws IOException;

    WriteContext cacheGetOption(FileHandle handle);

    WriteContext cacheGet(FileHandle handle, DfsClientCache clientCache) throws IOException;
}
