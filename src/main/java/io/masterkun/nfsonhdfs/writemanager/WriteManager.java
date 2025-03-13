package io.masterkun.nfsonhdfs.writemanager;

import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface WriteManager {

    /**
     * 新建文件，并准备写入数据
     *
     * @param parentFileId 父目录文件id
     * @param name         文件名
     * @param permission   权限
     * @return 新建文件id
     */
    long create(long parentFileId, String name, FsPermission permission) throws IOException;

    /**
     * 写入文件数据包
     *
     * @param fileId 文件id
     * @param data   数据包
     * @param offset 数据包写入到文件的偏移量
     * @param count  写入数据大小
     */
    void handleWrite(long fileId, byte[] data, long offset, int count) throws IOException;

    /**
     * 写入文件数据包
     *
     * @param fileId 文件id
     * @param data   数据包
     * @param offset 数据包写入到文件的偏移量
     */
    void handleWrite(long fileId, ByteBuffer data, long offset) throws IOException;

    /**
     * 提交数据进行持久化
     *
     * @param fileId 文件id
     * @param offset 提交偏移量
     * @param count  提交数据大小
     */
    void handleCommit(long fileId, long offset, int count) throws IOException;
}
