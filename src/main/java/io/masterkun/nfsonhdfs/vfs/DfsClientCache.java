package io.masterkun.nfsonhdfs.vfs;

import io.masterkun.nfsonhdfs.CallContext;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;

import java.io.IOException;

public interface DfsClientCache {
    /**
     * 获取指定租户的hdfs客户端
     *
     * @param principal 租户principal
     * @return hdfs客户端
     */
    DFSClient getDFSClient(String principal) throws IOException;

    /**
     * 获取租户hdfs客户端，需要从{@link CallContext}中获取租户
     *
     * @return hdfs客户端
     */
    DFSClient getDFSClient() throws IOException;

    /**
     * 获取超级用户的hdfs客户端
     *
     * @return hdfs客户端
     */
    DFSClient getSuperUserDFSClient() throws IOException;

    /**
     * 获取租户的文件输入流，需要从{@link CallContext}中获取租户
     *
     * @param fileId 文件id
     * @return 文件输入流
     */
    DFSInputStream getDfsInputStream(long fileId) throws IOException;

    /**
     * 删除文件输入流缓存，需要从{@link CallContext}中获取租户
     *
     * @param fileId 文件id
     */
    void invalidateInputStream(long fileId);

}
