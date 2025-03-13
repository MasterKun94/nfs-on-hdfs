package io.masterkun.nfsonhdfs.other;

import io.masterkun.nfsonhdfs.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;

public class HdfsIdPathTest {

    @Test
    public void test() throws IOException {
        System.setProperty("java.security.krb5.conf", "src/test/resources/krb5.conf");
        UserGroupInformation.loginUserFromKeytab(
                "admin/admin@HADOOP.COM",
                "src/test/resources/admin.keytab"
        );
        Configuration conf = new Configuration();
        final URI resolvedURI = Utils.getResolvedURI("/tmp");
        System.out.println(resolvedURI);
        DFSClient client = new DFSClient(resolvedURI, conf);
        final OutputStream outputStream = client.create("/tmp/cmk/test.txt", true);
        final HdfsDataOutputStream dfsOut = client.createWrappedOutputStream(
                (DFSOutputStream) outputStream, null);
        dfsOut.write("test".getBytes(StandardCharsets.UTF_8));
        dfsOut.hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        final long fileId = ((DFSOutputStream) outputStream).getFileId();
        System.out.println(fileId);
        System.out.println(dfsOut.getPos());
        dfsOut.close();
        final HdfsFileStatus fileInfo = client.getFileInfo(Utils.getFileIdPath(fileId));
        System.out.println(fileInfo.getFileId());

        final ContentSummary summary = client.getNamenode().getContentSummary("/tmp/cmk");
        System.out.println(summary.getSpaceConsumed());
        System.out.println(summary.getSpaceQuota());
        System.out.println(summary.getFileAndDirectoryCount());
        System.out.println(summary.getQuota());

        HdfsFileStatus f = client.getFileInfo(Utils.getFileIdPath(fileId, ".."));
        System.out.println(f.getPermission());
        System.out.println(Utils.getMode(f));
        System.out.println(Utils.getPermission(Utils.getMode(f)));

        System.out.println(f.getFileId());
        System.out.println(client.getFileInfo("/tmp/cmk").getFileId());


    }
}
