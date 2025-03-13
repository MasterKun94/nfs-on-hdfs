package io.masterkun.nfsonhdfs.writemanager;

import io.masterkun.nfsonhdfs.TestUtils;
import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.Utils;
import io.masterkun.nfsonhdfs.vfs.DfsClientCache;
import io.masterkun.nfsonhdfs.vfs.DfsClientCacheImpl;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class WriteManagerTest {

    private static WriteManager manager;
    private static DFSClient client;
    private static String root;

    @BeforeClass
    public static void pre() throws Exception {
        TestUtils.kerberosInit();
        AppConfig config = new AppConfig();
        root = "/tmp/" + UUID.randomUUID();
        config.setRootDir(root);
        config.getVfs().getIdMapping().setStrategy(AppConfig.IdMappingStrategy.TEST);
        Utils.init(config);
        DfsClientCache clientCache = new DfsClientCacheImpl();

        client = clientCache.getDFSClient("root");
        client.mkdirs(root);
        manager = new WriteManagerImpl(clientCache);
    }

    @AfterClass
    public static void end() throws Exception {
        client.delete(root, true);
    }

    @Before
    public void before() {
        TestUtils.contextInit();
    }

    @Test
    public void testWriteUnOrdered() throws Exception {
        OutputStream out = client.create(root + "/test-write-unordered", true);
        out.close();
        long fileId = ((DFSOutputStream) out).getFileId();

        byte[] b1 = UUID.randomUUID().toString().getBytes();
        long off1 = 0;
        int count1 = b1.length;
        byte[] b2 = UUID.randomUUID().toString().getBytes();
        long off2 = off1 + count1;
        int count2 = b2.length;
        byte[] b3 = UUID.randomUUID().toString().getBytes();
        long off3 = off2 + count2;
        int count3 = b3.length;
        byte[] b4 = UUID.randomUUID().toString().getBytes();
        long off4 = off3 + count3;
        int count4 = b4.length;
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        bout.write(b1);
        bout.write(b2);
        bout.write(b3);
        bout.write(b4);
        byte[] bytes = bout.toByteArray();
        manager.handleWrite(fileId, b2, off2, count2);
        manager.handleWrite(fileId, b1, off1, count1);
        manager.handleWrite(fileId, b2, off2, count2);
        manager.handleWrite(fileId, b4, off4, count4);
        manager.handleWrite(fileId, b3, off3, count3);
        manager.handleWrite(fileId, b1, off1, count1);
        manager.handleCommit(fileId, 0, 0);
        Thread.sleep(1000);
        HdfsFileStatus status = client.getFileInfo(Utils.getFileIdPath(fileId));
        assertEquals(status.getLen(), bytes.length);
        byte[] read = IOUtils.toByteArray(client.open(Utils.getFileIdPath(fileId)));
        assertArrayEquals(read, bytes);
    }

    @Test
    public void testWriteCommitEachWrite() throws Exception {
        testWrite(true);
    }

    @Test
    public void testWriteCommitFinally() throws Exception {
        testWrite(false);
    }

    private void testWrite(boolean commitEachWrite) throws Exception {
        OutputStream out = client.create(root + "/test-write-commit-" + commitEachWrite, true);
        out.close();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(bout));
        for (int i = 0; i < 1000; i++) {
            writer.write(UUID.randomUUID().toString());
        }
        byte[] bytes = bout.toByteArray();
        long fileId = ((DFSOutputStream) out).getFileId();
        InputStream in = new ByteArrayInputStream(bytes);
        int write = 0;
        byte[] buffer = new byte[8192];
        while (write < bytes.length) {
            int read = in.read(buffer);
            if (read == -1) {
                throw new IllegalArgumentException();
            }
            manager.handleWrite(fileId, ByteBuffer.wrap(buffer, 0, read), write);
            if (commitEachWrite) {
                manager.handleCommit(fileId, write, read);
            }
            write += read;
        }
        manager.handleCommit(fileId, 0, 0);
        Thread.sleep(1000);
        HdfsFileStatus status = client.getFileInfo(Utils.getFileIdPath(fileId));
        assertEquals(status.getLen(), bytes.length);
        byte[] read = IOUtils.toByteArray(client.open(Utils.getFileIdPath(fileId)));
        assertArrayEquals(read, bytes);
    }
}
