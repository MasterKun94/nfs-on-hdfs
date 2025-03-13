package io.masterkun.nfsonhdfs.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryYamlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.masterkun.nfsonhdfs.DummyVFS;
import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.Utils;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DistributedVfsCacheTest {

    private static HazelcastInstance hazelcastInstance;
    private static AtomicLong adder;
    private Subject subject = new Subject();
    private VirtualFileSystem vfs;
    private DistributedVfsCache vfsCache;
    private Inode root;

    @BeforeClass
    public static void init() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        AppConfig appConfig = mapper.readValue(new File("src/test/resources/nfs-server.yaml"), AppConfig.class);
        Utils.init(appConfig);
        Config config = new InMemoryYamlConfig(mapper.writeValueAsString(Collections.singletonMap("hazelcast", appConfig.getHazelcast())));
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        adder = new AtomicLong();
    }

    @Before
    public void setUp() throws Exception {
        vfs = spy(new DummyVFS());
        Utils.init((CacheLoaderHelper) vfs);
        root = vfs.getRootInode();
        vfsCache = new DistributedVfsCache(vfs, hazelcastInstance, Long.toString(adder.incrementAndGet()));
    }

    @After
    public void cleanup() throws Exception {
        vfsCache.clearCache();
    }

    @Test
    public void shouldReadThroughOnEmptyCache() throws IOException {
        final Stat getattr = vfsCache.getattr(root);
        final Stat getattr1 = verify(vfs).getattr(root);
    }

    @Test
    public void shouldReadThroughOnEmptyLookupCache() throws IOException {

        createFile(root, "foo");
        vfsCache.lookup(root, "foo");
        verify(vfs).lookup(root, "foo");
    }

    @Test
    public void shouldUseLookupCache() throws IOException {

        createFile(root, "foo");
        vfsCache.lookup(root, "foo");
        vfsCache.lookup(root, "foo");
        verify(vfs, times(1)).lookup(root, "foo");
    }

    @Test
    public void shouldUseCachedValue() throws IOException {

        vfsCache.getattr(root);
        vfsCache.getattr(root);
        verify(vfs, times(1)).getattr(root);
    }

    @Test
    public void shouldWriteThrough() throws IOException {

        Stat s = new Stat();
        vfsCache.setattr(root, s);

        verify(vfs).setattr(root, s);
    }

    @Test
    public void shouldInvalidateCacheOnWrite() throws IOException {

        Stat s = new Stat();

        vfsCache.getattr(root);
        vfsCache.setattr(root, s);
        vfsCache.getattr(root);
        verify(vfs, times(2)).getattr(root);
    }

    @Test
    public void shouldInvalidateOwnCacheEntryOnly() throws IOException {

        Stat s = new Stat();

        Inode foo = createFile(root, "foo");
        Inode bar = createFile(root, "bar");

        vfsCache.getattr(foo);
        vfsCache.setattr(bar, s);
        vfsCache.getattr(foo);
        verify(vfs, times(1)).getattr(foo);
    }

    @Test
    public void shouldInvalidateCacheOnCreate() throws IOException {

        vfsCache.getattr(root);
        vfsCache.create(root, Stat.Type.REGULAR, "foo", subject, 0640);
        vfsCache.getattr(root);
        verify(vfs, times(2)).getattr(root);
    }

    @Test
    public void shouldInvalidateCacheOnMkDir() throws IOException {

        vfsCache.getattr(root);
        vfsCache.mkdir(root, "foo", new Subject(), 0600);
        vfsCache.getattr(root);
        verify(vfs, times(2)).getattr(root);
    }

    @Test
    public void shouldInvalidateCacheOnMove() throws IOException {

        Inode src = createDir(root, "dirOne");
        Inode dst = createDir(root, "dirTwo");
        createFile(src, "foo");

        vfsCache.getattr(src);
        vfsCache.getattr(dst);

        vfsCache.move(src, "foo", dst, "bar");

        vfsCache.getattr(src);
        vfsCache.getattr(dst);

        verify(vfs, times(2)).getattr(src);
        verify(vfs, times(2)).getattr(dst);
    }

    @Test
    public void shouldInvalidateCacheOnRemove() throws IOException {

        createFile(root, "foo");
        vfsCache.getattr(root);
        vfsCache.remove(root, "foo");
        vfsCache.getattr(root);

        verify(vfs, times(2)).getattr(root);
    }

    @Test
    public void shouldInvalidateCacheOnSymlink() throws IOException {

        Inode foo = createFile(root, "foo");
        vfsCache.getattr(root);
        vfsCache.getattr(foo);
        vfsCache.symlink(root, "bar", "foo", subject, 0640);
        vfsCache.getattr(root);
        vfsCache.getattr(foo);

        verify(vfs, times(2)).getattr(root);
        verify(vfs, times(1)).getattr(foo);
    }

    @Test
    public void shouldInvalidateCacheOnLink() throws IOException {

        Inode src = createDir(root, "dirOne");
        Inode dst = createDir(root, "dirTwo");
        Inode file = createFile(src, "foo");

        vfsCache.getattr(src);
        vfsCache.getattr(dst);
        vfsCache.getattr(file);

        vfsCache.link(dst, file, "bar", subject);

        vfsCache.getattr(src);
        vfsCache.getattr(dst);
        vfsCache.getattr(file);

        verify(vfs, times(1)).getattr(src);
        verify(vfs, times(2)).getattr(dst);
        verify(vfs, times(2)).getattr(file);
    }

    @Test
    public void shouldInvalidateCacheOnXattrSet() throws IOException {

        Inode file = createFile(root, "foo");

        vfsCache.getattr(file);

        vfsCache.setXattr(file, "attr1", new byte[]{0x01}, VirtualFileSystem.SetXattrMode.CREATE);
        vfsCache.getattr(file);

        verify(vfs, times(2)).getattr(file);
    }

    @Test
    public void shouldInvalidateCacheOnXattrRemove() throws IOException {

        Inode file = createFile(root, "foo");

        vfsCache.setXattr(file, "attr1", new byte[]{0x01}, VirtualFileSystem.SetXattrMode.CREATE);
        vfsCache.getattr(file);
        vfsCache.removeXattr(file, "attr1");
        vfsCache.getattr(file);

        verify(vfs, times(2)).getattr(file);
    }

    @Test
    public void shouldReadThroughOnEmptyReaddirCache() throws IOException {

        vfsCache.list(root, DirectoryStream.ZERO_VERIFIER, 0L);
        verify(vfs).list(root, DirectoryStream.ZERO_VERIFIER, 0L);
    }

    @Ignore
    @Test
    public void shouldUseReaddirCache() throws IOException {

        vfsCache.list(root, DirectoryStream.ZERO_VERIFIER, 0L);
        vfsCache.list(root, DirectoryStream.ZERO_VERIFIER, 0L);

        verify(vfs, times(1)).list(root, DirectoryStream.ZERO_VERIFIER, 0L);
    }

    @Test
    public void shouldUseReaddirCacheWithCookie() throws IOException {

        DirectoryStream stream = vfsCache.list(root, DirectoryStream.ZERO_VERIFIER, 0L);
        vfsCache.list(root, stream.getVerifier(), 5L);

        verify(vfs, times(1)).list(root, DirectoryStream.ZERO_VERIFIER, 0L);
    }

    @Ignore
    @Test
    public void shouldReadThroughReaddirCacheOnUnknownVerifier() throws IOException {

        vfsCache.list(root, DirectoryStream.ZERO_VERIFIER, 0L);
        vfsCache.list(root, Arrays.copyOf(new byte[]{0x01}, nfs4_prot.NFS4_VERIFIER_SIZE), 0L);

        verify(vfs, times(2)).list(root, DirectoryStream.ZERO_VERIFIER, 0L);
    }

    private Inode createFile(Inode parent, String name) throws IOException {
        return vfs.create(parent, Stat.Type.REGULAR, name, subject, 0640);
    }

    private Inode createDir(Inode parent, String name) throws IOException {
        return vfs.mkdir(parent, name, subject, 0640);
    }
}
