package io.masterkun.nfsonhdfs;

import com.sun.security.auth.UnixNumericGroupPrincipal;
import com.sun.security.auth.UnixNumericUserPrincipal;
import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.Utils;
import io.masterkun.nfsonhdfs.vfs.DfsClientCache;
import io.masterkun.nfsonhdfs.vfs.DfsClientCacheImpl;
import io.masterkun.nfsonhdfs.vfs.HadoopVirtualFileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.acl.Acls;
import org.dcache.nfs.v4.xdr.acemask4;
import org.dcache.nfs.vfs.Stat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.Subject;
import java.net.URI;
import java.util.UUID;

import static org.dcache.nfs.util.UnixSubjects.hasGid;
import static org.dcache.nfs.util.UnixSubjects.hasUid;
import static org.dcache.nfs.util.UnixSubjects.isRootSubject;
import static org.dcache.nfs.v4.xdr.nfs4_prot.ACE4_WRITE_OWNER;

public class HadoopVirtualFileSystemTest {
    private static DFSClient client;
    private static String root;
    private static HadoopVirtualFileSystem fs;

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
        fs = new HadoopVirtualFileSystem(URI.create(root), clientCache);
    }

    @Before
    public void before() {
        TestUtils.contextInit();
    }

    @Test
    public void getStat() throws Exception {
        HdfsFileStatus fileInfo = client.getFileInfo(root);
        final Stat stat = fs.getStatHolder(fileInfo).newStat();
        final NfsIdMapping idMapper = fs.getIdMapper();
        System.out.println(stat);
        System.out.println(idMapper.uidToPrincipal(stat.getUid()));
        System.out.println(idMapper.gidToPrincipal(stat.getGid()));
        Subject subject = new Subject();
        subject.getPrincipals().add(new UnixNumericUserPrincipal(0));
        subject.getPrincipals().add(new UnixNumericGroupPrincipal(0, true));

        final int x = unixToAccessmask(subject, stat);
        System.out.println(x);
        System.out.println(acemask4.toString(x));
    }

    private int unixToAccessmask(Subject subject, Stat stat) {
        int mode = stat.getMode();
        boolean isDir = (mode & Stat.S_IFDIR) == Stat.S_IFDIR;
        int fromUnixMask;

        if (isRootSubject(subject)) {
            fromUnixMask = Acls.toAccessMask(Acls.RBIT | Acls.WBIT | Acls.XBIT, isDir, true);
            fromUnixMask |= ACE4_WRITE_OWNER;
        } else if (hasUid(subject, stat.getUid())) {
            fromUnixMask = Acls.toAccessMask(mode >> 6, isDir, true);
        } else if (hasGid(subject, stat.getGid())) {
            fromUnixMask = Acls.toAccessMask(mode >> 3, isDir, false);
        } else {
            fromUnixMask = Acls.toAccessMask(mode, isDir, false);
        }
        return fromUnixMask;
    }
}
