package io.masterkun.nfsonhdfs.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import io.masterkun.nfsonhdfs.cache.CacheLoaderHelper;
import io.masterkun.nfsonhdfs.cache.LookupCacheLoaderFactory;
import io.masterkun.nfsonhdfs.cache.StatCacheLoaderFactory;
import io.masterkun.nfsonhdfs.idmapping.ClientSftpIdMappingFactory;
import io.masterkun.nfsonhdfs.idmapping.FileIdMappingFactory;
import io.masterkun.nfsonhdfs.idmapping.IdMappingFactory;
import io.masterkun.nfsonhdfs.idmapping.LocalShellIdMappingFactory;
import io.masterkun.nfsonhdfs.idmapping.PasswdIdMappingFactory;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.cache.caffeine.CacheMetricsCollector;
import io.prometheus.client.hotspot.BufferPoolsExports;
import io.prometheus.client.hotspot.ClassLoadingExports;
import io.prometheus.client.hotspot.GarbageCollectorExports;
import io.prometheus.client.hotspot.MemoryAllocationExports;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import io.prometheus.client.hotspot.StandardExports;
import io.prometheus.client.hotspot.VersionInfoExports;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.SimpleIdMap;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystemException;

import static io.masterkun.nfsonhdfs.util.Constants.INODEID_PATH_PREFIX;


public class Utils {
    public static final NfsIdMapping SIMPLE_ID_MAPPING = new SimpleIdMap();
    private static final CacheMetricsCollector COLLECTOR = new CacheMetricsCollector().register();
    private static final Configuration hadoopConf;
    private static AppConfig appConfig;
    private static IdMappingFactory idMappingFactory;
    private static boolean initialized = false;

    static {
        hadoopConf = new Configuration();
        hadoopConf.addResource(Constants.NFS_SERVER_SITE_FILE);
    }

    public static NfsIdMapping getNfsIdMapping() {
        return idMappingFactory.get();
    }

    @VisibleForTesting
    public static void init(AppConfig appConfig) {
        Utils.appConfig = appConfig;
        idMappingFactory = switch (appConfig.getVfs().getIdMapping().getStrategy()) {
            case LOCAL -> new LocalShellIdMappingFactory();
            case CLIENT -> new ClientSftpIdMappingFactory();
            case FILE -> new FileIdMappingFactory();
            case TEST -> new PasswdIdMappingFactory();
        };
    }

    @VisibleForTesting
    public static void init(CacheLoaderHelper clh) {
        LookupCacheLoaderFactory.setInnerVfs(clh);
        StatCacheLoaderFactory.setInnerVfs(clh);
    }

    public static CacheMetricsCollector getCacheMetricsCollector() {
        return COLLECTOR;
    }

    public static AppConfig getServerConfig() {
        return appConfig;
    }

    public static FileSystem getFs() throws IOException {
        return FileSystem.get(hadoopConf);
    }

    public static FsPermission getPermission(int mode) {
        return new FsPermission(mode);
    }

    public static int getMode(HdfsFileStatus status) {
        int type;
        if (status.isFile()) {
            type = Stat.S_IFREG;
        } else if (status.isDirectory()) {
            type = Stat.S_IFDIR;
        } else if (status.isSymlink()) {
            type = Stat.S_IFLNK;
        } else {
            throw new IllegalArgumentException("unknown file " + status);
        }
        return type | status.getPermission().toShort();
    }

    public static URI getResolvedURI(String exportPath)
            throws IOException {
        FileSystem fs = getFs();
        URI fsURI = fs.getUri();
        String scheme = fs.getScheme();
        if (scheme.equalsIgnoreCase(FsConstants.VIEWFS_SCHEME)) {
            ViewFileSystem viewFs = (ViewFileSystem) fs;
            ViewFileSystem.MountPoint[] mountPoints = viewFs.getMountPoints();
            for (ViewFileSystem.MountPoint mount : mountPoints) {
                String mountedPath = mount.getMountedOnPath().toString();
                if (exportPath.startsWith(mountedPath)) {
                    String subpath = exportPath.substring(mountedPath.length());
                    fsURI = mount.getTargetFileSystemURIs()[0].resolve(subpath);
                    break;
                }
            }
        } else if (scheme.equalsIgnoreCase(HdfsConstants.HDFS_URI_SCHEME)) {
            fsURI = fsURI.resolve(exportPath);
        }

        if (!fsURI.getScheme().equalsIgnoreCase(HdfsConstants.HDFS_URI_SCHEME)) {
            throw new FileSystemException("Only HDFS is supported as underlying"
                    + "FileSystem, fs scheme:" + scheme + " uri to be added" + fsURI);
        }
        return fsURI;
    }

    public static Configuration getHadoopConf() {
        return hadoopConf;
    }

    public static long getFileId(Inode inode) {
        return Longs.fromByteArray(inode.getFileId());
    }

    public static Inode getInode(long fileId) {
        return Inode.forFile(Longs.toByteArray(fileId));
    }

    public static String getFileIdPath(long fileId) {
        return INODEID_PATH_PREFIX + fileId;
    }

    public static String getFileIdPath(long parentId, String fileName) {
        return INODEID_PATH_PREFIX + parentId + "/" + fileName;
    }

    public static synchronized void initializeMetricsExport() {
        if (!initialized) {
            initialized = true;
            final CollectorRegistry registry = CollectorRegistry.defaultRegistry;
            new StandardExports().register(registry);
            new MemoryPoolsExports().register(registry);
            new MemoryAllocationExports().register(registry);
            new BufferPoolsExports().register(registry);
            new GarbageCollectorExports().register(registry);
            new ThreadExports().register(registry);
            new ClassLoadingExports().register(registry);
            new VersionInfoExports().register(registry);
        }
    }
}
