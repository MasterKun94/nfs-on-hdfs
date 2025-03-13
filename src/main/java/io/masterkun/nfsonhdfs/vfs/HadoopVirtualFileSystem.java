package io.masterkun.nfsonhdfs.vfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import io.masterkun.nfsonhdfs.cache.CacheKey;
import io.masterkun.nfsonhdfs.cache.CacheLoaderHelper;
import io.masterkun.nfsonhdfs.cache.StatHolder;
import io.masterkun.nfsonhdfs.util.Utils;
import io.masterkun.nfsonhdfs.writemanager.WriteManager;
import io.masterkun.nfsonhdfs.writemanager.WriteManagerImpl;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.ipc.RemoteException;
import org.dcache.nfs.util.SubjectHolder;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.TreeSet;

public class HadoopVirtualFileSystem implements VirtualFileSystem, CacheLoaderHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopVirtualFileSystem.class);
    private final long rootFileId;
    private final DfsClientCache dfsClientCache;
    private final WriteManager writeManager;

    public HadoopVirtualFileSystem(URI export) throws IOException {
        this(export, new DfsClientCacheImpl());
    }

    public HadoopVirtualFileSystem(URI export, DfsClientCache dfsClientCache) throws IOException {
        this.dfsClientCache = dfsClientCache;
        this.rootFileId = dfsClientCache.getSuperUserDFSClient()
                .getFileInfo(export.getPath())
                .getFileId();
        this.writeManager = new WriteManagerImpl(dfsClientCache);
    }

    @Override
    public int access(Subject subject, Inode inode, int mode) {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.access(subject={}, inode={}, mode={})",
                    new SubjectHolder(subject), fileId, Stat.modeToString(mode));
        }
        return mode;
    }

    @Override
    public Inode create(Inode parent, Stat.Type type, String name, Subject subject, int mode) throws IOException {
        long parentFileId = Utils.getFileId(parent);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.create(parent={}, type={}, name={}, subject={}, mode={})",
                    parentFileId, type, name, new SubjectHolder(subject), Stat.modeToString(mode));
        }
        FsPermission permission = Utils.getPermission(mode);
        long createFileId = writeManager.create(parentFileId, name, permission);
        return Utils.getInode(createFileId);
    }

    @Override
    public FsStat getFsStat() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.getFsStat()");
        }
        String rootFileIdPath = Utils.getFileIdPath(rootFileId);
        DFSClient client = dfsClientCache.getSuperUserDFSClient();
        ContentSummary summary = client
                .getNamenode()
                .getContentSummary(rootFileIdPath);
        FsStatus fsStatus = client.getDiskStatus();
        return new FsStat(
                summary.getSpaceQuota() <= 0 ? fsStatus.getCapacity() : summary.getSpaceQuota(),
                summary.getQuota() <= 0 ? Long.MAX_VALUE : summary.getQuota(),
                summary.getSpaceConsumed() <= 0 ? fsStatus.getUsed() : summary.getSpaceConsumed(),
                summary.getFileAndDirectoryCount()
        );
    }

    @Override
    public Inode getRootInode() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.getRootInode()");
        }
        return Utils.getInode(rootFileId);
    }

    @Override
    public Inode lookup(Inode parent, String name) throws IOException {
        long parentFileId = Utils.getFileId(parent);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.lookup(parent={}, name={})", parentFileId, name);
        }
        String fileIdPath = Utils.getFileIdPath(parentFileId, name);
        HdfsFileStatus fileInfo = dfsClientCache.getSuperUserDFSClient().getFileInfo(fileIdPath);
        if (fileInfo == null) {
            throw new FileNotFoundException("file " + fileIdPath + " not found");
        }
        long fileId = fileInfo.getFileId();
        return Utils.getInode(fileId);
    }

    @Override
    public Inode link(Inode parent, Inode link, String name, Subject subject) {
        long parentFileId = Utils.getFileId(parent);
        long linkFileId = Utils.getFileId(link);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.link(parent={}, link={}, name={}, subject={})",
                    parentFileId, linkFileId, name, new SubjectHolder(subject));
        }
        throw new UnsupportedOperationException();
    }

    private byte[] toVerifier(long x) {
        return Longs.toByteArray(x);
    }

    @Override
    public DirectoryStream list(Inode inode, byte[] verifier, long cookie) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.list(inode={}, verifier={}, cookie={})", fileId, verifier, cookie);
        }
        String fileIdPath = Utils.getFileIdPath(fileId);
        TreeSet<DirectoryEntry> set = new TreeSet<>();
        long verifierLong = Long.MIN_VALUE;
        long currentCookie = 0;
        DFSClient dfsClient = dfsClientCache.getSuperUserDFSClient();
        DirectoryListing listing;
        byte[] startAfter = HdfsFileStatus.EMPTY_NAME;
        do {
            listing = dfsClient.listPaths(fileIdPath, startAfter);
            for (HdfsFileStatus status : listing.getPartialListing()) {
                verifierLong += status.getFileId() * 13;
                if (currentCookie >= cookie) {
                    set.add(buildDirectoryEntry(status, currentCookie));
                }
                currentCookie++;
            }
            startAfter = listing.getLastName();
        } while (listing.hasMore());
        return new DirectoryStream(toVerifier(verifierLong), set);
    }

    private DirectoryEntry buildDirectoryEntry(HdfsFileStatus status, long currentCookie) throws IOException {
        Inode inode = Utils.getInode(status.getFileId());
        return new DirectoryEntry(status.getLocalName(), inode, this.getStatHolder(status).getStatNotCreate(), currentCookie);
    }

    @VisibleForTesting
    public StatHolder getStatHolder(HdfsFileStatus status) throws IOException {
        Stat stat = new Stat();
        stat.setDev(0);
        stat.setIno(status.getFileId());
        stat.setMode(Utils.getMode(status));
        stat.setNlink(status.isDirectory() ? status.getChildrenNum() + 2 : 1);
        NfsIdMapping mapping = Utils.getNfsIdMapping();
        stat.setUid(mapping.principalToUid(status.getOwner()));
        stat.setGid(mapping.principalToGid(status.getGroup()));
        stat.setRdev(0);
        stat.setSize(status.isDirectory() ? (status.getChildrenNum() + 2) * 32L : status.getLen());
        stat.setGeneration(status.getModificationTime());
        stat.setATime(status.getAccessTime() == 0 ? status.getModificationTime() : status.getAccessTime());
        stat.setMTime(status.getModificationTime());
        stat.setCTime(status.getModificationTime());
        return new StatHolder(stat);
    }

    @Override
    public byte[] directoryVerifier(Inode inode) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.directoryVerifier(inode={})", fileId);
        }
        long verifierLong = Long.MIN_VALUE;
        long currentCookie = 0;
        String fileIdPath = Utils.getFileIdPath(fileId);
        DFSClient dfsClient = dfsClientCache.getSuperUserDFSClient();
        DirectoryListing listing;
        byte[] startAfter = HdfsFileStatus.EMPTY_NAME;
        do {
            listing = dfsClient.listPaths(fileIdPath, startAfter);
            for (HdfsFileStatus status : listing.getPartialListing()) {
                byte[] localNameInBytes = status.getLocalNameInBytes();
                verifierLong += Arrays.hashCode(localNameInBytes) + currentCookie * 1024;
                currentCookie++;
            }
            startAfter = listing.getLastName();
        } while (listing.hasMore());
        return toVerifier(verifierLong);
    }

    @Override
    public Inode mkdir(Inode parent, String name, Subject subject, int mode) throws IOException {
        long parentFileId = Utils.getFileId(parent);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.mkdir(parent={}, name={}, subject={}, mode={})",
                    parentFileId, name, new SubjectHolder(subject), Stat.modeToString(mode));
        }
        String newFileIdPath = Utils.getFileIdPath(parentFileId, name);
        FsPermission permission = Utils.getPermission(mode);
        DFSClient dfsClient = dfsClientCache.getDFSClient();
        dfsClient.mkdirs(newFileIdPath, permission, false);
        long fileId = dfsClient.getFileInfo(newFileIdPath).getFileId();
        return Utils.getInode(fileId);
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        long currentParentFileId = Utils.getFileId(src);
        long destFileId = Utils.getFileId(dest);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.move(src={}, oldName={}, desc={}, newName={})", currentParentFileId, oldName, destFileId, newName);
        }
        String currenFileIdPath = Utils.getFileIdPath(currentParentFileId, oldName);
        String newFileIdPath = Utils.getFileIdPath(destFileId, newName);
        if (currenFileIdPath.equals(newFileIdPath)) {
            return false;
        }
        dfsClientCache.getDFSClient().rename(
                currenFileIdPath,
                newFileIdPath,
                Options.Rename.OVERWRITE);
        return true;
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        return lookup(inode, "..");
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.read(inode={}, data=byte[], offset={}, count={})", fileId, offset, count);
        }
        DFSInputStream open = dfsClientCache.getDfsInputStream(fileId);
        int read = open.getPos() == offset ?
                open.read(data, 0, count) :
                open.read(offset, data, 0, count);
        if (read == -1) {
            LOG.debug("vfs.read(inode={}) reach end, close inputStream", fileId);
            dfsClientCache.invalidateInputStream(fileId);
        }
        return read;
    }

    @Override
    public int read(Inode inode, ByteBuffer data, long offset) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.read(inode={}, data=ByteBuffer, offset={})", fileId, offset);
        }
        DFSInputStream open = dfsClientCache.getDfsInputStream(fileId);
        int read = open.getPos() == offset ?
                open.read(data) :
                open.read(offset, data);
        if (read == -1) {
            LOG.debug("vfs.read(inode={}) reach end, close inputStream", fileId);
            dfsClientCache.invalidateInputStream(fileId);
            return 0;
        }
        return read;
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.write(inode={}, data=byte[], offset={}, count={}, stabilityLevel={})", fileId, offset, count, stabilityLevel);
        }
        writeManager.handleWrite(fileId, data, offset, count);
        return new WriteResult(StabilityLevel.UNSTABLE, count);
    }

    @Override
    public WriteResult write(Inode inode, ByteBuffer data, long offset, StabilityLevel stabilityLevel) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.write(inode={}, data={}, offset={}, stabilityLevel={})", fileId, data, offset, stabilityLevel);
        }
        int count = data.remaining();
        writeManager.handleWrite(fileId, data, offset);
        return new WriteResult(StabilityLevel.UNSTABLE, count);
    }

    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.commit(inode={}, offset={}, count={})", fileId, offset, count);
        }
        writeManager.handleCommit(fileId, offset, count);
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.readlink(inode={})", fileId);
        }
        try {
            return dfsClientCache.getSuperUserDFSClient().getLinkTarget(Utils.getFileIdPath(fileId));
        } catch (RemoteException e) {
            if (e.getMessage().contains("not supported")) {
                throw new UnsupportedOperationException(e);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void remove(Inode parent, String name) throws IOException {
        long parentFileId = Utils.getFileId(parent);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.remove(parent={}, name={})", parentFileId, name);
        }
        String targetIdPath = Utils.getFileIdPath(parentFileId, name);
        HdfsFileStatus fileInfo = dfsClientCache.getSuperUserDFSClient().getFileInfo(targetIdPath);
        if (fileInfo != null) {
            writeManager.handleCommit(fileInfo.getFileId(), 0, 0);
            dfsClientCache.getSuperUserDFSClient().delete(targetIdPath, false);
        }
    }

    @Override
    public Inode symlink(Inode parent, String name, String link, Subject subject, int mode) throws IOException {
        long parentFileId = Utils.getFileId(parent);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.symlink(parent={}, name={}, link={}, subject={}, mode={})",
                    parentFileId, name, link, new SubjectHolder(subject), Stat.modeToString(mode));
        }
        String linkIdPath = Utils.getFileIdPath(parentFileId, name);
        DFSClient dfsClient = dfsClientCache.getSuperUserDFSClient();
        try {
            dfsClient.createSymlink(link, linkIdPath, false);
        } catch (RemoteException e) {
            if (e.getMessage().contains("not supported")) {
                throw new UnsupportedOperationException(e);
            } else {
                throw e;
            }
        }
        long linkFileId = dfsClient.getFileLinkInfo(linkIdPath).getFileId();
        return Utils.getInode(linkFileId);
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.getattr(inode={})", fileId);
        }
        String fileIdPath = Utils.getFileIdPath(fileId);
        return getStatHolder(dfsClientCache.getSuperUserDFSClient().getFileInfo(fileIdPath)).getStatNotCreate();
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.setattr(inode={}, stat={})", fileId, formatStat(stat));
        }
        applyStatToPath(stat, fileId);
    }

    private Object formatStat(Stat stat) {
        return new Object() {

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                formatTo(builder);
                return builder.toString();
            }

            private boolean appendComa(boolean nonFirst, StringBuilder builder) {
                if (nonFirst) {
                    builder.append(", ");
                }
                return true;
            }

            public void formatTo(StringBuilder builder) {
                builder.append('{');
                boolean nonFirst = false;
                if (stat.isDefined(Stat.StatAttribute.MODE)) {
                    nonFirst = true;
                    builder.append("mode=").append(Stat.modeToString(stat.getMode()));
                }
                if (stat.isDefined(Stat.StatAttribute.NLINK)) {
                    nonFirst = appendComa(nonFirst, builder);
                    builder.append("nlink=").append(stat.getNlink());
                }
                if (stat.isDefined(Stat.StatAttribute.OWNER)) {
                    nonFirst = appendComa(nonFirst, builder);
                    builder.append("uid=").append(stat.getUid());
                }
                if (stat.isDefined(Stat.StatAttribute.GROUP)) {
                    nonFirst = appendComa(nonFirst, builder);
                    builder.append("gid=").append(stat.getGid());
                }
                if (stat.isDefined(Stat.StatAttribute.SIZE)) {
                    nonFirst = appendComa(nonFirst, builder);
                    builder.append("len=").append(Stat.sizeToString(stat.getSize()));
                }
                if (stat.isDefined(Stat.StatAttribute.MTIME)) {
                    appendComa(nonFirst, builder);
                    builder.append("mtime=").append(stat.getMTime());
                }
                if (stat.isDefined(Stat.StatAttribute.ATIME)) {
                    appendComa(nonFirst, builder);
                    builder.append("atime=").append(stat.getATime());
                }
                builder.append('}');
            }
        };
    }

    private void applyStatToPath(Stat stat, long fileId) throws IOException {
        String fileIdPath = Utils.getFileIdPath(fileId);
        DFSClient dfsClient = dfsClientCache.getSuperUserDFSClient();
        HdfsFileStatus fileStatus = dfsClient.getFileInfo(fileIdPath);
        long size;
        if (stat.isDefined(Stat.StatAttribute.SIZE) && (size = stat.getSize()) != fileStatus.getLen()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("vfs.applyStatToPath(inode={}, size={})", fileId, size);
            }
            writeManager.handleCommit(fileId, 0, 0);
            dfsClient.truncate(fileIdPath, size);
        }
        int mode = Utils.getMode(fileStatus);
        if (stat.isDefined(Stat.StatAttribute.MODE) && stat.getMode() != mode) {
            FsPermission permission = Utils.getPermission(stat.getMode());
            if (LOG.isDebugEnabled()) {
                LOG.debug("vfs.applyStatToPath(inode={}, permission={})", fileId, permission);
            }
            dfsClient.setPermission(fileIdPath, permission);
        }
        long modificationTime = fileStatus.getModificationTime();
        long accessTime = fileStatus.getAccessTime();
        boolean update = false;
        if (stat.isDefined(Stat.StatAttribute.MTIME) && modificationTime != stat.getMTime()) {
            modificationTime = stat.getMTime();
            update = true;
        }
        if (stat.isDefined(Stat.StatAttribute.ATIME) && accessTime != stat.getATime()) {
            accessTime = stat.getATime();
            update = true;
        }
        if (update) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("vfs.applyStatToPath(inode={}, mtime={}, atime={})", fileId, modificationTime, accessTime);
            }
            dfsClient.setTimes(fileIdPath, modificationTime, accessTime);
        }
        update = false;
        NfsIdMapping nfsIdMapping = Utils.getNfsIdMapping();
        int uid = nfsIdMapping.principalToUid(fileStatus.getOwner());
        int gid = nfsIdMapping.principalToGid(fileStatus.getGroup());
        if (stat.isDefined(Stat.StatAttribute.OWNER) && uid != stat.getUid()) {
            uid = stat.getUid();
            update = true;
        }
        if (stat.isDefined(Stat.StatAttribute.GROUP) && gid != stat.getGid()) {
            gid = stat.getGid();
            update = true;
        }
        if (update) {
            String user = nfsIdMapping.uidToPrincipal(uid);
            String group = nfsIdMapping.gidToPrincipal(gid);
            if (LOG.isDebugEnabled()) {
                LOG.debug("vfs.applyStatToPath(inode={}, mtime={}, atime={})", fileId, user, group);
            }
            dfsClient.setOwner(fileIdPath, user, group);
        }
    }

    @Override
    public nfsace4[] getAcl(Inode inode) {
        return new nfsace4[0];
    }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) {

    }

    @Override
    public boolean hasIOLayout(Inode inode) {
        return false;
    }

    @Override
    public AclCheckable getAclCheckable() {
        return AclCheckable.UNDEFINED_ALL;
    }

    @Override
    public NfsIdMapping getIdMapper() {
        return Utils.SIMPLE_ID_MAPPING;
    }

    @Override
    public boolean getCaseInsensitive() {
        return false;
    }

    @Override
    public boolean getCasePreserving() {
        return true;
    }

    @Override
    public byte[] getXattr(Inode inode, String attr) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.getXattr(inode={}, attr={})", fileId, attr);
        }
        String fileIdPath = Utils.getFileIdPath(fileId);
        return dfsClientCache.getSuperUserDFSClient().getXAttr(fileIdPath, attr);
    }

    @Override
    public void setXattr(Inode inode, String attr, byte[] value, SetXattrMode mode) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.setXattr(inode={}, attr={}, value={}, mode={})", fileId, attr, value, mode);
        }
        EnumSet<XAttrSetFlag> flags = switch (mode) {
            case CREATE -> EnumSet.of(XAttrSetFlag.CREATE);
            case REPLACE -> EnumSet.of(XAttrSetFlag.REPLACE);
            case EITHER -> EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE);
        };
        String fileIdPath = Utils.getFileIdPath(fileId);
        dfsClientCache.getSuperUserDFSClient().setXAttr(fileIdPath, attr, value, flags);
    }

    @Override
    public String[] listXattrs(Inode inode) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.listXattrs(inode={})", fileId);
        }
        String fileIdPath = Utils.getFileIdPath(fileId);
        return dfsClientCache.getSuperUserDFSClient().listXAttrs(fileIdPath).toArray(new String[0]);
    }

    @Override
    public void removeXattr(Inode inode, String attr) throws IOException {
        long fileId = Utils.getFileId(inode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("vfs.removeXattr(inode={}, attr={})", fileId, attr);
        }
        String fileIdPath = Utils.getFileIdPath(fileId);
        dfsClientCache.getSuperUserDFSClient().removeXAttr(fileIdPath, attr);
    }

    @Override
    public Long lookup(CacheKey key) throws IOException {
        long parentFileId = key.getParent();
        String name = key.getName();
        if (LOG.isDebugEnabled()) {
            LOG.debug("clh.lookup(parent={}, name={})", parentFileId, name);
        }
        String fileIdPath = Utils.getFileIdPath(parentFileId, name);
        HdfsFileStatus fileInfo = dfsClientCache.getSuperUserDFSClient().getFileInfo(fileIdPath);
        return fileInfo == null ? null : fileInfo.getFileId();
    }

    @Override
    public StatHolder getStat(Long fileId) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("clh.getStat(inode={})", fileId);
        }
        String fileIdPath = Utils.getFileIdPath(fileId);
        final HdfsFileStatus fileInfo = dfsClientCache.getSuperUserDFSClient().getFileInfo(fileIdPath);
        return fileInfo == null ? null : getStatHolder(fileInfo);
    }
}
