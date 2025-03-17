package io.masterkun.nfsonhdfs.vfs;

import com.google.common.primitives.Longs;
import com.sun.security.auth.UnixNumericUserPrincipal;
import io.masterkun.nfsonhdfs.CallContext;
import io.masterkun.nfsonhdfs.cache.CacheLoaderHelper;
import io.masterkun.nfsonhdfs.cache.StatHolder;
import io.masterkun.nfsonhdfs.idmapping.AbstractIdMapping;
import io.masterkun.nfsonhdfs.idmapping.HostedSftpIdMapping;
import io.masterkun.nfsonhdfs.idmapping.HostedSftpIdMappingFactory;
import io.masterkun.nfsonhdfs.util.Constants;
import io.masterkun.nfsonhdfs.util.Utils;
import lombok.SneakyThrows;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.apache.commons.lang3.StringUtils;
import org.dcache.nfs.ExportTable;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.status.PermException;
import org.dcache.nfs.status.ServerFaultException;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.SimpleIdMap;
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
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

public class HostedVirtualFileSystem implements VirtualFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(HostedVirtualFileSystem.class);
    private final VirtualFileSystem inner;
    private final CacheLoaderHelper helper;
    private final Inode rootInode;
    private final Inode realRootInode;
    private final Map<String, Inode> hostedInodes;
    private final Int2ObjectHashMap<Inode> exportIdxInodes;
    private final Int2ObjectHashMap<String> exportIdxHosts;
    private final LongHashSet hostedFileIds;
    private final HostedSftpIdMappingFactory idMappingFactory;
    private final NfsIdMapping simpleMapping = new SimpleIdMap();

    public HostedVirtualFileSystem(VirtualFileSystem inner, ExportTable exportTable) throws IOException {
        this.inner = inner;
        this.helper = (CacheLoaderHelper) inner;
        this.realRootInode = inner.getRootInode();
        this.rootInode = Inode.forFile(Longs.toByteArray(1000));
        Map<String, Inode> hostedInodes = new LinkedHashMap<>();
        Int2ObjectHashMap<Inode> exportIdxInodes = new Int2ObjectHashMap<>();
        Int2ObjectHashMap<String> exportIdxHosts = new Int2ObjectHashMap<>();
        LongHashSet hostedFileIds = new LongHashSet();
        AtomicLong adder = new AtomicLong(1000);
        exportTable.exports().forEachOrdered(fsExport -> {
            String host = fsExport.getPath().substring(1).split("/")[0];
            if (StringUtils.isEmpty(host)) {
                return;
            }
            try {
                InetAddress.getByName(host);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            byte[] fileId = Longs.toByteArray(adder.incrementAndGet());
            Inode inode = Inode.forFile(fileId);
            hostedInodes.put(host, inode);
            exportIdxInodes.put(fsExport.getIndex(), inode);
            exportIdxHosts.put(fsExport.getIndex(), host);
            hostedFileIds.add(adder.get());
        });
        this.hostedInodes = Collections.unmodifiableMap(hostedInodes);
        this.exportIdxInodes = exportIdxInodes;
        this.exportIdxHosts = exportIdxHosts;
        this.hostedFileIds = hostedFileIds;
        this.idMappingFactory =
                new HostedSftpIdMappingFactory((AbstractIdMapping) Utils.getNfsIdMapping());
    }

    private boolean isRootInode(Inode inode) {
        return Arrays.equals(inode.getFileId(), rootInode.getFileId());
    }

    private boolean isHosted(Inode inode) {
        return hostedFileIds.contains(Longs.fromByteArray(inode.getFileId()));
    }

    private void resolvePrincipal() throws IOException {
        CompoundContext context = CallContext.getCompoundContext();
        String host = exportIdxHosts.get(context.currentInode().exportIndex());
        if (host != null) {
            HostedSftpIdMapping mapping = idMappingFactory.get(host);
            for (Principal principal : context.getSubject().getPrincipals()) {
                if (principal instanceof UnixNumericUserPrincipal p) {
                    String realPrincipal = mapping.uidToRealPrincipal((int) p.longValue());
                    CallContext.setRealPrincipal(realPrincipal);
                }
            }
        }
    }

    private void cleanupPrincipal() {
        CallContext.clearRealPrincipal();
    }

    @Override
    public int access(Subject subject, Inode inode, int mode) {
        return mode;
    }

    @Override
    public Inode create(Inode parent, Stat.Type type, String name, Subject subject, int mode) throws IOException {
        if (isRootInode(parent)) {
            throw new PermException("parent is root");
        }
        if (isHosted(parent)) {
            parent = realRootInode;
        }
        try {
            resolvePrincipal();
            return inner.create(parent, type, name, subject, mode);
        } finally {
            cleanupPrincipal();
        }
    }

    @Override
    public FsStat getFsStat() throws IOException {
        return inner.getFsStat();
    }

    @Override
    public Inode getRootInode() {
        return rootInode;
    }

    @Override
    public Inode lookup(Inode parent, String name) throws IOException {
        if (isRootInode(parent)) {
            Inode inode = hostedInodes.get(name);
            if (inode == null) {
                throw new NoEntException("no " + name + " hosted found in root");
            }
            return inode;
        }
        if (isHosted(parent)) {
            parent = realRootInode;
        }
        return inner.lookup(parent, name);
    }

    @Override
    public Inode link(Inode parent, Inode link, String name, Subject subject) throws IOException {
        if (isRootInode(parent)) {
            throw new PermException("parent is root");
        }
        if (isRootInode(link)) {
            throw new PermException("link is root");
        }
        if (isHosted(parent)) {
            parent = realRootInode;
        }
        if (isHosted(link)) {
            link = realRootInode;
        }
        try {
            resolvePrincipal();
            return inner.link(parent, link, name, subject);
        } finally {
            cleanupPrincipal();
        }
    }

    @Override
    public DirectoryStream list(Inode inode, byte[] verifier, long cookie) throws IOException {
        DirectoryStream directoryStream;
        if (isRootInode(inode)) {
            StatHolder statHolder = helper.getStat(Longs.fromByteArray(realRootInode.getFileId()));
            TreeSet<DirectoryEntry> set = new TreeSet<>();
            long verifierLong = Long.MIN_VALUE;
            long currentCookie = 0;
            for (Map.Entry<String, Inode> entry : hostedInodes.entrySet()) {
                long fileId = Longs.fromByteArray(entry.getValue().getFileId());
                verifierLong += fileId * 13;
                if (currentCookie >= cookie) {
                    Stat stat = statHolder.newStat();
                    stat.setIno(fileId);
                    set.add(new DirectoryEntry(entry.getKey(), entry.getValue(), stat,
                            currentCookie));
                }
                currentCookie++;
            }
            directoryStream = new DirectoryStream(Longs.toByteArray(verifierLong), set);
        } else {
            if (isHosted(inode)) {
                inode = realRootInode;
            }
            directoryStream = inner.list(inode, verifier, cookie);
        }
        String host = exportIdxHosts.get(CallContext.getExportIdx());
        if (host != null) {
            NfsIdMapping mapping = idMappingFactory.get(host);
            return directoryStream.transform(entry -> {
                Stat stat = entry.getStat();
                String user = mapping.uidToPrincipal(stat.getUid());
                if (user.equals(Constants.NFS_NOBODY)) {
                    stat.setUid(stat.getUid());
                } else {
                    stat.setUid(Integer.parseInt(user));
                }
                String group = mapping.gidToPrincipal(stat.getGid());
                if (group.equals(Constants.NFS_NOBODY)) {
                    stat.setGid(stat.getGid());
                } else {
                    stat.setGid(Integer.parseInt(group));
                }
                return entry;
            });
        } else {
            return directoryStream;
        }
    }

    @Override
    public byte[] directoryVerifier(Inode inode) throws IOException {
        if (isRootInode(inode)) {
            long verifierLong = Long.MIN_VALUE;
            for (Map.Entry<String, Inode> entry : hostedInodes.entrySet()) {
                long fileId = Longs.fromByteArray(entry.getValue().getFileId());
                verifierLong += fileId * 13;
            }
            return Longs.toByteArray(verifierLong);
        }
        if (isHosted(inode)) {
            inode = realRootInode;
        }
        return inner.directoryVerifier(inode);
    }

    @Override
    public Inode mkdir(Inode parent, String name, Subject subject, int mode) throws IOException {
        if (isRootInode(parent)) {
            throw new PermException("parent is root");
        }
        if (isHosted(parent)) {
            parent = realRootInode;
        }
        try {
            resolvePrincipal();
            return inner.mkdir(parent, name, subject, mode);
        } finally {
            cleanupPrincipal();
        }
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        if (isRootInode(src)) {
            throw new PermException("src is root");
        }
        if (isRootInode(dest)) {
            throw new PermException("dest is root");
        }
        if (isHosted(src)) {
            src = realRootInode;
        }
        if (isHosted(dest)) {
            dest = realRootInode;
        }
        try {
            resolvePrincipal();
            return inner.move(src, oldName, dest, newName);
        } finally {
            cleanupPrincipal();
        }
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        if (isRootInode(inode)) {
            throw new PermException("inode is root");
        }
        if (isHosted(inode)) {
            return rootInode;
        }
        Inode parent = inner.parentOf(inode);
        if (Arrays.equals(parent.getFileId(), realRootInode.getFileId())) {
            Inode realParent = exportIdxInodes.get(inode.exportIndex());
            if (realParent == null) {
                throw new ServerFaultException("parent not found for exportIdx " + inode.exportIndex());
            }
            return realParent;
        }
        return parent;
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        return inner.read(inode, data, offset, count);
    }

    @Override
    public int read(Inode inode, ByteBuffer data, long offset) throws IOException {
        return inner.read(inode, data, offset);
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        return inner.readlink(inode);
    }

    @Override
    public void remove(Inode parent, String name) throws IOException {
        if (isRootInode(parent)) {
            throw new PermException("parent is root");
        }
        if (isHosted(parent)) {
            parent = realRootInode;
        }
        try {
            resolvePrincipal();
            inner.remove(parent, name);
        } finally {
            cleanupPrincipal();
        }
    }

    @Override
    public Inode symlink(Inode parent, String name, String link, Subject subject, int mode) throws IOException {
        if (isRootInode(parent)) {
            throw new PermException("parent is root");
        }
        if (isHosted(parent)) {
            parent = realRootInode;
        }
        return inner.symlink(parent, name, link, subject, mode);
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count,
                             StabilityLevel stabilityLevel) throws IOException {
        try {
            resolvePrincipal();
            return inner.write(inode, data, offset, count, stabilityLevel);
        } finally {
            cleanupPrincipal();
        }
    }

    @Override
    public WriteResult write(Inode inode, ByteBuffer data, long offset,
                             StabilityLevel stabilityLevel) throws IOException {
        try {
            resolvePrincipal();
            return inner.write(inode, data, offset, stabilityLevel);
        } finally {
            cleanupPrincipal();
        }
    }

    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        try {
            resolvePrincipal();
            inner.commit(inode, offset, count);
        } finally {
            cleanupPrincipal();
        }
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        Stat stat;
        if (isRootInode(inode)) {
            stat = inner.getattr(realRootInode);
            stat.setIno(Longs.fromByteArray(inode.getFileId()));
            int nlink = hostedInodes.size() + 2;
            long size = nlink * 32L;
            stat.setNlink(nlink);
            stat.setSize(size);
        } else if (isHosted(inode)) {
            stat = inner.getattr(realRootInode);
            stat.setIno(Longs.fromByteArray(inode.getFileId()));
        } else {
            stat = inner.getattr(inode);
        }
        String host = exportIdxHosts.get(CallContext.getExportIdx());
        if (host != null) {
            NfsIdMapping mapping = idMappingFactory.get(host);
            String user = mapping.uidToPrincipal(stat.getUid());
            if (user.equals(Constants.NFS_NOBODY)) {
                stat.setUid(stat.getUid());
            } else {
                stat.setUid(Integer.parseInt(user));
            }
            String group = mapping.gidToPrincipal(stat.getGid());
            if (group.equals(Constants.NFS_NOBODY)) {
                stat.setGid(stat.getGid());
            } else {
                stat.setGid(Integer.parseInt(group));
            }
        }
        return stat;
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        if (isRootInode(inode)) {
            throw new PermException("inode is root");
        }
        if (isHosted(inode)) {
            inode = realRootInode;
        }
        boolean defineOwner = stat.isDefined(Stat.StatAttribute.OWNER);
        boolean defineGroup = stat.isDefined(Stat.StatAttribute.GROUP);
        if (defineOwner || defineGroup) {
            String host = exportIdxHosts.get(CallContext.getExportIdx());
            if (host != null) {
                NfsIdMapping mapping = idMappingFactory.get(host);
                if (defineOwner) {
                    int uid = mapping.principalToUid(Integer.toString(stat.getUid()));
                    stat.setUid(uid);
                }
                if (defineGroup) {
                    int gid = mapping.principalToGid(Integer.toString(stat.getGid()));
                    stat.setGid(gid);
                }
            }
        }
        try {
            resolvePrincipal();
            inner.setattr(inode, stat);
        } finally {
            cleanupPrincipal();
        }
    }

    @Override
    public nfsace4[] getAcl(Inode inode) throws IOException {
        if (isRootInode(inode)) {
            throw new PermException("inode is root");
        }
        if (isHosted(inode)) {
            inode = realRootInode;
        }
        return inner.getAcl(inode);
    }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
        if (isRootInode(inode)) {
            throw new PermException("inode is root");
        }
        if (isHosted(inode)) {
            inode = realRootInode;
        }
        inner.setAcl(inode, acl);
    }

    @Override
    public boolean hasIOLayout(Inode inode) throws IOException {
        if (isRootInode(inode)) {
            throw new PermException("inode is root");
        }
        if (isHosted(inode)) {
            inode = realRootInode;
        }
        return inner.hasIOLayout(inode);
    }

    @Override
    public AclCheckable getAclCheckable() {
        return inner.getAclCheckable();
    }

    @SneakyThrows
    @Override
    public NfsIdMapping getIdMapper() {
        return simpleMapping;
    }

    @Override
    public boolean getCaseInsensitive() {
        return inner.getCaseInsensitive();
    }

    @Override
    public boolean getCasePreserving() {
        return inner.getCasePreserving();
    }

    public void refresh() throws Exception {
        idMappingFactory.refresh();
    }
}
