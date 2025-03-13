package io.masterkun.nfsonhdfs.vfs;

import io.masterkun.nfsonhdfs.cache.CacheKey;
import io.masterkun.nfsonhdfs.cache.CacheLoaderHelper;
import io.masterkun.nfsonhdfs.cache.StatHolder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.PathAccessDeniedException;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.PathOperationException;
import org.apache.hadoop.fs.PathPermissionException;
import org.apache.hadoop.security.AccessControlException;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.status.DeniedException;
import org.dcache.nfs.status.ExistException;
import org.dcache.nfs.status.ExpiredException;
import org.dcache.nfs.status.InvalException;
import org.dcache.nfs.status.IsDirException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.status.NotDirException;
import org.dcache.nfs.status.NotSuppException;
import org.dcache.nfs.status.PermException;
import org.dcache.nfs.status.ServerFaultException;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.AclCheckable;
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
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public class WrappedVirtualFileSystem implements VirtualFileSystem, CacheLoaderHelper {
    private static final Logger LOG = LoggerFactory.getLogger(WrappedVirtualFileSystem.class);
    private final HadoopVirtualFileSystem inner;

    public WrappedVirtualFileSystem(HadoopVirtualFileSystem inner) {
        this.inner = inner;
    }

    @Override
    public int access(Subject subject, Inode inode, int mode) throws IOException {
        try {
            return inner.access(subject, inode, mode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public Inode create(Inode parent, Stat.Type type, String name, Subject subject, int mode) throws IOException {
        try {
            return inner.create(parent, type, name, subject, mode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public FsStat getFsStat() throws IOException {
        try {
            return inner.getFsStat();
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public Inode getRootInode() throws IOException {
        try {
            return inner.getRootInode();
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public Inode lookup(Inode parent, String name) throws IOException {
        try {
            return inner.lookup(parent, name);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public Inode link(Inode parent, Inode link, String name, Subject subject) throws IOException {
        try {
            return inner.link(parent, link, name, subject);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public DirectoryStream list(Inode inode, byte[] verifier, long cookie) throws IOException {
        try {
            return inner.list(inode, verifier, cookie);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public byte[] directoryVerifier(Inode inode) throws IOException {
        try {
            return inner.directoryVerifier(inode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public Inode mkdir(Inode parent, String name, Subject subject, int mode) throws IOException {
        try {
            return inner.mkdir(parent, name, subject, mode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        try {
            return inner.move(src, oldName, dest, newName);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        try {
            return inner.parentOf(inode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @SuppressWarnings("all")
    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        try {
            return inner.read(inode, data, offset, count);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public int read(Inode inode, ByteBuffer data, long offset) throws IOException {
        try {
            return inner.read(inode, data, offset);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @SuppressWarnings("all")
    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
        try {
            return inner.write(inode, data, offset, count, stabilityLevel);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public WriteResult write(Inode inode, ByteBuffer data, long offset, StabilityLevel stabilityLevel) throws IOException {
        try {
            return inner.write(inode, data, offset, stabilityLevel);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        try {
            inner.commit(inode, offset, count);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        try {
            return inner.readlink(inode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void remove(Inode parent, String name) throws IOException {
        try {
            inner.remove(parent, name);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public Inode symlink(Inode parent, String linkName, String targetName, Subject subject, int mode) throws IOException {
        try {
            return inner.symlink(parent, linkName, targetName, subject, mode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        try {
            return inner.getattr(inode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        try {
            inner.setattr(inode, stat);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public nfsace4[] getAcl(Inode inode) throws IOException {
        try {
            return inner.getAcl(inode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
        try {
            inner.setAcl(inode, acl);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public boolean hasIOLayout(Inode inode) throws IOException {
        try {
            return inner.hasIOLayout(inode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public AclCheckable getAclCheckable() {
        return inner.getAclCheckable();
    }

    @Override
    public NfsIdMapping getIdMapper() {
        return inner.getIdMapper();
    }

    @Override
    public boolean getCaseInsensitive() {
        return inner.getCaseInsensitive();
    }

    @Override
    public boolean getCasePreserving() {
        return inner.getCasePreserving();
    }

    @Override
    public byte[] getXattr(Inode inode, String attr) throws IOException {
        try {
            return inner.getXattr(inode, attr);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void setXattr(Inode inode, String attr, byte[] value, SetXattrMode mode) throws IOException {
        try {
            inner.setXattr(inode, attr, value, mode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String[] listXattrs(Inode inode) throws IOException {
        try {
            return inner.listXattrs(inode);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void removeXattr(Inode inode, String attr) throws IOException {
        try {
            inner.removeXattr(inode, attr);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @SuppressWarnings("all")
    @Override
    public CompletableFuture<Long> copyFileRange(Inode src, long srcPos, Inode dst, long dstPos, long len) {
        return inner.copyFileRange(src, srcPos, dst, dstPos, len);
    }

    private IOException convert(Throwable e) {
        LOG.error("vfs error", e);
        if (e instanceof ChimeraNFSException) {
            return (IOException) e;
        }
        if (e instanceof AccessControlException || e instanceof PathAccessDeniedException || e instanceof AccessDeniedException) {
            return new DeniedException(e.getMessage(), e);
        }
        if (e instanceof FileNotFoundException || e instanceof PathNotFoundException || e instanceof NoSuchFileException) {
            return new NoEntException(e.getMessage(), e);
        }
        if (e instanceof SecurityException || e instanceof PathPermissionException) {
            return new PermException(e.getMessage(), e);
        }
        if (e instanceof UnsupportedOperationException || e instanceof PathOperationException) {
            return new NotSuppException(e.getMessage(), e);
        }
        if (e instanceof ParentNotDirectoryException || e instanceof PathIsNotDirectoryException || e instanceof NotDirectoryException) {
            return new NotDirException(e.getMessage(), e);
        }
        if (e instanceof PathIsDirectoryException) {
            return new IsDirException(e.getMessage(), e);
        }
        if (e instanceof FileAlreadyExistsException || e instanceof PathExistsException || e instanceof java.nio.file.FileAlreadyExistsException) {
            return new ExistException(e.getMessage(), e);
        }
        if (e instanceof TimeoutException) {
            return new ExpiredException(e.getMessage(), e);
        }
        if (e instanceof IllegalArgumentException) {
            return new InvalException(e.getMessage(), e);
        }
        return new ServerFaultException(e.getMessage(), e);
    }

    @Override
    public Long lookup(CacheKey key) throws IOException {
        try {
            return inner.lookup(key);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public StatHolder getStat(Long key) throws IOException {
        try {
            return inner.getStat(key);
        } catch (Exception e) {
            throw convert(e);
        }
    }
}
