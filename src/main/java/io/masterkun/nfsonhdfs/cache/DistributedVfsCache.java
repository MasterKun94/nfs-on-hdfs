package io.masterkun.nfsonhdfs.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.hazelcast.cache.ICache;
import com.hazelcast.core.HazelcastInstance;
import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.Utils;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.ForwardingFileSystem;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.CacheException;
import javax.security.auth.Subject;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Caching decorator.
 */
public class DistributedVfsCache extends ForwardingFileSystem implements CacheLoaderHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedVfsCache.class);
    private final ICache<CacheKey, Long> lookupCache;
    private final ICache<Long, StatHolder> statCache;
    private final VirtualFileSystem inner;
    private final long fsStatExpireMs;
    private volatile FsStat cachedFsStat;
    private volatile long fsStatRefreshTime;

    public DistributedVfsCache(VirtualFileSystem inner, HazelcastInstance hazelcastInstance,
                               String suffix) {
        this.inner = inner;
        final AppConfig.VfsCacheConfig cacheConfig = Utils.getServerConfig().getVfs().getVfsCache();
        this.fsStatExpireMs = cacheConfig.getFsStatExpireMs();
        this.lookupCache = hazelcastInstance.getCacheManager().getCache("lookupCache-" + suffix);
        this.statCache = hazelcastInstance.getCacheManager().getCache("statCache-" + suffix);
    }

    @Override
    protected VirtualFileSystem delegate() {
        return inner;
    }

    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        try {
            inner.commit(inode, offset, count);
        } finally {
            wait(invalidateStatCache(inode));
        }
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
        Inode inode = null;
        try {
            return inode = inner.symlink(parent, path, link, subject, mode);
        } finally {
            if (inode == null) {
                wait(invalidateStatCache(parent));
            } else {
                wait(invalidateStatCache(parent), invalidateStatCache(inode));
            }
        }
    }

    @Override
    public void remove(Inode parent, String path) throws IOException {
        Inode inode = lookup(parent, path);
        try {
            inner.remove(parent, path);
        } finally {
            wait(invalidateLookupCache(parent, path),
                    invalidateParentCache(inode),
                    invalidateStatCache(parent),
                    invalidateStatCache(inode));
        }
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        return lookup(inode, "..");
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        Inode inode = lookup(src, oldName);
        try {
            return inner.move(src, oldName, dest, newName);
        } finally {
            wait(invalidateParentCache(inode),
                    invalidateLookupCache(src, oldName),
                    invalidateLookupCache(dest, newName),
                    invalidateStatCache(src),
                    invalidateStatCache(dest));
        }
    }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        try {
            return inner.mkdir(parent, path, subject, mode);
        } finally {
            wait(invalidateStatCache(parent));
        }
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        Inode inode = null;
        try {
            return inode = inner.link(parent, link, path, subject);
        } finally {
            if (inode == null) {
                wait(invalidateStatCache(parent));
            } else {
                wait(invalidateStatCache(parent), invalidateStatCache(inode));
            }
        }
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        return inner.readlink(inode);
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {
        return lookupFromCacheOrLoad(parent, path);
    }

    @Override
    public FsStat getFsStat() throws IOException {
        if (System.currentTimeMillis() - fsStatRefreshTime > fsStatExpireMs) {
            synchronized (this) {
                long current = System.currentTimeMillis();
                if (current - fsStatRefreshTime > fsStatExpireMs) {
                    cachedFsStat = inner.getFsStat();
                    fsStatRefreshTime = current;
                    return cachedFsStat;
                }
            }
        }
        return cachedFsStat;
    }

    @Override
    public Inode create(Inode parent, Stat.Type type, String path, Subject subject, int mode) throws IOException {
        try {
            return inner.create(parent, type, path, subject, mode);
        } finally {
            wait(invalidateStatCache(parent));
        }
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        return statFromCacheOrLoad(inode);
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        try {
            inner.setattr(inode, stat);
        } finally {
            wait(invalidateStatCache(inode));
        }
    }

    @Override
    public boolean getCaseInsensitive() {
        return inner.getCaseInsensitive();
    }

    @Override
    public boolean getCasePreserving() {
        return inner.getCasePreserving();
    }

    /*
       Utility methods for cache manipulation.
     */

    /**
     * Discards cached value in lookup cache for given inode and path.
     *
     * @param parent inode
     * @param path   to invalidate
     */
    public CompletableFuture<Boolean> invalidateLookupCache(Inode parent, String path) {
        long fileId = Utils.getFileId(parent);
        CacheKey cacheKey = new CacheKey(fileId, path);
        return (CompletableFuture<Boolean>) lookupCache.removeAsync(cacheKey);
    }

    public CompletableFuture<Boolean> invalidateParentCache(Inode child) {
        long childFileId = Utils.getFileId(child);
        return (CompletableFuture<Boolean>) lookupCache.removeAsync(new CacheKey(childFileId, "." +
                "."));
    }

    /**
     * Discards cached {@link Stat} value for given {@link Inode}.
     *
     * @param inode The inode for which cached state value should be invalidated.
     */
    public CompletableFuture<Boolean> invalidateStatCache(final Inode inode) {
        return (CompletableFuture<Boolean>) statCache.removeAsync(Utils.getFileId(inode));
    }

    private void wait(CompletableFuture<?>... futures) {
        try {
            CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Async error", e);
        }
    }

    private Inode lookupFromCacheOrLoad(final Inode parent, final String path) throws IOException {
        try {
            long parentFileId = Utils.getFileId(parent);
            Long childFileId = lookupCache.get(new CacheKey(parentFileId, path));
            if (childFileId == null) {
                throw new NoEntException();
            }
            return Utils.getInode(childFileId);
        } catch (CacheException e) {
            Throwable t = e.getCause();
            Throwables.throwIfInstanceOf(t, IOException.class);
            throw new IOException(e.getMessage(), t);
        }
    }

    private Stat statFromCacheOrLoad(final Inode inode) throws IOException {
        try {
            StatHolder statHolder = statCache.get(Utils.getFileId(inode));
            if (statHolder == null) {
                throw new NoEntException();
            }
            return statHolder.newStat();
        } catch (CacheException e) {
            Throwable t = e.getCause();
            Throwables.throwIfInstanceOf(t, IOException.class);
            throw new IOException(e.getMessage(), t);
        }
    }

    @Override
    public DirectoryStream list(Inode inode, byte[] verifier, long cookie) throws IOException {
        return delegate().list(inode, verifier, cookie);
    }

    @Override
    public void removeXattr(Inode inode, String attr) throws IOException {
        try {
            inner.removeXattr(inode, attr);
        } finally {
            wait(invalidateStatCache(inode));
        }
    }

    @Override
    public void setXattr(Inode inode, String attr, byte[] value, SetXattrMode mode) throws IOException {
        try {
            inner.setXattr(inode, attr, value, mode);
        } finally {
            wait(invalidateStatCache(inode));
        }
    }

    @VisibleForTesting
    public void clearCache() {
        statCache.clear();
        lookupCache.clear();
    }

    @Override
    public Long lookup(CacheKey key) throws IOException {
        return ((CacheLoaderHelper) inner).lookup(key);
    }

    @Override
    public StatHolder getStat(Long key) throws IOException {
        return ((CacheLoaderHelper) inner).getStat(key);
    }
}
