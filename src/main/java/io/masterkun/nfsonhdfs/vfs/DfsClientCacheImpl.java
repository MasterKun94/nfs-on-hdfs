package io.masterkun.nfsonhdfs.vfs;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.masterkun.nfsonhdfs.CallContext;
import io.masterkun.nfsonhdfs.util.Utils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CompletionException;

public class DfsClientCacheImpl implements DfsClientCache {
    private static final Logger LOG = LoggerFactory.getLogger(DfsClientCacheImpl.class);
    private final LoadingCache<String, DFSClient> dfsClientCache;
    private final LoadingCache<Long, DFSInputStream> inputStreamCache;

    public DfsClientCacheImpl() throws IOException {
        this(
                Utils.getServerConfig().getRootDir(),
                Utils.getServerConfig().getVfs().getDfsClientCacheSpec(),
                Utils.getServerConfig().getVfs().getInputStreamCacheSpec()
        );
    }

    public DfsClientCacheImpl(String rootPath, String dfsClientCacheSpec,
                              String inputStreamCacheSpec) throws IOException {
        URI resolvedURI = Utils.getResolvedURI(rootPath);
        this.dfsClientCache = Caffeine.from(dfsClientCacheSpec)
                .removalListener((RemovalListener<String, DFSClient>) (key, value, cause) -> {
                    if (value != null) {
                        try {
                            LOG.info("Close {}", value);
                            value.close();
                        } catch (IOException e) {
                            LOG.error("Close {} error", value);
                        }
                    }
                })
                .recordStats()
                .build(new CacheLoader<String, DFSClient>() {
                    @Override
                    public @Nullable DFSClient load(String principal) throws Exception {
                        UserGroupInformation proxyUser =
                                UserGroupInformation.createProxyUser(principal,
                                        UserGroupInformation.getCurrentUser());
                        return proxyUser.doAs((PrivilegedExceptionAction<DFSClient>) () -> new DFSClient(resolvedURI, Utils.getHadoopConf()));
                    }
                });
        Utils.getCacheMetricsCollector().addCache("dfs_client_cache", dfsClientCache);
        this.inputStreamCache = Caffeine.from(inputStreamCacheSpec)
                .removalListener((RemovalListener<Long, DFSInputStream>) (key, value, cause) -> {
                    if (value != null) {
                        try {
                            LOG.info("Close DfsInputStream({})", key);
                            value.close();
                        } catch (IOException e) {
                            LOG.error("Close DfsInputStream({}) error", key, e);
                        }
                    }
                })
                .recordStats()
                .build(new CacheLoader<Long, DFSInputStream>() {
                    @Override
                    public @Nullable DFSInputStream load(Long key) throws Exception {
                        return getSuperUserDFSClient().open(Utils.getFileIdPath(key));
                    }
                });
        Utils.getCacheMetricsCollector().addCache("input_stream_cache", dfsClientCache);
    }

    @Override
    public DFSClient getDFSClient(String principal) throws IOException {
        try {
            DFSClient dfsClient = dfsClientCache.get(principal);
            if (!dfsClient.isClientRunning()) {
                LOG.warn("Refresh {} because not running", dfsClient);
                dfsClientCache.refresh(principal);
            }
            return dfsClientCache.get(principal);
        } catch (CompletionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException(e);
            }
        }

    }

    @Override
    public DFSClient getDFSClient() throws IOException {
        String principal = CallContext.getPrincipal();
        return getDFSClient(principal);
    }

    @Override
    public DFSClient getSuperUserDFSClient() throws IOException {
        return getDFSClient(UserGroupInformation.getCurrentUser().getShortUserName());
    }

    @Override
    public DFSInputStream getDfsInputStream(long fileId) throws IOException {
        try {
            return inputStreamCache.get(fileId);
        } catch (CompletionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException(e);
            }
        }
    }

    @Override
    public void invalidateInputStream(long fileId) {
        inputStreamCache.invalidate(fileId);
    }
}
