package io.masterkun.nfsonhdfs.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class LookupCacheLoaderFactory implements Factory<CacheLoader<CacheKey, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(LookupCacheLoaderFactory.class);
    private static CacheLoaderHelper helper;

    public LookupCacheLoaderFactory() {
        LOG.info("Init LookupCacheLoaderFactory");
    }

    public static void setInnerVfs(CacheLoaderHelper helper) {
        LookupCacheLoaderFactory.helper = helper;
    }

    @Override
    public CacheLoader<CacheKey, Long> create() {
        CacheLoaderHelper helper = Objects.requireNonNull(LookupCacheLoaderFactory.helper);
        return new CacheLoader<>() {
            @Override
            public Long load(CacheKey k) throws CacheLoaderException {
                try {
                    return helper.lookup(k);
                } catch (IOException e) {
                    throw new CacheLoaderException(e);
                }
            }

            @Override
            public Map<CacheKey, Long> loadAll(Iterable<? extends CacheKey> keys) throws CacheLoaderException {
                try {
                    Map<CacheKey, Long> map = new HashMap<>();
                    for (CacheKey k : keys) {
                        map.put(k, helper.lookup(k));
                    }
                    return map;
                } catch (IOException e) {
                    throw new CacheLoaderException(e);
                }
            }
        };
    }
}
