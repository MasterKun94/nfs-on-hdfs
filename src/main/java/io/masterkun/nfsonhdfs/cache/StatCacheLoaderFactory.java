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

public class StatCacheLoaderFactory implements Factory<CacheLoader<Long, StatHolder>> {

    private static final Logger LOG = LoggerFactory.getLogger(StatCacheLoaderFactory.class);
    private static CacheLoaderHelper helper;

    public StatCacheLoaderFactory() {
        LOG.info("Init StatCacheLoaderFactory");
    }

    public static void setInnerVfs(CacheLoaderHelper helper) {
        StatCacheLoaderFactory.helper = helper;
    }

    @Override
    public CacheLoader<Long, StatHolder> create() {
        CacheLoaderHelper helper = Objects.requireNonNull(StatCacheLoaderFactory.helper);
        return new CacheLoader<>() {
            @Override
            public StatHolder load(Long k) throws CacheLoaderException {
                try {
                    return helper.getStat(k);
                } catch (IOException e) {
                    throw new CacheLoaderException(e);
                }
            }

            @Override
            public Map<Long, StatHolder> loadAll(Iterable<? extends Long> keys) throws CacheLoaderException {
                try {
                    Map<Long, StatHolder> map = new HashMap<>();
                    for (Long k : keys) {
                        map.put(k, helper.getStat(k));
                    }
                    return map;
                } catch (IOException e) {
                    throw new CacheLoaderException(e);
                }
            }
        };
    }
}
