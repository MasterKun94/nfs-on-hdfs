package io.masterkun.nfsonhdfs.cache;

import java.io.IOException;

public interface CacheLoaderHelper {
    Long lookup(CacheKey key) throws IOException;

    StatHolder getStat(Long key) throws IOException;
}
