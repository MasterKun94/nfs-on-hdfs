package io.masterkun.nfsonhdfs.cache;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedVfsCacheDataSerializableFactory implements DataSerializableFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedVfsCacheDataSerializableFactory.class);

    public DistributedVfsCacheDataSerializableFactory() {
        LOG.info("Init DistributedVfsCacheDataSerializableFactory");
    }

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        return switch (typeId) {
            case 1 -> new CacheKey();
            case 2 -> new StatHolder();
            default -> throw new IllegalArgumentException("illegal type id " + typeId);
        };
    }
}
