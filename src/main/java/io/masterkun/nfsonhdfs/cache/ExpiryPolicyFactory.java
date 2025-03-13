package io.masterkun.nfsonhdfs.cache;

import io.masterkun.nfsonhdfs.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.concurrent.TimeUnit;

public class ExpiryPolicyFactory implements Factory<ExpiryPolicy> {
    private static final Logger LOG = LoggerFactory.getLogger(ExpiryPolicyFactory.class);

    public ExpiryPolicyFactory() {
        LOG.info("Init ExpiryPolicyFactory");
    }

    @Override
    public ExpiryPolicy create() {
        long cacheExpireMs = Utils.getServerConfig().getVfs().getVfsCache().getCacheExpireMs();
        Duration cacheExpireDuration = new Duration(TimeUnit.MILLISECONDS, cacheExpireMs);
        return new ExpiryPolicy() {
            @Override
            public Duration getExpiryForCreation() {
                return cacheExpireDuration;
            }

            @Override
            public Duration getExpiryForAccess() {
                return null;
            }

            @Override
            public Duration getExpiryForUpdate() {
                return cacheExpireDuration;
            }
        };
    }
}
