package io.masterkun.nfsonhdfs.idmapping;

import com.hazelcast.config.matcher.WildcardConfigPatternMatcher;
import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class HostedSftpIdMappingFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HostedSftpIdMappingFactory.class);
    private static final Map<String, HostedSftpIdMapping> mappings = new ConcurrentHashMap<>();
    private final Function<String, HostedSftpIdMapping> loader;

    public HostedSftpIdMappingFactory(AbstractIdMapping innerMapping) {
        this.loader = k -> {
            AppConfig.SftpConfig sftp = Utils.getServerConfig().getSftp();
            long reloadInterval = Utils.getServerConfig().getVfs().getIdMapping().getReloadInterval();
            for (AppConfig.SftpAddressInfo addressInfo : sftp.getAddressInfos()) {
                WildcardConfigPatternMatcher matcher = new WildcardConfigPatternMatcher();
                if (matcher.matches(addressInfo.getHost(), k)) {
                    LOG.info("Create IdMapping for address: {}", k);
                    return new HostedSftpIdMapping(
                            innerMapping,
                            k,
                            addressInfo.getPort(),
                            addressInfo.getUser(),
                            addressInfo.getPassword(),
                            reloadInterval
                    );
                }
            }
            try {
                return new HostedSftpIdMapping(
                        innerMapping,
                        k,
                        -1,
                        sftp.getDefaultUser(),
                        sftp.getDefaultPassword(),
                        reloadInterval
                );
            } catch (Exception e) {
                throw new IllegalArgumentException("IdMapping for address: " + k + " not found, fallback to LocalShellIdMapping");
            }
        };
    }

    public HostedSftpIdMapping get(String host) {
        return mappings.computeIfAbsent(host, loader);
    }

    public void refresh() throws Exception {
        for (HostedSftpIdMapping value : mappings.values()) {
            value.reload();
        }
    }
}
