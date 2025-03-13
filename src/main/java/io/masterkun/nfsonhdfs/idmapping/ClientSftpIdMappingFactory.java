package io.masterkun.nfsonhdfs.idmapping;

import com.hazelcast.config.matcher.WildcardConfigPatternMatcher;
import io.masterkun.nfsonhdfs.CallContext;
import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.Utils;
import org.dcache.nfs.v4.NfsIdMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientSftpIdMappingFactory implements IdMappingFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClientSftpIdMappingFactory.class);
    private final Map<InetAddress, NfsIdMapping> mappings = new ConcurrentHashMap<>();

    @Override
    public NfsIdMapping get() {
        InetAddress clientAddress = CallContext.getClientAddress();
        if (clientAddress == null) {
            return LocalShellIdMappingFactory.MAPPING;
        }
        return mappings.computeIfAbsent(clientAddress, k -> {
            AppConfig.SftpConfig sftp = Utils.getServerConfig().getSftp();
            long reloadInterval = Utils.getServerConfig().getVfs().getIdMapping().getReloadInterval();
            for (AppConfig.SftpAddressInfo addressInfo : sftp.getAddressInfos()) {
                WildcardConfigPatternMatcher matcher = new WildcardConfigPatternMatcher();
                String host = addressInfo.getHost();
                if (matcher.matches(host, k.getHostName()) || matcher.matches(host, k.getHostAddress())) {
                    LOG.info("Create IdMapping for address: {}", k);
                    return new ClientSftpIdMapping(
                            k.getHostAddress(),
                            addressInfo.getPort(),
                            addressInfo.getUser(),
                            addressInfo.getPassword(),
                            reloadInterval
                    );
                }
            }
            try {
                return new ClientSftpIdMapping(
                        clientAddress.getHostAddress(),
                        -1,
                        sftp.getDefaultUser(),
                        sftp.getDefaultPassword(),
                        reloadInterval
                );
            } catch (Exception e) {
                LOG.info("IdMapping for address: {} not found, fallback to LocalShellIdMapping", k, e);
                return LocalShellIdMappingFactory.MAPPING;
            }
        });
    }
}
