package io.masterkun.nfsonhdfs.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.dcache.oncrpc4j.rpc.IoStrategy;
import org.dcache.oncrpc4j.rpc.MemoryAllocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class AppConfig {

    private String serverName = "nfs-server";
    private String export = "conf/export";
    private String rootDir = "/tmp";
    private KerberosConf kerberos = new KerberosConf();
    private ServerConf server = new ServerConf();
    private VfsConf vfs = new VfsConf();
    private PrometheusConfig prometheus = new PrometheusConfig();
    private SftpConfig sftp = new SftpConfig();
    private Map<String, Object> hazelcast;

    public enum IdMappingStrategy {
        CLIENT,
        LOCAL,
        FILE,
        TEST,
    }

    public enum WriteCommitPolicy {
        FLUSH,
        HFLUSH,
        HSYNC
    }

    @Data
    public static class KerberosConf {
        private boolean dfsEnabled = false;
        private String dfsPrincipal = "";
        private String dfsKeytab = "";
        private boolean rpcEnabled = false;
        private String rpcPrincipal = "";
        private String rpcKeytab = "";
    }

    @Data
    public static class VfsConf {
        private String inputStreamCacheSpec = "maximumSize=128,expireAfterAccess=15m";
        private String dfsClientCacheSpec = "maximumSize=128,expireAfterAccess=24h";
        private VfsCacheConfig vfsCache = new VfsCacheConfig();
        private WriteManagerConfig writeManager = new WriteManagerConfig();
        private IdMappingConfig idMapping = new IdMappingConfig();
        private boolean hosted = false;
    }

    @Data
    public static class IdMappingConfig {
        private IdMappingStrategy strategy = IdMappingStrategy.LOCAL;
        private String file = "conf/id_mapping";
        private long reloadInterval = 1000 * 60 * 15;
    }

    @Data
    public static class VfsCacheConfig {
        private boolean enabled = false;
        private long fsStatExpireMs = 1000 * 60;
        private long cacheExpireMs = 1000 * 60 * 15;
    }

    @Data
    public static class WriteManagerConfig {
        private int writeHighWatermark = 1024 * 1024 * 8;
        private int writeLowWatermark = 1024 * 1024;
        private int writeThreadMaxNum = 100;
        private int writeThreadInitNum = 10;
        private int writeWaitTimeoutMs = 10 * 1000;
        private int commitIntervalMs = 1000;
        private int commitWaitTimeoutMs = 30 * 1000;
        private boolean autoCommit = false;
        private boolean contextCloseOnFinalCommit = true;
        private WriteCommitPolicy writeCommitPolicy = WriteCommitPolicy.HFLUSH;
        private int writeBufferLength = 65536;
    }

    @Data
    public static class ServerConf {
        private int port = 2049;
        private String bind = "0.0.0.0";
        private boolean nfsV3 = false;
        private boolean nfsV4 = true;
        private boolean tcp = true;
        private boolean udp = false;
        private boolean jmx = true;
        private boolean autoPublish = true;
        private Integer selectorThreadPoolSize;
        private Integer workerThreadPoolSize;
        private IoStrategy ioStrategy;
        private Boolean subjectPropagation;
        private MemoryAllocator memoryAllocator = MemoryAllocator.POOLED_DIRECT;
        private boolean cliEventEnabled = false;
    }

    @Data
    public static class DlmConfig {
        private String configPath = "";
    }

    @Data
    public static class PrometheusConfig {
        private boolean enabled = true;
        private int port = 12129;
        private int threadNum = 3;
    }

    @Data
    public static class SftpConfig {
        private List<SftpAddressInfo> addressInfos = new ArrayList<>();
        private String defaultUser;
        private String defaultPassword;
    }

    @Data
    public static class SftpAddressInfo {
        private String host;
        private int port = -1;
        private String user;
        private String password;
    }
}
