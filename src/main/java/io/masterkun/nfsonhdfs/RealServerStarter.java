package io.masterkun.nfsonhdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryYamlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.masterkun.nfsonhdfs.cache.CacheLoaderHelper;
import io.masterkun.nfsonhdfs.cache.DistributedVfsCache;
import io.masterkun.nfsonhdfs.idmapping.AbstractIdMapping;
import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.Utils;
import io.masterkun.nfsonhdfs.util.cli.CleanCacheEvent;
import io.masterkun.nfsonhdfs.util.cli.CliEvent;
import io.masterkun.nfsonhdfs.util.cli.RefreshFsTableEvent;
import io.masterkun.nfsonhdfs.util.cli.RefreshIdMappingEvent;
import io.masterkun.nfsonhdfs.util.memory.CustomPooledMemoryManagerFactory;
import io.masterkun.nfsonhdfs.vfs.HadoopVirtualFileSystem;
import io.masterkun.nfsonhdfs.vfs.HostedVirtualFileSystem;
import io.masterkun.nfsonhdfs.vfs.WrappedVirtualFileSystem;
import com.typesafe.config.ConfigFactory;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.dcache.nfs.ExportFile;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.MDSOperationExecutor;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.nlm.DistributedLockManager;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.oncrpc4j.rpc.OncRpcProgram;
import org.dcache.oncrpc4j.rpc.OncRpcSvc;
import org.dcache.oncrpc4j.rpc.OncRpcSvcBuilder;
import org.dcache.oncrpc4j.rpc.gss.GssSessionManager;
import org.glassfish.grizzly.memory.DefaultMemoryManagerFactory;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class RealServerStarter {

    private static final Logger LOG = LoggerFactory.getLogger(RealServerStarter.class);

    public static void main(String[] args) {
        try {
            start(args);
        } catch (Exception e) {
            LOG.error("NfsServer start failed", e);
            System.exit(1);
        }
    }

    private static void start(String[] args) throws Exception {
        if (System.getProperty(DefaultMemoryManagerFactory.DMMF_PROP_NAME) == null) {
            System.setProperty(DefaultMemoryManagerFactory.DMMF_PROP_NAME, CustomPooledMemoryManagerFactory.class.getName());
        }
        AppConfig appConfig;
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        if (args.length == 0) {
            appConfig = new AppConfig();
        } else {
            String path = args[0];
            LOG.info("Load server config from path {}", path);
            if (path.endsWith(".yaml") || path.endsWith(".yml")) {
                appConfig = mapper.readValue(new File(path), AppConfig.class);
            } else {
                Map<String, Object> map = ConfigFactory.parseFile(new File(path))
                        .resolve()
                        .root()
                        .unwrapped();
                appConfig = mapper.convertValue(map, AppConfig.class);
            }
            if (args.length > 1) {
                File hadoopConfDIr = new File(args[1]);
                for (File file : Objects.requireNonNull(hadoopConfDIr.listFiles())) {
                    if (file.getName().endsWith("-site.xml")) {
                        LOG.info("Add resource {}", file);
                        Utils.getHadoopConf().addResource(new URL(
                                "file", null, -1, file.getAbsolutePath()
                        ));
                    }
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Server config is :\t{}", mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(appConfig));
        }
        if (appConfig.getKerberos().isDfsEnabled()) {
            UserGroupInformation.setConfiguration(Utils.getHadoopConf());
            UserGroupInformation.loginUserFromKeytab(
                    appConfig.getKerberos().getDfsPrincipal(),
                    appConfig.getKerberos().getDfsKeytab()
            );
        }
        Utils.init(appConfig);

        AppConfig.PrometheusConfig prometheusConfig = appConfig.getPrometheus();
        if (prometheusConfig.isEnabled()) {
            Utils.initializeMetricsExport();
            HTTPServer.Builder builder = new HTTPServer.Builder()
                    .withPort(prometheusConfig.getPort())
                    .withDaemonThreads(true);
            if (prometheusConfig.getThreadNum() > 0) {
                ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(prometheusConfig.getThreadNum(), new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("metrics-server-%d")
                        .build());
                executor.setCorePoolSize(1);
                builder = builder.withExecutorService(executor);
            }
            HTTPServer httpServer = builder.build();
            LOG.info("Start Prometheus http server at port {}", httpServer.getPort());
            Runtime.getRuntime().addShutdownHook(new Thread(httpServer::close));
        }

        URI rootUri = Utils.getResolvedURI(appConfig.getRootDir());
        Config config;
        if (MapUtils.isNotEmpty(appConfig.getHazelcast())) {
            Object conf = Collections.singletonMap("hazelcast", appConfig.getHazelcast());
            config = new InMemoryYamlConfig(mapper.writeValueAsString(conf));
        } else {
            config = new Config();
        }
        VirtualFileSystem vfs = new HadoopVirtualFileSystem(rootUri);
        vfs = new WrappedVirtualFileSystem((HadoopVirtualFileSystem) vfs);
        Utils.init((CacheLoaderHelper) vfs);
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        LOG.info("Start HazelcastInstance {}", hazelcastInstance);
        // create an instance of a filesystem to be exported
        if (appConfig.getVfs().getVfsCache().isEnabled()) {
            LOG.info("Enable VfsCache");
            vfs = new DistributedVfsCache(vfs, hazelcastInstance, appConfig.getServerName());
        }
        // specify file with export entries
        ExportFile exportFile = new ExportFile(new File(appConfig.getExport()));
        exportFile.exports().forEach(export ->
                LOG.info("Export: {}", export)
        );
        VirtualFileSystem maybeVfsCache = vfs;
        if (appConfig.getVfs().isHosted()) {
            vfs = new HostedVirtualFileSystem(vfs, exportFile);
        }
        VirtualFileSystem maybeHostedVfs = vfs;
        if (appConfig.getServer().isCliEventEnabled()) {
            addListener(hazelcastInstance, appConfig, exportFile, maybeVfsCache, maybeHostedVfs);
        }

        AppConfig.ServerConf serverConf = appConfig.getServer();
        // create the RPC service which will handle NFS requests
        OncRpcSvcBuilder builder = new OncRpcSvcBuilder()
                .withPort(serverConf.getPort())
                .withServiceName(appConfig.getServerName());
        if (serverConf.isTcp()) {
            LOG.info("Enable tcp");
            builder = builder.withTCP();
        }
        if (serverConf.isUdp()) {
            LOG.info("Enable udp");
            builder = builder.withUDP();
        }
        if (StringUtils.isNotEmpty(serverConf.getBind())) {
            LOG.info("Bind {}", serverConf.getBind());
            builder = builder.withBindAddress(serverConf.getBind());
        }
        if (serverConf.isJmx()) {
            LOG.info("Enable jmx");
            builder = builder.withJMX();
        }
        if (serverConf.isAutoPublish()) {
            LOG.info("Enable auto_publish");
            builder = builder.withAutoPublish();
        } else {
            LOG.info("Disable auto_publish");
            builder = builder.withoutAutoPublish();
        }
        if (serverConf.getSelectorThreadPoolSize() != null) {
            LOG.info("Set selector_thread_pool_size={}", serverConf.getSelectorThreadPoolSize());
            builder = builder.withSelectorThreadPoolSize(serverConf.getSelectorThreadPoolSize());
        }
        if (serverConf.getWorkerThreadPoolSize() != null) {
            LOG.info("Set worker_thread_pool_size={}", serverConf.getWorkerThreadPoolSize());
            builder = builder.withWorkerThreadPoolSize(serverConf.getWorkerThreadPoolSize());
        }
        if (serverConf.getIoStrategy() != null) {
            LOG.info("Set io_strategy={}", serverConf.getIoStrategy());
            builder = builder.withIoStrategy(serverConf.getIoStrategy());
        } else {
            builder = builder.withWorkerThreadIoStrategy();
        }
        if (serverConf.getSubjectPropagation() != null) {
            LOG.info("Set subject_propagation={}", serverConf.getSubjectPropagation());
            builder = serverConf.getSubjectPropagation() ? builder.withSubjectPropagation() : builder.withoutSubjectPropagation();
        }
        if (serverConf.getMemoryAllocator() != null) {
            LOG.info("Set memory_allocator={}", serverConf.getMemoryAllocator());
            builder = builder.withMemoryAllocator(serverConf.getMemoryAllocator());
        }
        if (appConfig.getKerberos().isRpcEnabled()) {
            LOG.info("Enable rpc_kerberos");
            builder = builder.withGssSessionManager(new GssSessionManager(
                    (transport, context) -> {
                        Principal principal;
                        try {
                            principal = new KerberosPrincipal(context.getSrcName().toString());
                            LOG.debug("Kerberos login {}: {}", principal, context);
                        } catch (GSSException e) {
                            throw new RuntimeException(e);
                        }
                        return new Subject(
                                true,
                                ImmutableSet.of(principal),
                                Collections.emptySet(),
                                Collections.emptySet()
                        );
                    },
                    appConfig.getKerberos().getRpcPrincipal(),
                    appConfig.getKerberos().getRpcKeytab()
            ));
        }
        OncRpcSvc nfsSvc = builder.build();

        // register NFS servers at portmap service
        if (serverConf.isNfsV4()) {
            // create NFS v4.1 server
            NFSServerV41 nfs4 = new NFSServerV41.Builder()
                    .withExportTable(exportFile)
                    .withVfs(vfs)
                    .withLockManager(new DistributedLockManager(hazelcastInstance, appConfig.getServerName() + "_DLM"))
                    .withOperationExecutor(new MDSOperationExecutor() {
                        @Override
                        public nfs_resop4 execute(CompoundContext context, nfs_argop4 args) throws IOException {
                            try {
                                CallContext.setCtx(context);
                                if (LOG.isTraceEnabled()) {
                                    LOG.trace("nfs.execute(op={}, xid={}, local={}, remote={})",
                                            args.argop,
                                            context.getRpcCall().getXid(),
                                            context.getLocalSocketAddress(),
                                            context.getRemoteSocketAddress());
                                }
                                return super.execute(context, args);
                            } finally {
                                CallContext.clearCtx();
                            }
                        }
                    })
                    .build();
            nfsSvc.register(new OncRpcProgram(100003, 4), nfs4);
        }
        if (serverConf.isNfsV3()) {
            // create NFS v3 and mountd servers
            NfsServerV3 nfs3 = new NfsServerV3(exportFile, vfs);
            MountServer mountd = new MountServer(exportFile, vfs);
            nfsSvc.register(new OncRpcProgram(100003, 3), nfs3);
            nfsSvc.register(new OncRpcProgram(100005, 3), mountd);
        }
        // start RPC service
        nfsSvc.start();
        LOG.info("{} is started ", nfsSvc);
    }

    private static void addListener(HazelcastInstance hazelcastInstance,
                                    AppConfig appConfig,
                                    ExportFile exportFile,
                                    VirtualFileSystem maybeVfsCache,
                                    VirtualFileSystem maybeHostedVfs) {
        hazelcastInstance.<CliEvent>getTopic("cliEvent-" + appConfig.getServerName())
                .addMessageListener(message -> {
                    CliEvent event = message.getMessageObject();
                    if (event instanceof CleanCacheEvent) {
                        if (maybeVfsCache instanceof DistributedVfsCache cache) {
                            cache.clearCache();
                        }
                    } else if (event instanceof RefreshFsTableEvent) {
                        try {
                            exportFile.rescan();
                        } catch (IOException e) {
                            LOG.error("Refresh fs table error", e);
                        }
                    } else if (event instanceof RefreshIdMappingEvent) {
                        NfsIdMapping nfsIdMapping = Utils.getNfsIdMapping();
                        if (nfsIdMapping instanceof AbstractIdMapping mapping) {
                            try {
                                mapping.reload();
                            } catch (Exception e) {
                                LOG.error("Refresh id mapping error", e);
                            }
                        }
                        if (maybeHostedVfs instanceof HostedVirtualFileSystem) {
                            try {
                                ((HostedVirtualFileSystem) maybeHostedVfs).refresh();
                            } catch (Exception e) {
                                LOG.error("Refresh HostedVirtualFileSystem error", e);
                            }
                        }
                    }
                });
    }
}
