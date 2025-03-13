package io.masterkun.nfsonhdfs.idmapping;

import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.Utils;
import org.dcache.nfs.v4.NfsIdMapping;

import java.nio.file.Paths;

public class FileIdMappingFactory implements IdMappingFactory {

    private final FileIdMapping mapping;

    public FileIdMappingFactory() {
        final AppConfig.IdMappingConfig idMapping = Utils.getServerConfig().getVfs().getIdMapping();
        String idMappingFile = idMapping.getFile();
        long reloadInterval = idMapping.getReloadInterval();
        this.mapping = new FileIdMapping(Paths.get(idMappingFile).toAbsolutePath(), reloadInterval);
    }

    @Override
    public NfsIdMapping get() {
        return mapping;
    }
}
