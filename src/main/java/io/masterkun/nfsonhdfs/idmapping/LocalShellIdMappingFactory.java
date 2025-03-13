package io.masterkun.nfsonhdfs.idmapping;

import org.dcache.nfs.v4.NfsIdMapping;

public class LocalShellIdMappingFactory implements IdMappingFactory {

    public static final NfsIdMapping MAPPING = new UnixIdMapping();

    @Override
    public NfsIdMapping get() {
        return MAPPING;
    }
}
