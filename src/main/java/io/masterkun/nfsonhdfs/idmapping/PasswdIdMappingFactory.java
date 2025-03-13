package io.masterkun.nfsonhdfs.idmapping;

import org.dcache.nfs.v4.NfsIdMapping;

public class PasswdIdMappingFactory implements IdMappingFactory {
    private final NfsIdMapping mapping = new PasswdNfsIdMapping(15 * 60 * 1000);

    @Override
    public NfsIdMapping get() {
        return mapping;
    }
}
