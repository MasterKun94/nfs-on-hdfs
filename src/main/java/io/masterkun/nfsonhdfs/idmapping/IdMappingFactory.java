package io.masterkun.nfsonhdfs.idmapping;

import org.dcache.nfs.v4.NfsIdMapping;

public interface IdMappingFactory {
    NfsIdMapping get();
}
