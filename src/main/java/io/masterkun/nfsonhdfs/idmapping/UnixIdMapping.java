package io.masterkun.nfsonhdfs.idmapping;

import io.masterkun.nfsonhdfs.util.Utils;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.apache.hadoop.security.ShellBasedIdMapping;
import org.dcache.nfs.status.BadOwnerException;
import org.dcache.nfs.v4.NfsIdMapping;

import java.io.IOException;
import java.util.regex.Pattern;

public class UnixIdMapping implements NfsIdMapping {
    private final IdMappingServiceProvider idMappingServiceProvider;
    private final Pattern pattern = Pattern.compile("\\d+");

    public UnixIdMapping() {
        try {
            idMappingServiceProvider = new ShellBasedIdMapping(Utils.getHadoopConf(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int principalToUid(String principal) throws BadOwnerException {
        try {
            if (pattern.matcher(principal).matches()) {
                return Integer.parseInt(principal);
            } else {
                return idMappingServiceProvider.getUid(principal);
            }
        } catch (IOException e) {
            throw new BadOwnerException(e.getMessage(), e);
        }
    }

    @Override
    public int principalToGid(String principal) throws BadOwnerException {
        try {
            if (pattern.matcher(principal).matches()) {
                return Integer.parseInt(principal);
            } else {
                return idMappingServiceProvider.getGid(principal);
            }
        } catch (IOException e) {
            throw new BadOwnerException(e.getMessage(), e);
        }
    }

    @Override
    public String uidToPrincipal(int id) {
        return idMappingServiceProvider.getUserName(id, "unknown");
    }

    @Override
    public String gidToPrincipal(int id) {
        return idMappingServiceProvider.getGroupName(id, "unknown");
    }
}
