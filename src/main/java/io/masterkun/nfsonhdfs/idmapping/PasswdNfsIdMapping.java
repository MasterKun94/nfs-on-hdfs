package io.masterkun.nfsonhdfs.idmapping;

import com.google.common.collect.BiMap;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class PasswdNfsIdMapping extends AbstractIdMapping {

    public PasswdNfsIdMapping(long reloadInterval) {
        super(reloadInterval);
        try {
            reload();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doReload(BiMap<String, Integer> userUidMap,
                            BiMap<String, Integer> groupGidMap) throws Exception {
        InputStream passwd = getClass().getClassLoader().getResourceAsStream("passwd");
        if (passwd == null) {
            throw new IOException("passwd resource not found");
        }
        for (String readLine : IOUtils.readLines(passwd, StandardCharsets.UTF_8)) {
            String[] split = readLine.split(":");
            if (split[split.length - 1].equals("/bin/bash")) {
                userUidMap.put(split[0], Integer.parseInt(split[2]));
                groupGidMap.put(split[0], Integer.parseInt(split[3]));
            }
        }
        InputStream group = getClass().getClassLoader().getResourceAsStream("group");
        if (group == null) {
            throw new IOException("group resource not found");
        }
        for (String readLine : IOUtils.readLines(group, StandardCharsets.UTF_8)) {
            String[] split = readLine.split(":");
            groupGidMap.put(split[0], Integer.parseInt(split[2]));
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
