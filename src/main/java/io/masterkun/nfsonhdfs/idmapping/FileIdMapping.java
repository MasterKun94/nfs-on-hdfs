package io.masterkun.nfsonhdfs.idmapping;

import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class FileIdMapping extends AbstractIdMapping {
    private final Path idMappingPath;

    public FileIdMapping(Path idMappingPath, long reloadInterval) {
        super(reloadInterval);
        this.idMappingPath = idMappingPath;
        try {
            reload();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doReload(BiMap<String, Integer> userUidMap, BiMap<String, Integer> groupGidMap) throws Exception {
        try (InputStream stream = Files.newInputStream(idMappingPath)) {
            List<String> lines = IOUtils.readLines(stream, StandardCharsets.UTF_8);
            Splitter splitter = Splitter.on(",").trimResults();
            int id = 1110;
            for (String line : lines) {
                List<String> elems = splitter.splitToList(line);
                if (elems.isEmpty()) {
                    throw new IllegalArgumentException("illegal id_mapping line [" + line + "]");
                }
                String principal = elems.get(0);
                int uid;
                if (elems.size() < 2) {
                    if (principal.equals("root")) {
                        uid = 0;
                    } else {
                        do {
                            id++;
                        } while (userUidMap.containsValue(id) || groupGidMap.containsValue(id));
                        uid = id;
                    }
                } else {
                    uid = id = Integer.parseInt(elems.get(1));
                }
                String ug;
                if (elems.size() < 3) {
                    ug = "ug";
                } else {
                    ug = elems.get(2);
                }
                for (char c : ug.toLowerCase().toCharArray()) {
                    Integer oldVal = switch (c) {
                        case 'u' -> userUidMap.put(principal, uid);
                        case 'g' -> groupGidMap.put(principal, uid);
                        default -> throw new IllegalArgumentException("illegal id_mapping line [" + line + "]");
                    };
                    if (oldVal != null) {
                        throw new IllegalArgumentException("duplicate element in line [" + line + "]");
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return "FileIdMapping{" +
                "idMappingPath=" + idMappingPath +
                '}';
    }
}
