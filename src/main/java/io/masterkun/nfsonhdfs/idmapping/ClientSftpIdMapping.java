package io.masterkun.nfsonhdfs.idmapping;

import com.google.common.collect.BiMap;
import io.masterkun.nfsonhdfs.util.Utils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

public class ClientSftpIdMapping extends AbstractIdMapping {

    private final Path passwdPath;
    private final Path groupPath;
    private final String address;

    public ClientSftpIdMapping(String host, int port, String user, String password, long reloadInterval) {
        super(reloadInterval);
        this.address = port == -1 ? host : host + ":" + port;
        URI uri;
        try {
            uri = new URI(
                    "sftp",
                    user + ":" + password,
                    host,
                    port > 0 ? port : -1,
                    "/",
                    null,
                    null
            );
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        passwdPath = new Path(uri.resolve("/etc/passwd"));
        groupPath = new Path(uri.resolve("/etc/group"));
        try {
            reload();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doReload(BiMap<String, Integer> userUidMap, BiMap<String, Integer> groupGidMap) throws IOException {
        try (InputStream passwd = passwdPath.getFileSystem(Utils.getHadoopConf()).open(passwdPath)) {
            for (String readLine : IOUtils.readLines(passwd, StandardCharsets.UTF_8)) {
                String[] split = readLine.split(":");
                if (split[split.length - 1].equals("/bin/bash")) {
                    userUidMap.put(split[0], Integer.parseInt(split[2]));
                }
            }
        }
        try (InputStream group = groupPath.getFileSystem(Utils.getHadoopConf()).open(groupPath)) {
            for (String readLine : IOUtils.readLines(group, StandardCharsets.UTF_8)) {
                String[] split = readLine.split(":");
                groupGidMap.put(split[0], Integer.parseInt(split[2]));
            }
        }
    }

    @Override
    public String toString() {
        return "ClientSftpIdMapping{" +
                "address='" + address + '\'' +
                '}';
    }
}
