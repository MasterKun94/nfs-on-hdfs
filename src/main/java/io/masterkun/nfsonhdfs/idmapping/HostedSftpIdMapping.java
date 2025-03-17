package io.masterkun.nfsonhdfs.idmapping;

import com.google.common.collect.BiMap;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import io.masterkun.nfsonhdfs.util.sftp.SFTPInputStream;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class HostedSftpIdMapping extends AbstractIdMapping {

    private final Path passwdPath;
    private final Path groupPath;
    private final String address;
    private final AbstractIdMapping innerMapping;
    private final JSch jsch = new JSch();
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private volatile Int2ObjectHashMap<String> uidRealUserMap;

    public HostedSftpIdMapping(AbstractIdMapping innerMapping, String host, int port, String user
            , String password, long reloadInterval) {
        super(reloadInterval);
        this.address = port == -1 ? host : host + ":" + port;
        this.innerMapping = innerMapping;
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.user = Objects.requireNonNull(user);
        this.password = password == null ? "" : password;
        this.passwdPath = new Path("/etc/passwd");
        this.groupPath = new Path("/etc/group");
        try {
            reload();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String uidToRealPrincipal(int id) {
        maybeReload();
        return uidRealUserMap.get(id);
    }

    @Override
    protected void doReload(BiMap<String, Integer> userUidMap,
                            BiMap<String, Integer> groupGidMap) throws IOException {
        Session session = null;
        ChannelSftp channel = null;
        try {
            Int2ObjectHashMap<String> uidRealUserMap = new Int2ObjectHashMap<>();
            session = port <= 0 ? jsch.getSession(user, host) : jsch.getSession(user, host, port);
            session.setTimeout(20000);
            session.setPassword(password);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            try (InputStream passwd = new SFTPInputStream(channel, passwdPath, null)) {
                for (String readLine : IOUtils.readLines(passwd, StandardCharsets.UTF_8)) {
                    String[] split = readLine.split(":");
                    if (innerMapping.hasUserPrincipal(split[0])) {
                        int uid = innerMapping.principalToUid(split[0]);
                        userUidMap.put(split[2], uid);
                        uidRealUserMap.put(Integer.parseInt(split[2]), split[0]);
                    }
                }
            }
            try (InputStream group = new SFTPInputStream(channel, groupPath, null)) {
                for (String readLine : IOUtils.readLines(group, StandardCharsets.UTF_8)) {
                    String[] split = readLine.split(":");
                    if (innerMapping.hasGroupPrincipal(split[0])) {
                        int gid = innerMapping.principalToGid(split[0]);
                        groupGidMap.put(split[2], gid);
                    }
                }
            }
            this.uidRealUserMap = uidRealUserMap;
        } catch (JSchException e) {
            throw new IOException(e);
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }

    @Override
    public String toString() {
        return "HostedSftpIdMapping{" +
                "address='" + address + '\'' +
                ", innerMapping=" + innerMapping +
                '}';
    }
}
