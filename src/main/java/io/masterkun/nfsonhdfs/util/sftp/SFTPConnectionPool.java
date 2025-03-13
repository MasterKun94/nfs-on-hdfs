package io.masterkun.nfsonhdfs.util.sftp;

import com.jcraft.jsch.ChannelSftp;

import java.io.IOException;

/**
 * @author chenmingkun
 * date    2022/10/31
 * version 1.0
 */
public interface SFTPConnectionPool {

    void shutdown();

    ChannelSftp connect(String host, int port, String user,
                        String password, int timeout) throws IOException;

    void disconnect(ChannelSftp channel) throws IOException;


    /**
     * Class to capture the minimal set of information that distinguish
     * between different connections.
     */
    class ConnectionInfo {
        private final String host;
        private final int port;
        private final String user;
        private final String password;
        private final int timeout;

        ConnectionInfo(String host, int prt, String user, String password, int timeout) {
            this.host = host;
            this.port = prt;
            this.user = user;
            this.password = password;
            this.timeout = timeout;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }

        public int getTimeout() {
            return timeout;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj instanceof ConnectionInfo con) {

                boolean ret = this.host != null && this.host.equalsIgnoreCase(con.host);
                if (this.port >= 0 && this.port != con.port) {
                    ret = false;
                }
                if (this.user == null || !this.user.equalsIgnoreCase(con.user)) {
                    ret = false;
                }
                return ret;
            } else {
                return false;
            }

        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            if (host != null) {
                hashCode += host.hashCode();
            }
            hashCode += port;
            if (user != null) {
                hashCode += user.hashCode();
            }
            return hashCode;
        }
    }
}
