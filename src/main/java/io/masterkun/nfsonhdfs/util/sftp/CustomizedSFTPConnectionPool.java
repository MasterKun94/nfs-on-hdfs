package io.masterkun.nfsonhdfs.util.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

/**
 * @author chenmingkun
 * date    2022/10/31
 * version 1.0
 */
public class CustomizedSFTPConnectionPool implements SFTPConnectionPool {

    private static final Logger LOG = LoggerFactory.getLogger(CustomizedSFTPConnectionPool.class);

    private final GenericKeyedObjectPool<ConnectionInfo, ChannelSftp> pool;

    public CustomizedSFTPConnectionPool(int maxConnection, String keyFile) {
        GenericKeyedObjectPoolConfig<ChannelSftp> config = new GenericKeyedObjectPoolConfig<>();
        config.setMaxTotal(maxConnection * 5);
        config.setMaxTotalPerKey(maxConnection);
        config.setMaxIdlePerKey(maxConnection);
        config.setJmxEnabled(true);
        config.setJmxNamePrefix("CustomizedSFTPConnectionPool");
        config.setTestWhileIdle(true);
        config.setTestOnBorrow(false);
        config.setTestOnCreate(false);
        config.setTestOnReturn(false);
        config.setTimeBetweenEvictionRuns(Duration.ofMinutes(5));
        pool = new GenericKeyedObjectPool<>(new BaseKeyedPooledObjectFactory<>() {
            @Override
            public ChannelSftp create(ConnectionInfo key) throws Exception {
                String user = key.getUser();
                String password = key.getPassword();
                String host = key.getHost();
                int timeout = key.getTimeout();
                int port = key.getPort();
                // create a new connection and add to pool
                JSch jsch = new JSch();
                Session session;
                ChannelSftp channel;
                try {
                    if (user == null || user.length() == 0) {
                        user = System.getProperty("user.name");
                    }

                    if (password == null) {
                        password = "";
                    }

                    if (keyFile != null && keyFile.length() > 0) {
                        jsch.addIdentity(keyFile);
                    }

                    if (port <= 0) {
                        session = jsch.getSession(user, host);
                    } else {
                        session = jsch.getSession(user, host, port);
                    }
                    session.setTimeout(timeout);
                    session.setPassword(password);

                    java.util.Properties config = new java.util.Properties();
                    config.put("StrictHostKeyChecking", "no");
                    session.setConfig(config);

                    session.connect();
                    channel = (ChannelSftp) session.openChannel("sftp");
                    channel.connect();
                } catch (JSchException e) {
                    throw new IOException(StringUtils.stringifyException(e));
                }
                return channel;
            }

            @Override
            public PooledObject<ChannelSftp> wrap(ChannelSftp value) {
                return new DefaultPooledObject<>(value);
            }

            @Override
            public boolean validateObject(ConnectionInfo key, PooledObject<ChannelSftp> p) {
                final ChannelSftp object = p.getObject();
                if (object.isConnected()) {
                    try {
                        object.ls("/");
                        return true;
                    } catch (Exception e) {
                        LOG.error("Validate Sftp conn error", e);
                        return false;
                    }
                }
                return false;
            }

            @Override
            public void destroyObject(ConnectionInfo key, PooledObject<ChannelSftp> p) throws Exception {
                final ChannelSftp channel = p.getObject();
                if (channel.isConnected()) {
                    LOG.info("Disconnect sftp conn {}", channel);
                    Session session = channel.getSession();
                    channel.disconnect();
                    session.disconnect();
                }
            }
        }, config);
    }

    @Override
    public void shutdown() {
        pool.close();
    }

    @Override
    public ChannelSftp connect(String host, int port, String user, String password, int timeout) throws IOException {
        try {
            return pool.borrowObject(new ConnectionInfo(host, port, user, password, timeout));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void disconnect(ChannelSftp channel) throws IOException {
        try {
            final Session session = channel.getSession();
            final String host = session.getHost();
            final int port = session.getPort();
            final String user = session.getUserName();
            final int timeout = session.getTimeout();
            pool.returnObject(new ConnectionInfo(host, port, user, "", timeout), channel);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
