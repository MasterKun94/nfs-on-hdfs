/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.masterkun.nfsonhdfs.util.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SFTP FileSystem.
 */
public class SFTPFileSystem extends FileSystem {

    public static final Logger LOG = LoggerFactory.getLogger(SFTPFileSystem.class);
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024;
    public static final String FS_SFTP_USER_PREFIX = "fs.sftp.user.";
    public static final String FS_SFTP_PASSWORD_PREFIX = "fs.sftp.password.";
    public static final String FS_SFTP_HOST = "fs.sftp.host";
    public static final String FS_SFTP_HOST_PORT = "fs.sftp.host.port";
    public static final String FS_SFTP_KEYFILE = "fs.sftp.keyfile";
    public static final String FS_SFTP_TIMEOUT = "fs.sftp.timeout";
    public static final String FS_SFTP_CONNECTION_MAX = "fs.sftp.connection.max";
    public static final String E_SAME_DIRECTORY_ONLY =
            "only same directory renames are supported";
    public static final String E_HOST_NULL = "Invalid host specified";
    public static final String E_USER_NULL =
            "No user specified for sftp connection. Expand URI or credential file.";
    public static final String E_PATH_DIR = "Path %s is a directory.";
    public static final String E_FILE_STATUS = "Failed to get file status";
    public static final String E_FILE_NOTFOUND = "File %s does not exist.";
    public static final String E_FILE_EXIST = "File already exists: %s";
    public static final String E_CREATE_DIR =
            "create(): Mkdirs failed to create: %s";
    public static final String E_DIR_CREATE_FROMFILE =
            "Can't make directory for path %s since it is a file.";
    public static final String E_MAKE_DIR_FORPATH =
            "Can't make directory for path \"%s\" under \"%s\".";
    public static final String E_DIR_NOTEMPTY = "Directory: %s is not empty.";
    public static final String E_FILE_CHECK_FAILED = "File check failed";
    public static final String E_SPATH_NOTEXIST = "Source path %s does not exist";
    public static final String E_DPATH_EXIST =
            "Destination path %s already exist, cannot rename!";
    public static final String E_FAILED_GETHOME = "Failed to get home directory";
    public static final String E_FAILED_DISCONNECT = "Failed to disconnect";
    public static final String E_FS_CLOSED = "FileSystem is closed!";
    private static final int DEFAULT_SFTP_PORT = 22;
    private static final int DEFAULT_MAX_CONNECTION = 10;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private SFTPConnectionPool connectionPool;
    private URI uri;

    /**
     * Set configuration from UI.
     *
     * @param uriInfo
     * @param conf
     * @throws IOException
     */
    private void setConfigurationFromURI(URI uriInfo, Configuration conf)
            throws IOException {

        // get host information from URI
        String host = uriInfo.getHost();
        host = (host == null) ? conf.get(FS_SFTP_HOST, null) : host;
        if (host == null) {
            throw new IOException(E_HOST_NULL);
        }
        conf.set(FS_SFTP_HOST, host);

        int port = uriInfo.getPort();
        port = (port == -1)
                ? conf.getInt(FS_SFTP_HOST_PORT, DEFAULT_SFTP_PORT)
                : port;
        conf.setInt(FS_SFTP_HOST_PORT, port);

        // get user/password information from URI
        String userAndPwdFromUri = uriInfo.getUserInfo();
        if (userAndPwdFromUri != null) {
            String[] userPasswdInfo = userAndPwdFromUri.split(":");
            String user = userPasswdInfo[0];
            user = URLDecoder.decode(user, StandardCharsets.UTF_8);
            conf.set(FS_SFTP_USER_PREFIX + host, user);
            if (userPasswdInfo.length > 1) {
                conf.set(FS_SFTP_PASSWORD_PREFIX + host + "." +
                        user, userPasswdInfo[1]);
            }
        }

        String user = conf.get(FS_SFTP_USER_PREFIX + host);
        if (user == null || user.equals("")) {
            throw new IllegalStateException(E_USER_NULL);
        }

        int connectionMax =
                conf.getInt(FS_SFTP_CONNECTION_MAX, DEFAULT_MAX_CONNECTION);
        String keyFile = conf.get(FS_SFTP_KEYFILE, null);

        connectionPool = new CustomizedSFTPConnectionPool(connectionMax, keyFile);
    }

    /**
     * Connecting by using configuration parameters.
     *
     * @return An FTPClient instance
     * @throws IOException
     */
    private ChannelSftp connect(URI uri) throws IOException {
        checkNotClosed();

        Configuration conf = getConf();
        String host = conf.get(FS_SFTP_HOST, null);
        int port = conf.getInt(FS_SFTP_HOST_PORT, DEFAULT_SFTP_PORT);
        String user;
        String pwd;
        if (uri == null || StringUtils.isEmpty(uri.getUserInfo())) {
            user = conf.get(FS_SFTP_USER_PREFIX + host, null);
            pwd = conf.get(FS_SFTP_PASSWORD_PREFIX + host + "." + user, null);
        } else {
            String[] arr = uri.getUserInfo().split(":", 2);
            user = arr[0];
            pwd = arr[1];
        }

        int timeout = conf.getInt(FS_SFTP_TIMEOUT, 60000);

        return connectionPool.connect(host, port, user, pwd, timeout);
    }

    /**
     * Logout and disconnect the given channel.
     *
     * @param channel
     * @throws IOException
     */
    private void disconnect(ChannelSftp channel) throws IOException {
        connectionPool.disconnect(channel);
    }

    /**
     * Resolve against given working directory.
     *
     * @param workDir
     * @param path
     * @return absolute path
     */
    private Path makeAbsolute(Path workDir, Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workDir, path);
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     *
     * @throws IOException
     */
    private boolean exists(ChannelSftp channel, Path file) throws IOException {
        try {
            getFileStatus(channel, file);
            return true;
        } catch (FileNotFoundException fnfe) {
            return false;
        } catch (IOException ioe) {
            throw new IOException(E_FILE_STATUS, ioe);
        }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     */
    @SuppressWarnings("unchecked")
    private FileStatus getFileStatus(ChannelSftp client, Path file)
            throws IOException {
        FileStatus fileStat = null;
        Path workDir;
        try {
            workDir = new Path(client.pwd());
        } catch (SftpException e) {
            throw new IOException(e);
        }
        Path absolute = makeAbsolute(workDir, file);
        Path parentPath = absolute.getParent();
        if (parentPath == null) { // root directory
            long length = -1; // Length of root directory on server not known
            boolean isDir = true;
            int blockReplication = 1;
            long modTime = -1; // Modification time of root directory not known.
            Path root = new Path("/");
            return new FileStatus(length, isDir, blockReplication, DEFAULT_BLOCK_SIZE,
                    modTime,
                    root.makeQualified(this.getUri(), this.getWorkingDirectory(client)));
        }
        String pathName = parentPath.toUri().getPath();
        Vector<LsEntry> sftpFiles;
        try {
            sftpFiles = (Vector<LsEntry>) client.ls(pathName);
        } catch (SftpException e) {
            throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
        }
        if (sftpFiles != null) {
            for (LsEntry sftpFile : sftpFiles) {
                if (sftpFile.getFilename().equals(file.getName())) {
                    // file found in directory
                    fileStat = getFileStatus(client, sftpFile, parentPath);
                    break;
                }
            }
            if (fileStat == null) {
                throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
            }
        } else {
            throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
        }
        return fileStat;
    }

    /**
     * Convert the file information in LsEntry to a {@link FileStatus} object. *
     *
     * @param sftpFile
     * @param parentPath
     * @return file status
     * @throws IOException
     */
    private FileStatus getFileStatus(ChannelSftp channel, LsEntry sftpFile,
                                     Path parentPath) throws IOException {

        SftpATTRS attr = sftpFile.getAttrs();
        long length = attr.getSize();
        boolean isDir = attr.isDir();
        boolean isLink = attr.isLink();
        if (isLink) {
            String link = parentPath.toUri().getPath() + "/" + sftpFile.getFilename();
            try {
                link = channel.realpath(link);

                Path linkParent = new Path("/", link);

                FileStatus fstat = getFileStatus(channel, linkParent);
                isDir = fstat.isDirectory();
                length = fstat.getLen();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        int blockReplication = 1;
        // Using default block size since there is no way in SFTP channel to know of
        // block sizes on server. The assumption could be less than ideal.
        long modTime = attr.getMTime() * 1000L; // convert to milliseconds
        long accessTime = attr.getATime() * 1000L;
        FsPermission permission = getPermissions(sftpFile);
        // not be able to get the real user group name, just use the user and group
        // id
        String user = Integer.toString(attr.getUId());
        String group = Integer.toString(attr.getGId());
        Path filePath = new Path(parentPath, sftpFile.getFilename());

        return new FileStatus(length, isDir, blockReplication, DEFAULT_BLOCK_SIZE, modTime,
                accessTime, permission, user, group, filePath.makeQualified(
                this.getUri(), this.getWorkingDirectory(channel)));
    }

    /**
     * Return file permission.
     *
     * @param sftpFile sftpFile
     * @return file permission
     */
    private FsPermission getPermissions(LsEntry sftpFile) {
        return new FsPermission((short) sftpFile.getAttrs().getPermissions());
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     */
    private boolean mkdirs(ChannelSftp client, Path file, FsPermission permission)
            throws IOException {
        boolean created = true;
        Path workDir;
        try {
            workDir = new Path(client.pwd());
        } catch (SftpException e) {
            throw new IOException(e);
        }
        Path absolute = makeAbsolute(workDir, file);
        String pathName = absolute.getName();
        if (!exists(client, absolute)) {
            Path parent = absolute.getParent();
            created = mkdirs(client, parent, FsPermission.getDefault());
            if (created) {
                String parentDir = parent.toUri().getPath();
                boolean succeeded = true;
                try {
                    final String previousCwd = client.pwd();
                    client.cd(parentDir);
                    client.mkdir(pathName);
                    client.cd(previousCwd);
                } catch (SftpException e) {
                    throw new IOException(String.format(E_MAKE_DIR_FORPATH, pathName,
                            parentDir));
                }
                created = created & succeeded;
            }
        } else if (isFile(client, absolute)) {
            throw new IOException(String.format(E_DIR_CREATE_FROMFILE, absolute));
        }
        return created;
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     *
     * @throws IOException
     */
    private boolean isFile(ChannelSftp channel, Path file) throws IOException {
        try {
            return !getFileStatus(channel, file).isDirectory();
        } catch (FileNotFoundException e) {
            return false; // file does not exist
        } catch (IOException ioe) {
            throw new IOException(E_FILE_CHECK_FAILED, ioe);
        }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     */
    private boolean delete(ChannelSftp channel, Path file, boolean recursive)
            throws IOException {
        Path workDir;
        try {
            workDir = new Path(channel.pwd());
        } catch (SftpException e) {
            throw new IOException(e);
        }
        Path absolute = makeAbsolute(workDir, file);
        String pathName = absolute.toUri().getPath();
        FileStatus fileStat = null;
        try {
            fileStat = getFileStatus(channel, absolute);
        } catch (FileNotFoundException e) {
            // file not found, no need to delete, return true
            return false;
        }
        if (!fileStat.isDirectory()) {
            boolean status = true;
            try {
                channel.rm(pathName);
            } catch (SftpException e) {
                status = false;
            }
            return status;
        } else {
            boolean status = true;
            FileStatus[] dirEntries = listStatus(channel, absolute);
            if (dirEntries.length > 0) {
                if (!recursive) {
                    throw new IOException(String.format(E_DIR_NOTEMPTY, file));
                }
                for (FileStatus dirEntry : dirEntries) {
                    delete(channel, new Path(absolute, dirEntry.getPath()),
                            recursive);
                }
            }
            try {
                channel.rmdir(pathName);
            } catch (SftpException e) {
                status = false;
            }
            return status;
        }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     */
    @SuppressWarnings("unchecked")
    private FileStatus[] listStatus(ChannelSftp client, Path file)
            throws IOException {
        Path workDir;
        try {
            workDir = new Path(client.pwd());
        } catch (SftpException e) {
            throw new IOException(e);
        }
        Path absolute = makeAbsolute(workDir, file);
        FileStatus fileStat = getFileStatus(client, absolute);
        if (!fileStat.isDirectory()) {
            return new FileStatus[]{fileStat};
        }
        Vector<LsEntry> sftpFiles;
        try {
            sftpFiles = (Vector<LsEntry>) client.ls(absolute.toUri().getPath());
        } catch (SftpException e) {
            throw new IOException(e);
        }
        ArrayList<FileStatus> fileStats = new ArrayList<>();
        for (LsEntry entry : sftpFiles) {
            String fname = entry.getFilename();
            // skip current and parent directory, ie. "." and ".."
            if (!".".equalsIgnoreCase(fname) && !"..".equalsIgnoreCase(fname)) {
                fileStats.add(getFileStatus(client, entry, absolute));
            }
        }
        return fileStats.toArray(new FileStatus[0]);
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     *
     * @param channel
     * @param src
     * @param dst
     * @return rename successful?
     * @throws IOException
     */
    private boolean rename(ChannelSftp channel, Path src, Path dst)
            throws IOException {
        Path workDir;
        try {
            workDir = new Path(channel.pwd());
        } catch (SftpException e) {
            throw new IOException(e);
        }
        Path absoluteSrc = makeAbsolute(workDir, src);
        Path absoluteDst = makeAbsolute(workDir, dst);

        if (!exists(channel, absoluteSrc)) {
            throw new IOException(String.format(E_SPATH_NOTEXIST, src));
        }
        if (exists(channel, absoluteDst)) {
            throw new IOException(String.format(E_DPATH_EXIST, dst));
        }
        boolean renamed = true;
        try {
            final String previousCwd = channel.pwd();
            channel.cd("/");
            channel.rename(src.toUri().getPath(), dst.toUri().getPath());
            channel.cd(previousCwd);
        } catch (SftpException e) {
            renamed = false;
        }
        return renamed;
    }

    @Override
    public void initialize(URI uriInfo, Configuration conf) throws IOException {
        super.initialize(uriInfo, conf);

        setConfigurationFromURI(uriInfo, conf);
        setConf(conf);
        this.uri = uriInfo;
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        ChannelSftp channel = connect(f.toUri());
        Path workDir;
        try {
            workDir = new Path(channel.pwd());
        } catch (SftpException e) {
            throw new IOException(e);
        }
        Path absolute = makeAbsolute(workDir, f);
        FileStatus fileStat = getFileStatus(channel, absolute);
        if (fileStat.isDirectory()) {
            disconnect(channel);
            throw new IOException(String.format(E_PATH_DIR, f));
        }
        try {
            // the path could be a symbolic link, so get the real path
            absolute = new Path("/", channel.realpath(absolute.toUri().getPath()));
        } catch (SftpException e) {
            throw new IOException(e);
        }
        return new FSDataInputStream(
                new SFTPInputStream(channel, absolute, statistics)) {
            private boolean closed;

            @Override
            public synchronized void close() throws IOException {
                if (closed) {
                    return;
                }
                try {
                    super.close();
                } finally {
                    disconnect(channel);
                    closed = true;
                }
            }
        };
    }

    /**
     * A stream obtained via this call must be closed before using other APIs of
     * this class or else the invocation will block.
     */
    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
                                     boolean overwrite, int bufferSize, short replication,
                                     long blockSize,
                                     Progressable progress) throws IOException {
        final ChannelSftp client = connect(f.toUri());
        Path workDir;
        try {
            workDir = new Path(client.pwd());
        } catch (SftpException e) {
            throw new IOException(e);
        }
        Path absolute = makeAbsolute(workDir, f);
        if (exists(client, f)) {
            if (overwrite) {
                delete(client, f, false);
            } else {
                disconnect(client);
                throw new IOException(String.format(E_FILE_EXIST, f));
            }
        }
        Path parent = absolute.getParent();
        if (parent == null || !mkdirs(client, parent, FsPermission.getDefault())) {
            parent = (parent == null) ? new Path("/") : parent;
            disconnect(client);
            throw new IOException(String.format(E_CREATE_DIR, parent));
        }
        OutputStream os;
        try {
            final String previousCwd = client.pwd();
            client.cd(parent.toUri().getPath());
            os = client.put(f.getName());
            client.cd(previousCwd);
        } catch (SftpException e) {
            throw new IOException(e);
        }

        return new FSDataOutputStream(os, statistics) {
            private boolean closed1;

            @Override
            public synchronized void close() throws IOException {
                if (closed1) {
                    return;
                }
                super.close();
                disconnect(client);
                closed1 = true;
            }
        };
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
                                     Progressable progress)
            throws IOException {
        throw new UnsupportedOperationException("Append is not supported "
                + "by SFTPFileSystem");
    }

    /*
     * The parent of source and destination can be different. It is suppose to
     * work like 'move'
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        ChannelSftp channel = connect(src.toUri());
        try {
            return rename(channel, src, dst);
        } finally {
            disconnect(channel);
        }
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        ChannelSftp channel = connect(f.toUri());
        try {
            return delete(channel, f, recursive);
        } finally {
            disconnect(channel);
        }
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        ChannelSftp client = connect(f.toUri());
        try {
            return listStatus(client, f);
        } finally {
            disconnect(client);
        }
    }

    @Override
    public Path getWorkingDirectory() {
        // Return home directory always since we do not maintain state.
        return getHomeDirectory();
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        // we do not maintain the working directory state
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     */
    private Path getWorkingDirectory(ChannelSftp client) {
        // Return home directory always since we do not maintain state.
        return getHomeDirectory(client);
    }

    @Override
    public Path getHomeDirectory() {
        ChannelSftp channel = null;
        try {
            channel = connect(null);
            return new Path(channel.pwd());
        } catch (Exception ioe) {
            return null;
        } finally {
            try {
                disconnect(channel);
            } catch (IOException ioe) {
                // ignore
            }
        }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     */
    private Path getHomeDirectory(ChannelSftp channel) {
        try {
            return new Path(channel.pwd());
        } catch (Exception ioe) {
            return null;
        }
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        ChannelSftp client = connect(f.toUri());
        try {
            return mkdirs(client, f, permission);
        } finally {
            disconnect(client);
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        ChannelSftp channel = connect(f.toUri());
        try {
            return getFileStatus(channel, f);
        } finally {
            disconnect(channel);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.getAndSet(true)) {
            return;
        }
        try {
            super.close();
        } finally {
            if (connectionPool != null) {
                connectionPool.shutdown();
            }
        }
    }

    /**
     * Verify that the input stream is open. Non blocking; this gives
     * the last state of the volatile {@link #closed} field.
     *
     * @throws IOException if the connection is closed.
     */
    private void checkNotClosed() throws IOException {
        if (closed.get()) {
            throw new IOException(uri + ": " + E_FS_CLOSED);
        }
    }
}
