package io.masterkun.nfsonhdfs.writemanager;

import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.StringBuilderFormattable;
import io.masterkun.nfsonhdfs.util.Utils;
import io.masterkun.nfsonhdfs.vfs.FileHandle;
import io.prometheus.client.Gauge;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * 数据写入上下文，每个上下文对应一个文件写入的数据流
 */
public class WriteContextImpl implements WriteContext, StringBuilderFormattable {

    private static final Logger LOG = LoggerFactory.getLogger(WriteContextImpl.class);
    private static final Gauge PENDING_WRITE_BYTES_GAUGE = Gauge.build()
            .name("pending_write_bytes")
            .help("Pending Write Bytes")
            .register();
    private static final Gauge CURRENT_WRITE_TASK_GAUGE = Gauge.build()
            .name("current_write_task")
            .help("Current Write Task")
            .register();
    private static final ExecutorService WRITE_SERVICE = new ThreadPoolExecutor(
            Utils.getServerConfig().getVfs().getWriteManager().getWriteThreadInitNum(),
            Utils.getServerConfig().getVfs().getWriteManager().getWriteThreadMaxNum(),
            1,
            TimeUnit.HOURS,
            new ArrayBlockingQueue<>(10)
    );
    private static final ThreadLocal<Queue<PendingAction>> PENDING_ACTION_TL = ThreadLocal.withInitial(
            () -> new ManyToOneConcurrentArrayQueue<>(128));
    private static final ThreadLocal<Queue<PendingAction>> UNORDERED_ACTION_TL = ThreadLocal.withInitial(
            PriorityQueue::new);
    private static final AppConfig.WriteCommitPolicy WRITE_COMMIT_POLICY = Utils.getServerConfig()
            .getVfs()
            .getWriteManager()
            .getWriteCommitPolicy();
    private final FileHandle fileHandle;
    private final DFSClient dfsClient;
    /**
     * 待写入数据量
     */
    private final AtomicLong pendingWriteBytes = new AtomicLong();
    private final AtomicReference<PendingCommit> finalCommit = new AtomicReference<>();
    private final WriteContextFactoryImpl factory;
    /**
     * 待写入或待提交请求队列，按照偏移量，写入数据量排序
     */
    private final Queue<PendingAction> pendingActions;
    private HdfsDataOutputStream out;
    /**
     * 写入线程
     */
    private volatile Thread parkThread;
    /**
     * 当前提交的偏移量
     */
    private volatile boolean running;
    private volatile Exception writeException;
    private boolean firstWrite = true;
    private long commitOffset = 0;

    private WriteContextImpl(FileHandle fileHandle, DFSClient dfsClient, HdfsDataOutputStream out, WriteContextFactoryImpl factory) {
        this.fileHandle = fileHandle;
        this.dfsClient = dfsClient;
        this.out = out;
        this.factory = factory;
        this.pendingActions = startWrite();
    }

    public static WriteContextImpl get(FileHandle fileHandle, DFSClient dfsClient, HdfsDataOutputStream out, WriteContextFactoryImpl factory) {
        return new WriteContextImpl(fileHandle, dfsClient, out, factory);
    }

    private void check() throws IOException {
        if (writeException != null) {
            if (writeException instanceof IOException) {
                throw (IOException) writeException;
            } else if (writeException instanceof RuntimeException) {
                throw (RuntimeException) writeException;
            } else {
                throw new RuntimeException(writeException);
            }
        }
    }

    /**
     * 写入数据
     */
    @Override
    public long handleWrite(long offset, byte[] data, int count) throws IOException {
        check();
        if (running) {
            pendingActions.add(ByteArrayPendingWrite.get(offset, count, data));
            unpark();
            PENDING_WRITE_BYTES_GAUGE.inc(count);
            return pendingWriteBytes.addAndGet(count);
        } else {
            throw new IOException("write context already closed");
        }
    }

    @Override
    public long handleWrite(long offset, ByteBuffer buffer) throws IOException {
        check();
        if (running) {
            int count = buffer.remaining();
            pendingActions.add(ByteBufferPendingWrite.get(offset, count, buffer));
            unpark();
            PENDING_WRITE_BYTES_GAUGE.inc(count);
            return pendingWriteBytes.addAndGet(count);
        } else {
            throw new IOException("write context already closed");
        }
    }

    /**
     * 持久化数据
     */
    @Override
    public void handleCommit(long offset, int count) throws IOException {
        if (writeException != null) {
            if (writeException instanceof IOException) {
                throw (IOException) writeException;
            } else if (writeException instanceof RuntimeException) {
                throw (RuntimeException) writeException;
            } else {
                throw new RuntimeException(writeException);
            }
        }
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (running) {
            if (offset == 0 && count == 0) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} receive final commit", this);
                }
                finalCommit.set(new PendingCommit(offset, count, f));
            } else {
                pendingActions.add(new PendingCommit(offset, count, f));
            }
            unpark();
        } else {
            throw new IOException("write context already closed");
        }
        int commitWaitTimeoutMs = Utils.getServerConfig()
                .getVfs()
                .getWriteManager()
                .getCommitWaitTimeoutMs();
        try {
            f.get(commitWaitTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException(e);
            }
        } catch (TimeoutException e) {
            throw new IOException(e);
        }
    }

    private void park(Thread currentThread, long parkNanos) {
        parkThread = currentThread;
        try {
            LockSupport.parkNanos(this, parkNanos);
        } finally {
            parkThread = null;
        }
    }

    private void unpark() {
        LockSupport.unpark(parkThread);
    }

    /**
     * 开启写入线程
     */
    private Queue<PendingAction> startWrite() {
        AppConfig.WriteManagerConfig config = Utils.getServerConfig()
                .getVfs()
                .getWriteManager();
        final CompletableFuture<Queue<PendingAction>> f = new CompletableFuture<>();
        this.running = true;
        WRITE_SERVICE.submit(() -> {
            final Queue<PendingAction> pendingActions = PENDING_ACTION_TL.get();
            final Queue<PendingAction> unorderedActions = UNORDERED_ACTION_TL.get();
            pendingActions.clear();
            unorderedActions.clear();
            f.complete(pendingActions);
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} start write", WriteContextImpl.this);
            }
            WriteContextImpl.CURRENT_WRITE_TASK_GAUGE.inc();

            final Thread currentThread = Thread.currentThread();
            final int writeWaitTimeoutMs = config.getWriteWaitTimeoutMs();
            final boolean contextCloseOnFinalCommit = config.isContextCloseOnFinalCommit();
            final int commitIntervalMs = config.getCommitIntervalMs();
            final boolean autoCommit = Utils.getServerConfig().getVfs().getWriteManager().isAutoCommit();
            final Gauge pendingWriteBytesGauge = WriteContextImpl.PENDING_WRITE_BYTES_GAUGE;
            final AtomicReference<PendingCommit> finalCommit = WriteContextImpl.this.finalCommit;
            final AtomicLong pendingWriteBytes = WriteContextImpl.this.pendingWriteBytes;

            try {
                // 设置写入线程，以便于当有新数据写入时及时唤醒
                long refreshTime = System.currentTimeMillis();
                long commitTime = refreshTime;
                long current;
                long parkNanos = TimeUnit.MILLISECONDS.toNanos(1000);
                while ((current = System.currentTimeMillis()) - refreshTime < writeWaitTimeoutMs) {
                    if (autoCommit && out.getPos() > commitOffset && current - commitTime > commitIntervalMs) {
                        // 定时commit，防止有缓冲数据长时间未持久化
                        doCommit(AppConfig.WriteCommitPolicy.HSYNC);
                        commitTime = current;
                    }
                    // 客户端发来数据处理
                    PendingAction head = pendingActions.poll();
                    if (head == null) {
                        // 没有客户端发来数据
                        PendingCommit pendingCommit;
                        if (unorderedActions.isEmpty() && (pendingCommit = finalCommit.get()) != null) {
                            // 如果没有任何未处理的消息并且收到最终提交消息，则进行提交
                            doCommit(pendingCommit);
                            if (contextCloseOnFinalCommit) {
                                if (pendingActions.isEmpty()) {
                                    // 跳出循环
                                    break;
                                }
                            } else {
                                finalCommit.compareAndSet(pendingCommit, null);
                            }
                        }
                    } else if (head instanceof PendingWrite write) {
                        // 数据写入处理
                        if (write.offset() > out.getPos()) {
                            // 消息的写入偏移量大于预期的写入偏移量，可能出现消息乱序，将该消息放入乱序队列，并继续轮询
                            unorderedActions.add(write);
                            park(currentThread, parkNanos);
                            continue;
                        } else {
                            // 消息的写入偏移量小于等于预期的写入偏移量
                            refreshTime = current;
                            // 写入数据
                            doWrite(write);
                            firstWrite = false;
                            // 写入成功，更新指标
                            pendingWriteBytesGauge.dec(write.count());
                            pendingWriteBytes.getAndAdd(-write.count());
                        }
                    } else if (head instanceof PendingCommit commit) {
                        // 数据提交处理
                        if (commit.offset() + commit.count() > out.getPos()) {
                            // 提交偏移量+提交数据大于预期写入偏移量，可能出现消息乱序，将该消息放入乱序队列，并继续轮询
                            unorderedActions.add(commit);
                            park(currentThread, parkNanos);
                            continue;
                        } else {
                            // 提交偏移量+提交数据小于等于预期写入偏移量
                            refreshTime = current;
                            // 提交数据
                            doCommit(commit);
                            commitTime = current;
                        }
                    } else {
                        throw new RuntimeException();
                    }

                    // 乱序消息处理
                    boolean park = head == null;
                    if (!unorderedActions.isEmpty()) {
                        head = unorderedActions.peek();
                        while (head != null) {
                            if (head instanceof PendingWrite write) {
                                if (write.offset() > out.getPos()) {
                                    // 写入偏移量大于预期写入偏移量，跳出乱序消息处理循环
                                    break;
                                }
                                head = unorderedActions.poll();
                                if (head == null || head != write) {
                                    throw new IllegalArgumentException("unexpected");
                                }
                                // 写入乱序消息
                                doWrite(write);
                                firstWrite = false;
                                pendingWriteBytesGauge.dec(write.count());
                                pendingWriteBytes.getAndAdd(-write.count());
                            } else if (head instanceof PendingCommit commit) {
                                if (commit.offset() + commit.count() > out.getPos()) {
                                    // 提交偏移量+提交数据大于预期写入偏移量，跳出乱序消息处理循环
                                    park(currentThread, parkNanos);
                                    break;
                                }
                                head = unorderedActions.poll();
                                if (head == null || head != commit) {
                                    throw new IllegalArgumentException("unexpected");
                                }
                                // 提交乱序消息
                                doCommit(commit);
                                commitTime = current;
                            } else {
                                throw new RuntimeException();
                            }
                            park = false;
                            head = unorderedActions.peek();
                        }
                    }
                    if (park) {
                        park(currentThread, parkNanos);
                    }
                }

                running = false;
                doCommit(WRITE_COMMIT_POLICY);
                if (pendingActions.isEmpty()) {
                    if (finalCommit.getAndSet(null) != null) {
                        // 如果没有挂起的消息并且有最终提交消息说明写入已完成
                        LOG.info("{} closing because write finished", this);
                    } else {
                        // 如果没有挂起的消息了但是也没有最终提交消息那么告警
                        LOG.info("{} closing because no pending commands received", this);
                    }
                } else {
                    // 还有挂起的消息，异常
                    LOG.error("{} wait timeout for {}ms, current pending actions are: {}", this, writeWaitTimeoutMs, pendingActions);
                    throw new TimeoutException("still has pending actions");
                }
            } catch (Exception e) {
                writeException = e;
                running = false;
                LOG.error("{} write error", WriteContextImpl.this, e);
            } finally {
                try {
                    // 清理
                    running = false;
                    factory.cleanUp(fileHandle, WriteContextImpl.this);
                    CURRENT_WRITE_TASK_GAUGE.dec();
                    IOUtils.cleanupWithLogger(LOG, out);
                } finally {
                    PendingCommit pendingCommit = finalCommit.get();
                    if (pendingCommit != null) {
                        pendingCommit.hook().completeExceptionally(new TimeoutException("file commit timeout"));
                    }
                    cleanUp(pendingActions);
                    pendingActions.clear();
                    cleanUp(unorderedActions);
                    unorderedActions.clear();
                }
            }
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanUp(Collection<PendingAction> pendingActions) {
        if (!pendingActions.isEmpty()) {
            for (PendingAction pendingAction : pendingActions) {
                try {
                    if (pendingAction instanceof PendingCommit commit) {
                        commit.hook().completeExceptionally(new TimeoutException("write context cleanup"));
                    } else if (pendingAction instanceof PendingWrite write) {
                        PENDING_WRITE_BYTES_GAUGE.dec(write.count());
                        pendingWriteBytes.getAndAdd(-write.count());
                        write.release();
                    } else {
                        LOG.error("Unexpected {}", pendingAction);
                    }
                } catch (Exception e) {
                    LOG.error("Unexpected error", e);
                }
            }
        }
    }

    private void doWrite(PendingWrite write) throws IOException {
        try {
            int count = write.count();
            if (write.offset() < out.getPos()) {
                // 写入偏移量小于预期写入偏移量，说明有重复写，
                String fileIdPath = Utils.getFileIdPath(fileHandle.fileId());
                long writeOffset = write.offset();
                if (writeOffset == 0 && firstWrite) {
                    // 写偏移量为0，直接覆盖文件从头开始写
                    LOG.warn("{} receive {}, start truncate", this, write);
                    out.close();
                    dfsClient.truncate(fileIdPath, 0);
                    out = dfsClient.append(
                            fileIdPath,
                            dfsClient.getConf().getIoBufferSize(),
                            EnumSet.of(CreateFlag.APPEND),
                            null,
                            null
                    );
                    commitOffset = out.getPos();
                } else {
                    // 提交当前缓冲数据后进行数据比对
                    doCommit(AppConfig.WriteCommitPolicy.HSYNC);
                    try (DFSInputStream in = dfsClient.open(fileIdPath)) {
                        if (writeOffset + count <= out.getPos()) {
                            // 完全重复写，如果数据比对完全一致那么视作成功写入直接返回
                            checkRepeatWrite(in, count, write);
                            return;
                        } else {
                            // 部分重复写，部分新数据，如果重复写数据比对完全一致就写入新数据
                            int off = (int) (out.getPos() - writeOffset);
                            checkRepeatWrite(in, off, write);
                            // 切片，只写未重复写的部分
                            write = write.slice(count - off);
                        }
                    }
                }
            }
            // 写入数据
            write.writeTo(out);
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} write data {}", this, write);
            }
        } finally {
            write.release();
        }
    }

    /**
     * 重复写检查
     */
    private void checkRepeatWrite(DFSInputStream in, int count, PendingWrite write) throws IOException {
        long startOffset = write.offset();
        long endOffset = startOffset + count;
        if (in.getFileLength() < endOffset && write.offset() > 0) {
            throw new IllegalArgumentException("file len is less than " + endOffset);
        }
        LOG.warn("{} receive repeatable write from offset {} to {}", this, startOffset, endOffset);
        if (!write.dataEquals(in, count)) {
            throw new IOException("different write data");
        }
    }

    /**
     * 提交数据
     */
    private void doCommit(PendingCommit commit) throws IOException {
        try {
            if (commit.offset() + commit.count() > commitOffset || (commit.offset() == 0 && commit.count() == 0)) {
                doCommit(WRITE_COMMIT_POLICY);
            }
            commit.hook().complete(null);
        } catch (Exception e) {
            commit.hook().completeExceptionally(e);
            throw e;
        }
    }

    private void doCommit(AppConfig.WriteCommitPolicy commitPolicy) throws IOException {
        if (commitOffset == out.getPos()) {
            return;
        }
        switch (commitPolicy) {
            case FLUSH -> out.flush();
            case HSYNC -> out.hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
            case HFLUSH -> out.hflush();
            default -> throw new IllegalArgumentException("illegal type " + WRITE_COMMIT_POLICY);
        }
        commitOffset = out.getPos();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        formatTo(builder);
        return builder.toString();
    }

    @Override
    public void formatTo(StringBuilder buffer) {
        buffer.append("WriteContext{handle=");
        fileHandle.formatTo(buffer);
        buffer.append(", pos=")
                .append(out.getPos())
                .append(", running=")
                .append(running)
                .append('}');
    }
}
