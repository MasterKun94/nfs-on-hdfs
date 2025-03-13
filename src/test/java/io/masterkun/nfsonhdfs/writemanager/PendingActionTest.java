package io.masterkun.nfsonhdfs.writemanager;


import io.masterkun.nfsonhdfs.util.AppConfig;
import io.masterkun.nfsonhdfs.util.Utils;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;

public class PendingActionTest {

    public static void main(String[] args) {
        PriorityBlockingQueue<PendingAction> queue = new PriorityBlockingQueue<>();
        Utils.init(new AppConfig());
        queue.add(ByteBufferPendingWrite.get(100, 100, ByteBuffer.allocate(100)));
        queue.add(ByteArrayPendingWrite.get(100, 100, new byte[100]));
        queue.add(new PendingCommit(100, 100, new CompletableFuture<>()));
        for (PendingAction pendingAction : queue) {
            System.out.println(pendingAction);
        }
    }

}
