package io.masterkun.nfsonhdfs.writemanager;

import java.util.concurrent.CompletableFuture;

record PendingCommit(long offset, int count,
                     CompletableFuture<Void> hook) implements PendingAction {
    @Override
    public int self() {
        return 1;
    }
}
