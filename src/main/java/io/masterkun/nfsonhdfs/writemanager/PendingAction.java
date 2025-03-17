package io.masterkun.nfsonhdfs.writemanager;

sealed interface PendingAction extends Comparable<PendingAction> permits PendingCommit,
        PendingWrite {
    long offset();

    int count();

    int self();

    @Override
    default int compareTo(PendingAction o) {
        long offsetDiff = offset() - o.offset();
        int countDiff;
        return offsetDiff == 0 ?
                (countDiff = count() - o.count()) == 0 ?
                        self() - o.self() :
                        countDiff :
                offsetDiff < 0 ?
                        -1 : 1;
    }
}
