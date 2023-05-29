package org.binara.sachin;

public interface DistributedTxListener {
    void onGlobalCommit();
    void onGlobalAbort();
}
