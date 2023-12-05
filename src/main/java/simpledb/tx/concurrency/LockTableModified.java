package simpledb.tx.concurrency;

import simpledb.file.BlockId;

import java.util.*;

class LockTableModified {

    private static final long MAX_TIME = 10000; // 10 seconds
    private Map<BlockId, List<Integer>> locks = new HashMap<>();

    public synchronized void sLock(BlockId blk, int txid) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasXlock(blk) && !waitingTooLong(timestamp)) {
                checkAbort(txid);
                wait(MAX_TIME);
            }
            if (hasXlock(blk))
                throw new LockAbortException();
            List<Integer> txids = locks.computeIfAbsent(blk, k -> new ArrayList<>());
            txids.add(txid);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void xLock(BlockId blk, int txid) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasOtherSLocks(blk) && !waitingTooLong(timestamp)) {
                checkAbort(txid);
                wait(MAX_TIME);
            }
            if (hasOtherSLocks(blk))
                throw new LockAbortException();
            locks.put(blk, Collections.singletonList(-txid));
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(BlockId blk, int txid) {
        List<Integer> txids = locks.get(blk);
        if (txids != null) {
            txids.remove(Integer.valueOf(txid));
            if (txids.isEmpty())
                locks.remove(blk);
        }
        notifyAll();
    }

    private void checkAbort(int txid) {
        for (List<Integer> txids : locks.values()) {
            for (int heldTxid : txids) {
                if (heldTxid < 0 && heldTxid < txid) {
                    throw new LockAbortException();
                }
            }
        }
    }

    private boolean hasXlock(BlockId blk) {
        List<Integer> txids = locks.get(blk);
        return txids != null && txids.stream().anyMatch(txid -> txid < 0);
    }

    private boolean hasOtherSLocks(BlockId blk) {
        List<Integer> txids = locks.get(blk);
        return txids != null && txids.size() > 1;
    }

    private boolean waitingTooLong(long starttime) {
        return System.currentTimeMillis() - starttime > MAX_TIME;
    }
}
