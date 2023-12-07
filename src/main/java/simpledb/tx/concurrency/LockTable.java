package simpledb.tx.concurrency;

import simpledb.file.BlockId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class LockTable {
   private Map<BlockId, List<Integer>> locks = new HashMap<>();
   private Map<Integer, Long> timestamps = new HashMap<>();
   private long MAX_TIME = 10000;

   public synchronized void sLock(BlockId blk, int txid) {
      try {
         long timestamp = System.currentTimeMillis();
         timestamps.put(txid, timestamp);
         while (hasXlock(blk) && !waitingTooLong(timestamp)) {
            checkAbort(txid, blk, timestamp);
            wait(100);
         }
         if (hasXlock(blk))
         {
            System.out.println(blk.toString() + " failed to get S lock");
            throw new LockAbortException();}
         List<Integer> txids = locks.computeIfAbsent(blk, k -> new ArrayList<>());
         txids.add(txid);
      } catch (InterruptedException e) {
         throw new LockAbortException();
      }
   }

   public synchronized void xLock(BlockId blk, int txid) {
      try {
         long timestamp = System.currentTimeMillis();
         timestamps.put(txid, timestamp);
         while (hasOtherSLocks(blk) && !waitingTooLong(timestamp)) {
            checkAbort(txid, blk, timestamp);
            wait(100);
         }
         if (hasOtherSLocks(blk)){
            System.out.println(blk.toString() + " failed to get X lock");
            throw new LockAbortException();
         }
         locks.put(blk, Collections.singletonList(-txid));
      } catch (InterruptedException e) {
         throw new LockAbortException();
      }
   }

   public synchronized void unlock(BlockId blk, int txid) {
      List<Integer> txids = locks.get(blk);
      if (txids != null) {
         txids.remove(Integer.valueOf(txid));
         if (!txids.isEmpty())
            locks.remove(blk);
      }
      notifyAll();
   }

   private void checkAbort(int txid, BlockId blk, long requestingTimestamp) {
      List<Integer> txids = locks.get(blk);
      if (txids != null) {
         for (int heldTxid : txids) {
            Long heldTimestamp = timestamps.get(heldTxid);
            if (heldTimestamp != null && heldTimestamp < requestingTimestamp) {
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
