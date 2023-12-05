package simpledb.tx.concurrency;

import simpledb.file.BlockId;
import simpledb.tx.concurrency.LockAbortException;

import java.util.*;

class LockTable {
   private static final long MAX_TIME = 10000;

   private Map<BlockId, List<Integer>> locks = new HashMap<>();
   private ThreadLocal<Integer> currentTxId = new ThreadLocal<>();

   private Map<Integer, Integer> blkIdTxId = new HashMap<>();

   public synchronized void sLock(BlockId blk) {
      try {
         int txId = generateTransactionId();
         //currentTxId.set(txId);
         blkIdTxId.put(blk.number(), txId);
         long timestamp = System.currentTimeMillis();
         while (hasXlock(blk) && !waitingTooLong(timestamp)) {
            checkAbort(txId);
            wait(MAX_TIME);
         }
         if (hasXlock(blk))
            throw new LockAbortException();
         List<Integer> txids = locks.computeIfAbsent(blk, k -> new ArrayList<>());
         //txids.add(getCurrentTxId());
         txids.add(txId);
      } catch (InterruptedException e) {
         throw new LockAbortException();
      } finally {
         currentTxId.remove();
      }
   }

   public synchronized void xLock(BlockId blk) {
      try {
         int txId = generateTransactionId();
         //currentTxId.set(txId);
         blkIdTxId.put(blk.number(), txId);
         long timestamp = System.currentTimeMillis();
         while (hasOtherSLocks(blk) && !waitingTooLong(timestamp)) {
            checkAbort(txId);
            wait(MAX_TIME);
         }
         if (hasOtherSLocks(blk))
            throw new LockAbortException();
         //locks.put(blk, Collections.singletonList(-getCurrentTxId()));
         locks.put(blk, Collections.singletonList(-txId));
      } catch (InterruptedException e) {
         throw new LockAbortException();
      } finally {
         currentTxId.remove();
      }
   }

   public synchronized void unlock(BlockId blk) {
      try {
         //int currentTxIdValue = getCurrentTxId();
         int currentTxIdValue = blkIdTxId.get(blk.number());
         List<Integer> txids = locks.get(blk);
         if (txids != null) {
            txids.remove(Integer.valueOf(currentTxIdValue));
            if (txids.isEmpty()) {
               locks.remove(blk);
               notifyAll();
            }
         }
      } catch (IllegalStateException e) {
         e.printStackTrace();
      }
   }

   private void checkAbort(int currentTxid) {
      //int currentTxid = getCurrentTxId();
      for (List<Integer> txids : locks.values()) {
         for (int heldTxid : txids) {
            if (heldTxid < 0 && heldTxid < currentTxid) {
               //System.out.println("Transaction " + getCurrentTxId() + " aborts");
               throw new LockAbortException();
            }
         }
      }
   }

   private int getCurrentTxId() {
      Integer txId = currentTxId.get();
      if (txId == null) {
         throw new IllegalStateException("Transaction ID not set for the current thread.");
      }
      return txId;
   }

   private int generateTransactionId() {
      // You may implement your logic to generate unique transaction IDs
      return new Random().nextInt(1000) + 1; // For simplicity, using a random number here
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
