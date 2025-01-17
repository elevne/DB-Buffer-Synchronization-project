package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BufferMgr {
   private List<Buffer> bufferList;
   private Map<BlockId, Buffer> bufferMap;

   private static final long MAX_TIME = 10000;

   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
      bufferList = new LinkedList<>();
      bufferMap = new HashMap<>();
      for (int i = 0; i < numbuffs; i++)
         bufferList.add(new Buffer(fm, lm));
   }

   public synchronized int available() {
      return bufferList.size();
   }

   public synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferMap.values())
         if (buff.modifyingTx() == txnum)
            buff.flush();
   }

   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
         bufferList.add(buff);
         notifyAll();
      }
   }

   public synchronized Buffer pin(BlockId blk) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = tryToPin(blk);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = tryToPin(blk);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      } catch (InterruptedException e) {
         throw new BufferAbortException();
      }
   }

   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }

   private Buffer tryToPin(BlockId blk) {
      Buffer buff = bufferMap.get(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         bufferMap.put(blk, buff);
      }
      if (!buff.isPinned())
         bufferList.remove(buff);
      buff.pin();
      return buff;
   }

   private Buffer chooseUnpinnedBuffer() {
      if (bufferList.isEmpty())
         return null;
      else
         return bufferList.remove(0);
   }

   public synchronized void printStatus() {
      System.out.println("Allocated Buffers:");
      for (Buffer buff : bufferMap.values()) {
         System.out.println("Buffer " + buff.block().number() + ": [file " + buff.block().fileName() + ", block " + buff.block().number() + "] " + (buff.isPinned() ? "pinned" : "unpinned"));
      }

      System.out.println("Unpinned Buffers in LRU order:");
      for (Buffer buff : bufferList) {
         System.out.print(buff.block().number() + " ");
      }
      System.out.println();
   }

}
