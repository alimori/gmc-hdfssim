package hdfs.replicationsimulator;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * A datanode is storing data and execute commands to transfer files.
 *
 *
 * @author peteratt
 * this class manipulate by Ali Mortazavi
 * @version 0.1
 */
public class Datanode extends Node {

    private int datacenterId;
    private int id;
    
    // ino badan kamel kon ali.
    private int processRequestPerSecond = 10;

    private long lastHB;

    private List<Block> blocks;

    /* 
     * Status
     */
    private long time_up;
    private long time_down;
    private boolean failed;

    private List<Event> commandQueue;

    private Queue<Block> pendingBlocks;

    public boolean blockChecking = false;

    public Datanode(int datacenterId, int id, int capacity) {
        this.datacenterId = datacenterId;
        this.id = id;
        this.blocks = new ArrayList<>();
        this.lastHB = Node.now() - 3000;
        this.commandQueue = new ArrayList<>();
        this.pendingBlocks = new ConcurrentLinkedQueue<>();
    }

    int getId() {
        return this.id;
    }

    public int getDatacenterId() {
        return datacenterId;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setDatacenterId(int datacenterId) {
        this.datacenterId = datacenterId;
    }

    /**
     * Adds a command to the FIFO queue of the NameNode
     *
     * @param e
     * @return
     */
    public boolean addCommand(Event e) {
        commandQueue.add(e);
        return true;
    }

    /*
     * Add a block to the Local list of Nodes (Local)
     */
    public void addBlock(Block block) {
        blocks.add(block);
    }

    public void removeBlock(Block block) {
        blocks.remove(block);
    }

    boolean hasFailed() {
        return failed;
    }

    public long getLastHB() {
        return lastHB;
    }

    public void setLastHB(long lastHB) {
        this.lastHB = lastHB;
    }

    public void setUploadingTime(long time) {
        this.time_up = time;
    }

    public long getUploadingTime() {
        return this.time_up;
    }

    public void setDownloadingTime(long time) {
        this.time_down = time;
    }

    public long getDownloadingTime() {
        return this.time_down;
    }

    /*
     public void setPendingBlock(int idBlock) {
     Block b = findBlockById(idBlock);
     pendingBlocks.add(b);
     if (blockChecking) {
     Thread t = new Thread(new BlockChecker());
     t.start();
     }
     }
	
     private Block findBlockById(int idBlock) {
     Block b = Simulator.getNamenode().getBlocksMap().getBlockInfo(idBlock);
     return b;
     }

     */
    public boolean kill() {
        long now = Node.now();
        for (Block block : blocks) {
            block.setFailTime(now);
        }
        this.failed = true;
        return this.hasFailed();
    }

    Block getBlock(int dataitemId, int blockId) {
        for (Block block : blocks) {
            if (block.getDataitemId() == dataitemId && block.getId() == blockId) {
                return block;
            }
        }
        return null;
    }

}
