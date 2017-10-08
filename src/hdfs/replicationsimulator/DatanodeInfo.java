package hdfs.replicationsimulator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This is the DataInfo that the namenode stores
 * 
 * @author in the main source of this class was not written name of author,
 * this class manipulate by Ali Mortazavi
 */
public class DatanodeInfo {

    private int datacenterId;
    private int capacity;
    private int remainCapacity;
    private int id;
    private long lastHB;
    private List<BlockInfo> blocks;

    private List<Event> commands;
    Object datanodeLock = new Object();

    public DatanodeInfo(int datacenterId, int id, int capacity) {
        this.datacenterId = datacenterId;
        this.id = id;
        this.blocks = new ArrayList<BlockInfo>();
        this.capacity = capacity;
        this.remainCapacity = capacity;
        this.lastHB = Node.now(); //To correct problems at startup
        this.commands = new ArrayList<Event>();
    }

    int getId() {
        return this.id;
    }

    public int getDatacenterId() {
        return datacenterId;
    }

    public void setDatacenterId(int datacenterId) {
        this.datacenterId = datacenterId;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getLastHB() {
        return lastHB;
    }

    public void setLastHB(long lastHB) {
        this.lastHB = lastHB;
    }

    public void addBlock(BlockInfo block) {
        remainCapacity -= block.getSize();
        blocks.add(block);

    }

    public boolean removeBlock(BlockInfo block) {
        remainCapacity += block.getSize();
        return blocks.remove(block);
    }

    // aval in ziri bood bad man an ra taghir dadam va BlockInfo kardam
//    public boolean removeBlock(Block block) {
//        remainCapacity += block.getSize();
//        return blocks.remove(block);
//    }
    Iterator<BlockInfo> getBlockIterator() {
        return this.blocks.iterator();
    }

    List<BlockInfo> getBlockList() {
        return blocks;
    }

    public int getCapacity() {
        return this.capacity;
    }

    public int getRemainCapacity() {
        return remainCapacity;
    }

    public void setRemainCapacity(int remainCapacity) {
        this.remainCapacity = remainCapacity;
    }

    public boolean isGoodTarget(int blockSize) {
        //return (this.getCapacity() >= blockSize && !containingNodes.contains(this.id));
        //return (this.getCapacity() >= blockSize && !(this.containNode(containingNodes)));

        //return (this.remainCapacity() >= blockSize);
        return (this.getRemainCapacity() >= blockSize);
    }

    public void addBlockToBeReplicated(BlockInfo block, DatanodeInfo target) {

        commands.add(new Event(this.datacenterId, this.id, Event.REPLICATION, 0L, target.getDatacenterId(),
                target.getId(), block.getDataitemId(), block.getId(), 1, -1));

    }

    public List<Event> getCommands() {
        return commands;
    }

//    public int remainCapacity() {
//        int allBlocksSiez = 0;
//        for (BlockInfo blockInfo : blocks) {
//            allBlocksSiez += blockInfo.getSize();
//        }
//        return capacity - allBlocksSiez;
//    }
    public boolean containNode(List<DatanodeInfo> datanodeInfos) {
        for (DatanodeInfo datanodeInfo : datanodeInfos) {
            if (this.datacenterId == datanodeInfo.datacenterId && this.id == datanodeInfo.id) {
                return true;
            }
        }
        return false;
    }
}
