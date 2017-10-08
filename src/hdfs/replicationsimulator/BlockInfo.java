package hdfs.replicationsimulator;

/**
 * 
 * @author in the main source of this class was not written name of author,
 * this class manipulate by Ali Mortazavi
 */
public class BlockInfo extends Block implements Comparable<BlockInfo> {

    //DatanodeInfo datanode; // oon datanode i ke in block toosh zakhire ast

    BlockInfo(int dataitemId, int id, int size) {
        super(dataitemId, id, size);
       // datanode = null;
    }

    /**
     * Add a node to the list of storage nodes
     */
//    public void addDatanode(DatanodeInfo datanodeInfo) {
//        datanode = datanodeInfo;
//    }
//
//    boolean removeDataNode(DatanodeInfo DatanodeInfo) {
//        datanode = null;
//        return true;
//    }
//
//    public DatanodeInfo getDatanode() {
//        return datanode;
//    }
    
    

    //*************************************************************************
    /**
     * Remove a node to the list of storage nodes
     */
//    boolean removeDataNode(DatanodeInfo DatanodeInfo) {
//        return datanodes.remove(DatanodeInfo);
//    }
    //****************************************************zzzzzzzzzzzzzzzzzzzzzzzzzzz
//    boolean hasFailed() {      
//        return datanodes.isEmpty();
//    }
    
//
//    int numberOfReplicas() {
//        return datanodes.size();
//    }
//
//    public Iterator<DatanodeInfo> nodeIterator() {
//        return datanodes.iterator();
//    }

    /**
     * {@inheritDoc}
     */
    public int compareTo(BlockInfo b) {
        if (b.getId() > this.getId()) {
            return 1;
        } else {
            return -1;
        }
    }

//    public List<DatanodeInfo> getContainingNodes() {
//        return datanodes;
//    }
//    public DatanodeInfo getNode(int datacenterId) {
//        for (DatanodeInfo datanodeInfo : datanodes) {
//            if (datanodeInfo.getDatacenterId() == datacenterId) {
//                return datanodeInfo;
//            }
//        }
//        return null;
//    }
//    public long getFailTime() {
//        return failTime;
//    }
//
//    public void setFailTime(long failTime) {
//        this.failTime = failTime;
//    }
}
