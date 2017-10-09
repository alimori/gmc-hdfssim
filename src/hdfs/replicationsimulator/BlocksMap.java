package hdfs.replicationsimulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Map to store information about blocks
 * 
 * @author in the main source of this class was not written name of author,
 * this class manipulate by Ali Mortazavi
 */
public class BlocksMap {

    private Map<List<Integer>, BlockInfo> map;
    private int datacenterId;

    /*
     * Constructor (Initialize the blocksMap)
     */
    BlocksMap(int datacenterId) {
        this.datacenterId = datacenterId;
        this.map = new HashMap<>();
    }

    /*
     * Add a block to the map
     */
//    public void addBlock(BlockInfo block) {
//        //Integer[] key = {block.getDataitemId(), block.getId()};
//        List<Integer> key = new ArrayList<>(Arrays.asList(block.getDataitemId(), block.getId()));
//        map.put(key, block);
//    }

    /**
     * Remove data-node reference from the block. Remove the block from the
     * block map only if it does not belong to any file and data-nodes.
     */
//    boolean removeNode(BlockInfo b, DatanodeInfo node) {
//        //BlockInfo info = map.get(b);
//        BlockInfo info = b;
//        if (info == null) {
//            System.out.println("BlockInfo is null.");
//            return false;
//        }
//
//        // remove block from the data-node block list (useless, but we never know) and the node from the block info 
//        //node.removeBlock(info);//TODO Temporary comment, to check==> PROVOKE FAILURE
//        boolean removed = info.removeDataNode(node);
//
//        /*
//         * if (info.getDatanode(0) == null // no datanodes left && info.inode ==
//         * null) { // does not belong to a file map.remove(b); // remove block
//         * from the map }
//         */
//        if (info.hasFailed()) {
//            Simulator.addTrace(new SimTrace(SimTrace.DATA_LOSS, info.getDataitemId(), info.getId()));
//        }
//        return removed;
//    }

//  
    
//
//    public int getcontainingNodes(BlockInfo block) {
//        return block.numberOfReplicas();
//
//    }
//
//    public BlockInfo getBlockInfo(int dataitemId2, int blockId2) {
//        //Integer[] key = {dataitemId2, blockId2};
//
//        List<Integer> key = new ArrayList<>(Arrays.asList(dataitemId2, blockId2));
//        return map.get(key);
//
//    }

}
