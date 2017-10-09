package hdfs.replicationsimulator;

import gmc_hdfs.replicationsimulator.Datacenter;
import gmc_hdfs.replicationsimulator.DatacentersTopology;
import gmc_hdfs.replicationsimulator.Dataitem;
import gmc_hdfs.replicationsimulator.DataitemInfo;
import gmc_hdfs.replicationsimulator.DataitemsMap;
import gmc_hdfs.replicationsimulator.Request;
import gmc_hdfs.replicationsimulator.RequestType;
import gmc_hdfs.replicationsimulator.User;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Random;

/**
 *
 * This node store the metadata, the blocks locality and handle the replication
 * system
 *
 * @author Corentone
 * this class manipulate by Ali Mortazavi
 * @version 0.1
 */
public class Namenode extends Node {

    int datacenterId;

    // Map every block
    private DatanodesMap DatanodesMap;
    //private BlocksMap BlocksMap;
    private DataitemsMap DataitemsMap;

    private List<DatanodeInfo> heartbeats;
    private Queue<Event> toNamenode;
    private Queue<Event> toDatanodes;
    private Queue<Request> requests;

    Object datanodeLock = new Object();
    Object requestLock = new Object();

    public Daemon hbthread = null; // HeartbeatMonitor thread
    public Daemon replthread = null; // inside datacenter Replication thread for repair failures
    public Daemon communicationthread = null; // Replication thread
    public Daemon requestThread = null; // request thread

    boolean isRunning = false;

//    private UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks(Simulator.getNumberofReplicas());
    private UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks();
    private PendingReplicationBlocks pendingReplications;

    // Duplicated in AllDatanode TODO
    private static final int BLOCK_SIZE = Simulator.getBlockSize(); // *1000 ro vardashtam
    // heartbeatExpireInterval is how long namenode waits for datanode to report
    // heartbeat
    private long heartbeatExpireInterval = Simulator.getHeartbeat() * 2 + 200;
    // interreplicationRecheckInterval is how often namenode checks for new
    // replication work ... man esme in ro mizaram repair kardan failure ha, na replication
    private long interreplicationRecheckInterval = 2 * 1000L;
    // heartbeatRecheckInterval is how often namenode checks for expired datanodes
    private long heartbeatRecheckInterval = Math.round((Simulator.getHeartbeat() / 5));
    //Communication recheck interval
    private long communicationRecheckInterval = 500L;

//    private final int neededReplicas = Simulator.getNumberofReplicas();
    private int replIndex = 0;
    private long missingBlocksInCurIter = 0;
    private long missingBlocksInPrevIter = 0;
    volatile long pendingReplicationBlocksCount = 0L;
    volatile long corruptReplicaBlocksCount = 0L;
    volatile long underReplicatedBlocksCount = 0L;
    volatile long scheduledReplicationBlocksCount = 0L;
    volatile long excessBlocksCount = 0L;
    volatile long pendingDeletionBlocksCount = 0L;

    volatile long distanceUserFromData = 0L;
    volatile long numberOfRead = 0L;
    volatile long numberOfReplicaRead = 0L;

    Random r = new Random();

    // field haye ziro man neveshtam:
    boolean canTransfer = false;
    //Datacenter datacenter;


    /*
     * Constructor, Initialize threads and Maps
     */
    public Namenode(int datacenterId) {
        this.datacenterId = datacenterId;

        DatanodesMap = new DatanodesMap(datacenterId);
        //BlocksMap = new BlocksMap(datacenterId);
        DataitemsMap = new DataitemsMap(datacenterId);

        heartbeats = DatanodesMap.getHeartbeats();

        toNamenode = Simulator.getToNamenode(datacenterId);
        toDatanodes = Simulator.getToDatanodes(datacenterId);
        requests = Simulator.getRequests(datacenterId);

        this.hbthread = new Daemon(new HeartbeatMonitor());
        System.out.println("Namenode #" + datacenterId + " HeartbeatMonitor is new() . . .");

        this.replthread = new Daemon(new InterReplicationMonitor());
        System.out.println("Namenode #" + datacenterId + " ReplicationMonitor is new() . . .");

        this.communicationthread = new Daemon(new CommunicationMonitor());
        System.out.println("Namenode #" + datacenterId + " CommunicationMonitor is new() . . .");

        // man ezafe kardam        
        this.requestThread = new Daemon(new RequestMonitor());
        System.out.println("Namenode #" + datacenterId + " RequestHandler is new() . . .");

        System.out.println("Namenode #" + datacenterId + " created.");

    }

    /*
     * Add a node to the namenode (mainly for Init)
     */
    void addNode(DatanodeInfo DatanodeInfo) {
        DatanodesMap.addDatanodeInfo(DatanodeInfo);
    }

    /*
     * Add a block to the namenode (mainly for Init)
     */
//    void addBlock(BlockInfo Block) {
//        BlocksMap.addBlock(Block);
//    }
    void addDataitem(DataitemInfo dataitemInfo) {
        DataitemsMap.addDataitem(dataitemInfo);
    }

    // in ra bayad benvisam va kamel konam.
    boolean transferDataitem(Dataitem dataitem, Datacenter destination) {

        return true;
    }

    /*
     * Start the namenode (only contains replication system)
     */
    public void start() {
        isRunning = true;
        communicationthread.start();
        pendingReplications = new PendingReplicationBlocks();
        hbthread.start();
        replthread.start();
        requestThread.start();
    }

    /**
     * Periodically calls heartbeatCheck().
     */
    class HeartbeatMonitor implements Runnable {

        /**
         */
        @Override
        public void run() {

            Simulator.addTrace(new SimTrace("Namenode-" + datacenterId + " heartbeat monitor created"));

            while (isRunning) {
                try {
                    // TODO
                    heartbeatCheck();
                    Thread.sleep(heartbeatRecheckInterval);
                } catch (Exception e) {
                    // FSNamesystem.LOG.error(StringUtils.stringifyException(e));
                }
            }
        }
    }

    /**
     * Check if there are any expired heartbeats, and if so, whether any blocks
     * have to be re-replicated. While removing dead datanodes, make sure that
     * only one datanode is marked dead at a time within the synchronized
     * section. Otherwise, a cascading effect causes more datanodes to be
     * declared dead.
     */
    void heartbeatCheck() {
        boolean allAlive = false;
        while (!allAlive) {
            boolean foundDead = false;
            int nodeID = -1;
            int datacenterID = -1; ////

            // locate the first dead node.
            synchronized (heartbeats) {
                for (DatanodeInfo nodeInfo : heartbeats) {
                    if (isDatanodeDead(nodeInfo)) {
                        foundDead = true;
                        nodeID = nodeInfo.getId();
                        datacenterID = nodeInfo.getDatacenterId(); ////
                        break;
                    }
                }
            }

            // acquire the fsnamesystem lock, and then remove the dead node.
            if (foundDead) {
                synchronized (this) {

                    synchronized (DatanodesMap) {
                        synchronized (heartbeats) {
                            DatanodeInfo nodeInfo = null;
                            //Integer[] key = {datacenterID, nodeID};
                            nodeInfo = DatanodesMap.getDatanodeInfo(datacenterID, nodeID);

                            //System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAA :   " + datacenterID + "-" + nodeID);
                            if (nodeInfo != null && isDatanodeDead(nodeInfo)) {

                                //System.out.println("BBBBBBBBBBBBBBBBBBBBB :   " + datacenterID + "-" + nodeID);
                                /*
                                 * NameNode.stateChangeLog.info(
                                 * "BLOCK* NameSystem.heartbeatCheck: " +
                                 * "lost heartbeat from " + nodeInfo.getName());
                                 */
                                heartbeats.remove(nodeInfo);

                                long failDetectTime = Node.now();
                                synchronized (Simulator.getTraceList()) {
                                    Simulator.addTrace(new SimTrace(
                                            SimTrace.FAILURE_DETECTION, nodeInfo.getDatacenterId(), nodeInfo
                                            .getId(), failDetectTime));
                                }
                                removeDatanode(nodeInfo, failDetectTime);
                                //System.out.println("CCCCCCCCCCCCCCCCCCCCCCCCCC :   " + datacenterID + "-" + nodeID);

                            }

                        }
                    }
                }

            }
            allAlive = !foundDead;
        }
    }

    private void removeDatanode(DatanodeInfo nodeInfo, long lastFailDetectTime) {

        int numberOfFailBlocks = nodeInfo.getBlockList().size();
        Simulator.addFailDatanode(nodeInfo, numberOfFailBlocks);

        for (BlockInfo block : nodeInfo.getBlockList()) {
            block.setFailTime(lastFailDetectTime);
            block.setFailNodeInfo(nodeInfo);
            //block.removeDataNode(nodeInfo);
            //System.out.println("22222222244444444444444499999999999999");
            removeStoredBlock(block, nodeInfo);
            //System.out.println("22222222255555555555555555555551111111111");
        }

        //System.out.println("22222222255555555555555554444444444444");
        DatanodesMap.removeDatanode(nodeInfo);
        //System.out.println("22222222255555555555555555566666666666666");
        /*
         for (Iterator<BlockInfo> it = nodeInfo.getBlockIterator(); it.hasNext();) {
         removeStoredBlock(it.next(), nodeInfo);
         }
         */
    }

    private boolean isDatanodeDead(DatanodeInfo node) {
        return (node.getLastHB() < (Node.now() - heartbeatExpireInterval));
    }

    /**
     * Periodically calls computeReplicationWork().
     */
    class InterReplicationMonitor implements Runnable {

        static final int INVALIDATE_WORK_PCT_PER_ITERATION = 32;
        static final float REPLICATION_WORK_MULTIPLIER_PER_ITERATION = 2;

        @Override
        public void run() {
            while (isRunning) {

                computeDatanodeWork();
                processPendingReplications();
                try {
                    Thread.sleep(interreplicationRecheckInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private void processPendingReplications() {
            List<BlockInfo> timedOutItems = pendingReplications.getTimedOutBlocks();
            if (timedOutItems != null) {
                synchronized (this) {
                    for (BlockInfo blockInfo : timedOutItems) {
                        neededReplications.add(blockInfo);
                    }
                }
                /*
                 * If we know the target datanodes where the replication
                 * timedout, we could invoke decBlocksScheduled() on it. Its ok
                 * for now.
                 */
            }

        }
    }

    public int computeDatanodeWork() {
        int workFound = 0;
        int blocksToProcess = 0;
        int nodesToProcess = 0;
        // blocks should not be replicated or removed if safe mode is on

        synchronized (heartbeats) {
            blocksToProcess = (int) (heartbeats.size() * InterReplicationMonitor.REPLICATION_WORK_MULTIPLIER_PER_ITERATION);
            nodesToProcess = (int) Math.ceil((double) heartbeats.size()
                    * InterReplicationMonitor.INVALIDATE_WORK_PCT_PER_ITERATION
                    / 100);
        }
        //System.out.println("####################computeReplicationWork");
        workFound = computeReplicationWork(blocksToProcess);
        //System.out.println("####################computeReplicationWork 1111111:  " + workFound);
        // Update FSNamesystemMetrics counters
        synchronized (this) {
            pendingReplicationBlocksCount = pendingReplications.size();
            underReplicatedBlocksCount = neededReplications.size();
            scheduledReplicationBlocksCount = workFound;
        }

        return workFound;
    }

    /**
     * Scan blocks in {@link #neededReplications} and assign replication work to
     * data-nodes they belong to.
     *
     * The number of process blocks equals either twice the number of live
     * data-nodes or the number of under-replicated blocks whichever is less.
     *
     * @return number of blocks scheduled for replication during this iteration.
     */
    private int computeReplicationWork(int blocksToProcess) {
        // Choose the blocks to be replicated
        List<BlockInfo> blocksToReplicate = chooseUnderReplicatedBlocks(blocksToProcess);

        // replicate blocks
        int scheduledReplicationCount = 0;

        for (BlockInfo block : blocksToReplicate) {
            if (computeReplicationWorkForBlock(block)) {
                scheduledReplicationCount++;
            }
        }

        return scheduledReplicationCount;
    }

    /**
     * Get a list of block lists to be replicated The index of block lists
     * represents the
     *
     * @param blocksToProcess
     * @return Return a list of block lists to be replicated. The block list
     * index represents its replication priority.
     */
    synchronized List<BlockInfo> chooseUnderReplicatedBlocks(int blocksToProcess) {
        // initialize data structure for the return value
        List<BlockInfo> blocksToReplicate = new ArrayList<>();

        synchronized (neededReplications) {
            if (neededReplications.size() == 0) {
                missingBlocksInCurIter = 0;
                missingBlocksInPrevIter = 0;
                return blocksToReplicate;
            }

            // Go through all blocks that need replications.
            Iterator<BlockInfo> neededReplicationsIterator = neededReplications.iterator();

            // skip to the first unprocessed block, which is at replIndex
            for (int i = 0; i < replIndex && neededReplicationsIterator.hasNext(); i++) {
                neededReplicationsIterator.next();
            }
            // # of blocks to process equals either twice the number of live
            // data-nodes or the number of under-replicated blocks whichever is
            // less

            blocksToProcess = Math.min(blocksToProcess, neededReplications.size());

            for (int blkCnt = 0; blkCnt < blocksToProcess; blkCnt++, replIndex++) {
                if (!neededReplicationsIterator.hasNext()) {
                    // start from the beginning
                    replIndex = 0;
                    missingBlocksInPrevIter = missingBlocksInCurIter;
                    missingBlocksInCurIter = 0;
                    blocksToProcess = Math.min(blocksToProcess, neededReplications.size());
                    if (blkCnt >= blocksToProcess) {
                        break;
                    }
                    neededReplicationsIterator = neededReplications.iterator();
                    assert neededReplicationsIterator.hasNext() : "neededReplications should not be empty.";
                }

                BlockInfo block = neededReplicationsIterator.next();
//                int priority = ((BlockIterator) neededReplicationsIterator)
//                        .getPriority();
//                if (priority < 0 || priority >= blocksToReplicate.size()) {
//                    // LOG.warn("Unexpected replication priority: " + priority +
//                    // " " + block);
//                } else {
                blocksToReplicate.add(block);
//                }
            } // end for
        } // end synchronized
        return blocksToReplicate;
    }

    /**
     * Replicate a block
     *
     * @param block block to be replicated
     * @param priority a hint of its priority in the neededReplication queue
     * @return if the block gets replicated or not
     */
    boolean computeReplicationWorkForBlock(BlockInfo block) {
        int requiredReplication;
        int numEffectiveReplicas;
        List<DatanodeInfo> containingNodes;
        DatanodeInfo srcNode;

        synchronized (this) {
            synchronized (neededReplications) {
                containingNodes = new ArrayList<>(); //

                DataitemInfo dataitemInfo = DataitemsMap.getDataitemInfo(block.getDataitemId());
                containingNodes = dataitemInfo.getContainingNodes(block);

                requiredReplication = 1;
//                requiredReplication = neededReplicas - containingNodes.size();

                //System.out.println("########## contaning nodes for block #" + block.getDataitemId() + "_" + block.getId()
                //   + "      size= " + containingNodes.size());
                // get a source data-node
                //containingNodes = new ArrayList<DatanodeInfo>(); 
                int numReplicas = 0;// Useless in our SIMULATOR
                srcNode = chooseSourceDatanode(block, containingNodes);

                //System.out.println("#########Sourc node #" + srcNode.getDatacenterId() + "_" + srcNode.getId() + "   sssss . . ..");
                if (numReplicas <= 0) {
                    missingBlocksInCurIter++;
                }
                if (srcNode == null) // block can not be replicated from any node
                {
                    //System.out.println(".#####Error:... Line 444 Namenode ..####  DATA LOSS.");
//                    Simulator.addTrace(new SimTrace(SimTrace.DATA_LOSS, block.getDataitemId(),
//                            block.getId()));
                    //return true;
                    return false;
                }

                // do not schedule more if enough replicas is already pending
                numEffectiveReplicas = pendingReplications.getReplica(block);

                if (numEffectiveReplicas >= requiredReplication) {
                    neededReplications.remove(block); // remove from
                    // neededReplications
                    replIndex--;
                    /*
                     * NameNode.stateChangeLog.info("BLOCK* " +
                     * "Removing block " + block +
                     * " from neededReplications as it has enough replicas.");
                     */
                    //System.out.println("@@@@@@@@@@ return false . . .");
                    return false;
                }
            }
        }

        // choose replication targets: NOT HODING THE GLOBAL LOCK
        //System.out.println("@@@@@@@@@@@@@ before targets . . . . ..........................");
        DatanodeInfo target = chooseTarget(DatanodesMap.getHeartbeats(), BLOCK_SIZE);
        //System.out.println("%%%%%%%%%%%  target size: :::::::::::::::::::::::::::::::::::::::::" + targets.length);

        if (target == null) {
            System.out.println("%%%%%%%%%%%%%%%%% target is null");
            return false;
        }

        synchronized (this) {
            synchronized (neededReplications) {
                // do not schedule more if enough replicas is already pending
                //int numReplicas = BlocksMap.getcontainingNodes(block);
                //int numReplicas = DataitemsMap.getDataitemInfo(block.getDataitemId()).getContainingNodes(block).size();

                numEffectiveReplicas = pendingReplications.getReplica(block);
                if (numEffectiveReplicas >= 1) {
                    neededReplications.remove(block); // remove from
                    // neededReplications
                    replIndex--;

                    /*
                     * NameNode.stateChangeLog.info("BLOCK* " +
                     * "Removing block " + block +
                     * " from neededReplications as it has enough replicas.");
                     */
                    return false;
                }

                // Add block to the to be replicated list
                // @TODO
                srcNode.addBlockToBeReplicated(block, target);

                // Move the block-replication into a "pending" state.
                // The reason we use 'pending' is so we can retry
                // replications that fail after an appropriate amount of time.
                pendingReplications.add(block);
                /*
                 * NameNode.stateChangeLog.debug( "BLOCK* block " + block +
                 * " is moved from neededReplications to pendingReplications");
                 */

                // remove from neededReplications
                if (numEffectiveReplicas >= 0) {
                    neededReplications.remove(block); // remove from
                    // neededReplications
                    replIndex--;
                }
            }
        }

        return true;
    }

    private DatanodeInfo chooseTarget(List<DatanodeInfo> Nodes, int blockSize) {

//        DatanodeInfo[] nodes = new DatanodeInfo[numReplicas];
        DatanodeInfo chosen;
        //nodes[0] = null;
//        numReplicas = (numReplicas > DatanodesMap.getHeartbeats().size()) ? DatanodesMap
//                .getHeartbeats().size() : numReplicas;
        chosen = DatanodesMap.mostRemainCapacieyNode(Nodes);

        //    if (chosen.isGoodTarget(blockSize)) {
        return chosen;
     //   }

        //    return null;
//        int i = 0;
//        do {
//            //chosen = DatanodesMap.randomNode();  
//            chosen = DatanodesMap.mostRemainCapacieyNode(containingNodes); // -containingNodes
//            //System.out.println("choooooooooooooooooooooooooooooooosen:"+chosen.getDatacenterId()+"_"+chosen.getId());
//            if (chosen.isGoodTarget(blockSize)) {
//                //System.out.println("iiiiiiiiiiiiiiiiiiffffffffffffffff: " +numReplicas);
//                numReplicas--;
//                nodes[i] = chosen;
//                i++;
//            }
//        } while (numReplicas > 0 || chosen == null);
        //System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$: "+nodes.length);
    }

    /**
     * Parse the data-nodes the block belongs to and choose one, which will be
     * the replication source.
     *
     * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
     * since the former do not have write traffic and hence are less busy. We do
     * not use already decommissioned nodes as a source. Otherwise we choose a
     * random node among those that did not reach their replication limit.
     *
     * In addition form a list of all nodes containing the block and calculate
     * its replication numbers.
     *
     * DESCRIPTION IS NOT REVELEVANT IN SIMULATOR
     */
    private DatanodeInfo chooseSourceDatanode(BlockInfo block, List<DatanodeInfo> containingNodes) {
        //containingNodes.clear();
        DatanodeInfo srcNode = null;

        List<DatanodeInfo> containing = new ArrayList<>();
        for (DatanodeInfo datanodeInfo : containingNodes) {
            containing.add(datanodeInfo);
        }

        while (srcNode == null && !containing.isEmpty()) {
            int min = 10;
            DatanodeInfo datanode = null;
            for (DatanodeInfo datanodeInfo : containing) {
                if ((DatacentersTopology.getDistance(datacenterId, datanodeInfo.getDatacenterId())) < min) {
                    min = DatacentersTopology.getDistance(datacenterId, datanodeInfo.getDatacenterId());
                    datanode = datanodeInfo;
                }
            }
            if (!Simulator.getDatacentersTopology().getDatacenters().get(datanode.getDatacenterId())
                    .getNamenode().isDatanodeDead(datanode)) {
                srcNode = datanode;
            }
            containing.remove(datanode);
        }

        // halate zir Random ast
//        for (DatanodeInfo datanodeInfo : containingNodes) {
//            if (r.nextBoolean()) {
//                srcNode = datanodeInfo;
//            }
//            if (srcNode != null) {
//                return srcNode;
//            }
//        }
        //System.out.println("@@@@@@@@@ " + srcNode.getDatacenterId() + "_" + srcNode.getId() + "  ...");
        return srcNode;
    }

    /**
     * Periodically calls heartbeatCheck().
     */
    class CommunicationMonitor implements Runnable {

        /**
         */
        public void run() {
            while (isRunning) {
                try {
                    // TODO
                    CommunicationCheck();
                    Thread.sleep(communicationRecheckInterval);
                } catch (Exception e) {
                    // FSNamesystem.LOG.error(StringUtils.stringifyException(e));
                }
            }
        }
    }

    public void CommunicationCheck() {
        Event event;
        do {
            synchronized (toNamenode) {// Maybe the lock must be done on an
                // object
                event = toNamenode.poll();
            }

            if (event != null && !event.isHandle) {

                handleEvent(event);

            }

        } while (event != null);
    }

    private void handleEvent(Event event) {

        switch (event.getAction()) {
            case Event.HEARTBEAT:
                handleHeartbeat(event.getSourceDatacenter(), event.getSource(), event.getTime());
                event.setIsHandle(true);
                break;

            case Event.BLOCKRECEPTION:
                handleBlockReception(event.getSourceDatacenter(), event.getSource(), event.getDataitemId(),
                        event.getBlockId(), event.getIsFailBolck(), event.getCutDatacenter());
                event.setIsHandle(true);
                break;

            case Event.REMOVE_DATAITEM:
                handleRemoveDataitem(event.getDataitemId(), event.getSourceDatacenter());
                event.setIsHandle(true);
                break;

            case Event.BLOCKREMOVED:
                handleBlockRemoved(event.getSourceDatacenter(), event.getSource(),
                        event.getDataitemId(), event.getBlockId());
                event.setIsHandle(true);
                break;

            case Event.REPLICATE_DATAITEM:
                handleReplicateDataitem(event.getDataitemId(), event.getSourceDatacenter(),
                        event.getDestinationDatacenter(), event.getCutDatacenter());
                event.setIsHandle(true);
                break;

            case Event.BLOCKREADED:
                handleBlockReaded(event.getUser(), event.getDataitemId(), event.getBlockId(),
                        event.getSourceDatacenter());
                event.setIsHandle(true);
                break;
        }

    }

    private void handleBlockReaded(User user, int dataitemId, int blockId, int datacenter) {
        DataitemInfo dataitemInfo = DataitemsMap.getDataitemInfo(dataitemId);
        BlockInfo blockinfo = dataitemInfo.getBlockes().get(blockId);

    }

    private void handleBlockRemoved(int datacenterId, int datanodeId, int dataitemId, int blockId) {

        DataitemInfo dataitemInfo = DataitemsMap.getDataitemInfo(dataitemId);
        BlockInfo blockinfo = dataitemInfo.getBlockes().get(blockId);
        DatanodeInfo datanode = DatanodesMap.getDatanodeInfo(datacenterId, datanodeId);

        dataitemInfo.removeBlock(blockinfo, datacenterId);

        if (dataitemInfo.getBlockToDatanodeMap().get(datacenterId).isEmpty()) {
            dataitemInfo.removeDatacentersList(datacenterId);

            dataitemInfo.removeCapacity(datacenterId);

            dataitemInfo.getReadReplicaMap().remove(datacenterId);

            if ("copy".equals(dataitemInfo.getMasterCopy().get(datacenterId))) {
                dataitemInfo.getMasterCopy().remove(datacenterId);
            }
            if ("master".equals(dataitemInfo.getMasterCopy().get(datacenterId))) {
                dataitemInfo.getMasterCopy().remove(datacenterId);
                dataitemInfo.updateMasterCopy();
            }
            DataitemsMap.removeDataitem(dataitemId);
            System.out.println("Dataitem #" + dataitemId + " Removed from Datacenter #" + datacenterId);

        }
    }

    private void handleRemoveDataitem(int dataitemId, int datacenterId) {

        DataitemInfo dataitemInfo = DataitemsMap.getDataitemInfo(dataitemId);

        for (BlockInfo blockInfo : dataitemInfo.getBlockes()) {
            DatanodeInfo datanodeInfo = dataitemInfo.getDatanode(blockInfo, datacenterId);
            addToDatanodeQueue(new Event(Event.REMOVE_BLOCK, datacenterId, datanodeInfo.getId(),
                    dataitemId, blockInfo.getId()));
        }
    }

    private void handleReplicateDataitem(int dataitemId, int sourceDatacenterId,
            int destinationDatacenterId, int cutDatacenter) {
        DataitemInfo dataitemInfo = DataitemsMap.getDataitemInfo(dataitemId);

        for (BlockInfo blockInfo : dataitemInfo.getBlockes()) {

            DatanodeInfo datanodeInfo = dataitemInfo.getBlockToDatanodeMap().get(sourceDatacenterId).
                    get(blockInfo);

            addToDatanodeQueue(new Event(sourceDatacenterId, datanodeInfo.getId(), Event.REPLICATION,
                    Node.now(), destinationDatacenterId, datanodeInfo.getId(), dataitemId,
                    blockInfo.getId(), 0, cutDatacenter));  // in 0 yani be khatere failure nist va replicate ast
        }

    }

    private void handleBlockReception(int sourceDatacenter, int source, int dataitemId,
            int blockId, int isBlockFail, int cutDatacenter) {

        // yani block fail shode va baraye hamin replicate mishavad
        if (isBlockFail == 1) {

            DataitemInfo dataitemInfo = DataitemsMap.getDataitemInfo(dataitemId);
            BlockInfo blockinfo = dataitemInfo.getBlockes().get(blockId);
            //BlockInfo blockinfo = BlocksMap.getBlockInfo(dataitemId, blockId);
            DatanodeInfo datanode = DatanodesMap.getDatanodeInfo(sourceDatacenter, source);

            // Add the block to the Datanode list
            datanode.addBlock(blockinfo);

            // Add the datanode to the block list
            //blockinfo.addDatanode(datanode);
            dataitemInfo.addBlockToDatanodeMap(blockinfo, datanode);

            // delete the pendingReplication entry
            pendingReplications.remove(blockinfo);

            long receivedTime = Node.now();
            long ttr = 0L;
            if (blockinfo.getFailTime() != 0) { // yani agar fail shode bood
                ttr = receivedTime - blockinfo.getFailTime();
            }
            synchronized (Simulator.getTraceList()) {
                Simulator.addTrace(new SimTrace(SimTrace.BLOCK_RECEIVED, sourceDatacenter, source,
                        dataitemId, blockId, receivedTime, ttr));

                synchronized (Simulator.getFailDatanodes()) {
                    Simulator.blockRepeared(blockinfo.getFailNodeInfo());

                    if (Simulator.isRepearDatanode(blockinfo.getFailNodeInfo())) {
                        Simulator.addTrace(new SimTrace(SimTrace.DATANODE_REPEARED, blockinfo.getFailNodeInfo().
                                getDatacenterId(), blockinfo.getFailNodeInfo().getId(), ttr, Node.now()));
                    }
                }
            }
            blockinfo.setFailTime(0L);
            blockinfo.setFailNodeInfo(null);

        } // yani block fail nashode va bar asase tasmime Replication manager replicate mishavad
        else if (isBlockFail == 0) {

            DataitemInfo dataitemInfo = Simulator.getReplicationManager().getDataitemInfo(dataitemId);

            BlockInfo blockinfo = dataitemInfo.getBlockes().get(blockId);

            DatanodeInfo datanode = DatanodesMap.getDatanodeInfo(sourceDatacenter, source);

            Datacenter datacenter = Simulator.getDatacentersTopology().getDatacenters().
                    get(sourceDatacenter);

            // Add the block to the Datanode list
            datanode.addBlock(blockinfo);

            // Add the datanode to the block list
            dataitemInfo.addBlockToDatanodeMap(blockinfo, datanode);

            if (dataitemInfo.getBlockToDatanodeMap().get(sourceDatacenter).size()
                    == dataitemInfo.getBlockes().size()) {

                dataitemInfo.addToDatacentersList(datacenter);
                dataitemInfo.addToMasterCopyMap(datacenter.getId(), "copy");
                dataitemInfo.addToReplicaReadMap(datacenter.getId(), 0);
                dataitemInfo.setCapacity(datacenter.getId(), Simulator.getProcessCapacity());
                DataitemsMap.addDataitem(dataitemInfo);

                System.out.println("Dataitem #" + dataitemId + " placed in Datacenter #"
                        + sourceDatacenter);

                if (cutDatacenter != -1) {
                    Simulator.getToNamenode(cutDatacenter).add(new Event(
                            Event.REMOVE_DATAITEM, dataitemId, cutDatacenter));
                }

                //notify();
            }
            //  }
            //System.out.println("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
            //DataitemsMap.addDataitem(dataitemInfo);
        }

    }

    private void handleHeartbeat(int datacenterId, int id, long time) {
        DatanodeInfo datanode = DatanodesMap.getDatanodeInfo(datacenterId, id);
        //System.out.println("00000000000000");
        // System.out.println("Datanode #" + datanode.getDatacenterId() + "_" + datanode.getId() + "........");
        datanode.setLastHB(time);

        //System.out.println("yyyyyyyyyyyyyyy");
        // Give the commands to the datanodes
        synchronized (Simulator.getToDatanodes(datacenterId)) {//TODO CHECK THAT
            //System.out.println("sssssssssssssssss");
            for (Event command : datanode.getCommands()) {
                //System.out.println("xxxxxxxxxxxxxxxxxxx");
                this.addToDatanodeQueue(command);
                datanode.getCommands().remove(command); // man ezafe kardam
                //System.out.println("zzzzzzzzzzzzzzzzzzzzzz");
            }
        }
    }

    public void initAddBlock(Datanode dn, BlockInfo block) {
        DatanodeInfo datanode = DatanodesMap.getDatanodeInfo(dn.getDatacenterId(), dn.getId());
        datanode.addBlock(block);
        //block.addDatanode(datanode);
        DataitemInfo dataitemInfo = DataitemsMap.getDataitemInfo(block.getDataitemId());
        //dataitemInfo.addBlock(block);
        dataitemInfo.addBlockToDatanodeMap(block, datanode);

//        System.out.println("Block #" + block.getDataitemId() + "_" + block.getId() + "  ----  Datanode #"
//                + datanode.getDatacenterId() + "_" + datanode.getId() + "  ----  Remain Capacity: "
//                + datanode.getRemainCapacity());
    }

    /**
     * Modify (block-->datanode) map. Possibly generate replication tasks, if
     * the removed block is still valid.
     */
    synchronized void removeStoredBlock(BlockInfo block, DatanodeInfo node) {
        /*
         * NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
         * +block + " from "+node.getName());
         */

        if (!DataitemsMap.removeNode(block, node)) {
            //System.out.println("8888888888555555555555555555---555555555555");
            return;
        }
        //System.out.println("8888888888885555555555555555555888888888888888");
//        updateNeededReplications(block);// TODO
        synchronized (Simulator.getTraceList()) {
            Simulator.addTrace(new SimTrace(SimTrace.SCHEDULED_FOR_REPAIR, block.getDataitemId(), block
                    .getId(), block.getFailTime(), Node.now(), node.getDatacenterId(), node.getId()));
        }
        synchronized (neededReplications) {
            //System.out.println("88888888888888666666666666664444444444444444");
            neededReplications.add(block);
            //System.out.println("888888888888888888886666666666------6666666666");

        }
    }

    /* updates a block in under replication queue */
//    synchronized void updateNeededReplications(BlockInfo block) {
//        //DataitemInfo dataitemInfo = DataitemsMap.getDataitemInfo(block.getDataitemId());
////        int repl = dataitemInfo.numberOfReplicas(block);
////        int curExpectedReplicas = neededReplicas;
//
////        int repl = 0;
////        int curExpectedReplicas = 1;
//        Simulator.addTrace(new SimTrace(SimTrace.SCHEDULED_FOR_REPAIR, block.getDataitemId(), block
//                .getId(), block.getFailTime()));
//
////        neededReplications.update(block, repl, 0, curExpectedReplicas,
////                curReplicasDelta, expectedReplicasDelta);
//        neededReplications.add(block);
//    }
    public boolean addToDatanodeQueue(Event e) {

        synchronized (datanodeLock) {
            boolean result = toDatanodes.add(e);
            // System.out.println("added heartbeat to namenode.");
            return result;
        }
    }

    public boolean addToRequestQueue(Request r) {
        synchronized (requests) {
            boolean result = requests.add(r);
            // System.out.println("added heartbeat to namenode.");
            return result;
        }
    }

//    public BlocksMap getBlocksMap() {
//        return BlocksMap;
//    }
    public DataitemsMap getDataitemsMap() {
        return DataitemsMap;
    }

    /**
     * ************************* in ha ro khodam neveshtam: ***************
     */
    public void linkToNamenode(Namenode namenode) {
        canTransfer = true;
    }

    //////////////////// REQUEST MONITORE ///////////////////////////////////
    class RequestMonitor implements Runnable {

        public void run() {
            while (isRunning) {
                try {
                    // TODO

                    requestCheck();
                } catch (Exception e) {
                    // FSNamesystem.LOG.error(StringUtils.stringifyException(e));
                }
            }
        }

    }

    public void requestCheck() {

        Request request;
        do {
            synchronized (requests) {// Maybe the lock must be done on an
                // object
                //event = toNamenode.poll();
                request = requests.poll();

//                System.out.println("REQUEST from user   " + request.getUser().getDatacenterId()
//                +"_"+request.getUser().getId());
            }

            if (request != null) {
                handleRequest(request);
            }

        } while (request != null);
    }

    public void handleRequest(Request request) {

        long id = request.getId();
        User user = request.getUser();
        RequestType requestType = request.getRequestType();
        int dataitemId = request.getDataitemID();

        switch (requestType) {

            case READ:

                if (DataitemsMap.contain(dataitemId)) {

                    DataitemInfo dataitemInfo = DataitemsMap.getDataitemInfo(dataitemId);
                    if (!isOverload(dataitemInfo, datacenterId)) {

//                    System.out.println("REQUEST from user #" + request.getUser().getDatacenterId()
//                            + "_" + request.getUser().getId() + "  for dataitem #" + request.getDataitemID()
//                            + " placed in datacenter #" + datacenterId);
//                    System.out.println(" ");
                        if (user.isOwner(dataitemId) || dataitemInfo.getShare()) {

                            for (BlockInfo blockInfo : dataitemInfo.getBlockes()) {
                                DatanodeInfo datanodeInfo = dataitemInfo.getBlockToDatanodeMap().
                                        get(datacenterId).get(blockInfo);
                                addToDatanodeQueue(new Event(id, Event.READBLOCK, user, dataitemId,
                                        blockInfo.getId(),
                                        datanodeInfo.getDatacenterId(), datanodeInfo.getId()));
                            }

                            //dataitemInfo.addReadCount(user.getDatacenterId()); // tedade read yeki ziad mishavad
                        } else {
                            System.out.println("user #" + user.getId() + " can not access to Dataitem #"
                                    + dataitemId);
                        }

                        // baraye request based karbord darad va tedad request ha da har nahie ra neshan midahad                      
                        dataitemInfo.addReadCount(user.getDatacenterId());

                        // taeen konande zarfiat node ast
                        //dataitemInfo.decCapacity(datacenterId);

                        // 2 taye zir baraye lookaup path ast.
                        distanceUserFromData += (DatacentersTopology.getDistance(datacenterId,
                                user.getDatacenterId()) + 1) * 2;
                        numberOfRead++;

                        // 2 taye zir baraye replica utilization hastand
                        numberOfReplicaRead++;
                        dataitemInfo.addReplicaRead(datacenterId);

                        // baraye sabte traffic hasel az request ha baraye ravesh traffic based hast.
                        dataitemInfo.addreadRequestTraffic(datacenterId);

                        dataitemInfo.incRequestTraffic(datacenterId, datacenterId);

                    } else {
                        int destinationDatacenterId = Simulator.getDatacentersTopology().getDatacenters()
                                .get(datacenterId).getRouter().findDataitemPlace(dataitemId);

                        Datacenter destinationDatacenter = Simulator.getDatacentersTopology().getDatacenters()
                                .get(destinationDatacenterId);

                        Queue<Integer> path = Simulator.getDatacentersTopology().getDatacenters()
                                .get(datacenterId).getRouter().getRoutingPath(datacenterId,
                                        destinationDatacenterId);

                        while (!path.isEmpty()) {
                            int nextHopDatacenterId = path.poll();
                            dataitemInfo.addreadRequestTraffic(nextHopDatacenterId);
                            dataitemInfo.incRequestTraffic(destinationDatacenterId, nextHopDatacenterId);
                        }

                        destinationDatacenter.getNamenode().addToRequestQueue(request);

//                        distanceUserFromData += (DatacentersTopology.getDistance(datacenterId,
//                                destinationDatacenterId) + 1) * 2;
//                        DataitemsMap.addReadInfo(user, dataitemId);
                        //System.out.println("OVERLOAD : Reuest #" + id + " in datacenter #" + datacenterId);
                    }
                    // 

                } else {

                    int destinationDatacenterId = Simulator.getDatacentersTopology().getDatacenters()
                            .get(datacenterId).getRouter().findDataitemPlace(dataitemId);

                    DataitemInfo dataitemInfo = Simulator.getDatacentersTopology().getDatacenters()
                            .get(destinationDatacenterId).getNamenode().DataitemsMap.getDataitemInfo(dataitemId);

//                    Queue<Integer> path = Simulator.getDatacentersTopology().getDatacenters()
//                            .get(datacenterId).getRouter().getRoutingPath(datacenterId, destinationDatacenterId);
//                     Datacenter destinationDatacenter = Simulator.getDatacentersTopology().getDatacenters()
//                            .get(destinationDatacenterId);
//                    destinationDatacenter.getNamenode().addToRequestQueue(request);
                    int nextHopDatacenterId = Simulator.getDatacentersTopology().getDatacenters().
                            get(datacenterId).getRouter().nextHop(datacenterId, destinationDatacenterId);

                    Datacenter nextHopDatacenter = Simulator.getDatacentersTopology().getDatacenters()
                            .get(nextHopDatacenterId);
                    nextHopDatacenter.getNamenode().addToRequestQueue(request);

                    dataitemInfo.addreadRequestTraffic(datacenterId);

                    dataitemInfo.incRequestTraffic(destinationDatacenterId, datacenterId);

//                    System.out.println("REQUEST from user #" + request.getUser().getDatacenterId()
//                            + "_" + request.getUser().getId() + "  for dataitem #" + request.getDataitemID()
//                            + " unplaced in datacenter #" + datacenterId + " transfer for datacenter" + destinationDatacenterId);
//                    System.out.println(" ");
                    //DataitemsMap.addReadInfo(user, dataitemId);
                    // khate zir baray mohasebe lookupPathLenght
                    //distanceUserFromData += (DatacentersTopology.getDistance(datacenterId, destinationDatacenterId) + 1) * 2;
                    //numberOfRead++;
                }

                break;

            case WRITE:
                if (!isAlreadyExistDataitem(dataitemId)) {
                    // partition(dataitem);
                } else {
                    // create new replica for this dataitem
                }
                break;

            case DELETE:
                if (user.isOwner(dataitemId)) {
                    synchronized (DataitemsMap) {
                        //BlocksMap.
                    }
                }
                break;

        }

    }

    boolean isOverload(DataitemInfo dataitemInfo, int datacenterId) {
//        if (dataitemInfo.overload(datacenterId)) {
//            return true;
//        } else {
//            return false;
//        }
        return false;
    }

    //**********************************************
    int search(int dataitemId) {

        return 0;
    }

    // methode zir dataitem ra be block hayi mishekanad
//    public void partition(Dataitem dataitem) {
//        int numBlocks = dataitem.getSize() / Simulator.getBlockSize();
//        for (int i = 0; i < numBlocks; i++) {
//            Block block = new Block(dataitem.getId(), i, Simulator.getBlockSize());
//            BlocksMap.addBlock((BlockInfo) block);
//            //BlocksMap.addBlock(new BlockInfo(dataitem.getId(), i, Simulator.getBlockSize()));
//        }
//
//    }
    //methode zir chek mikonad ke in data item az ghabl dar system mojood ast ya jadid ast.
    public boolean isAlreadyExistDataitem(int dataitemId) {

        return false;
    }

    public void setDistanceUserFromData(long distanceUserFromData) {
        this.distanceUserFromData = distanceUserFromData;
    }

    public long getDistanceUserFromData() {
        return distanceUserFromData;
    }

    public void setNumberOfRead(long numberOfRead) {
        this.numberOfRead = numberOfRead;
    }

    public long getNumberOfRead() {
        return numberOfRead;
    }

    public void setNumberOfReplicaRead(long numberOfReplicaRead) {
        this.numberOfReplicaRead = numberOfReplicaRead;
    }

    public long getNumberOfReplicaRead() {
        return numberOfReplicaRead;
    }

}
