package hdfs.replicationsimulator;

import gmc_hdfs.replicationsimulator.User;

/**
 * 
 * @author in the main source of this class was not written name of author,
 * this class manipulate by Ali Mortazavi
 */
public class Event {

    public boolean isHandle = false;
    private int source;
    private int sourceDatacenter;

    private int action;
    private long time;

    private int destination;
    private int destinationDatacenter;

    private int dataitemId;
    private int blockId;

//    private BlockInfo blockInfo;
//    private DatanodeInfo datanodeInfo;
    private User user;

    private int isFailBolck; // agar 1 bood yani true va 0 yani false
    private int cutDatacenter;

    private long requestId;

    public final static int HEARTBEAT = 1;
    public final static int BLOCKRECEPTION = 2;

    public final static int REPLICATION = 3;
    public final static int PENDINGTRANSFER = 4;
    public final static int PENDINGTRANSFERFORUSER = 10;

    public final static int READBLOCK = 5;

    // 2 ta EVENT ZIR AZ SAMTE ReplicationManager maiyand
    public final static int REMOVE_DATAITEM = 6;
    public final static int REPLICATE_DATAITEM = 7;

    public final static int REMOVE_BLOCK = 8;
    public final static int BLOCKREMOVED = 9;

    public final static int BLOCKREADED = 11;

    public static final int FAILURE = 0;

    // baraye HEARTBEAT and FAILURE
    public Event(int sourceDatacenter, int source, int action, long time) {
        this.sourceDatacenter = sourceDatacenter;
        this.source = source;
        this.action = action;
        this.time = time;
    }

    public Event(int sourceDatacenter, int source, int action, long time, int param1) {
        this.sourceDatacenter = sourceDatacenter;
        this.source = source;
        this.action = action;
        this.time = time;
        this.destination = param1;
    }

    // for  BLOCKREMOVED
    public Event(int datacenterSource, int source, int action, long time, int dataitemId, int blockId) {
        this.sourceDatacenter = datacenterSource;
        this.source = source;
        this.action = action;
        this.time = time;
        this.dataitemId = dataitemId;
        this.blockId = blockId;
    }

    // for BLOCKRECEPTION 
    public Event(int datacenterSource, int source, int action, long time, int dataitemId,
            int blockId, int isBockFail, int cutDatacenter) {
        this.sourceDatacenter = datacenterSource;
        this.source = source;
        this.action = action;
        this.time = time;
        this.dataitemId = dataitemId;
        this.blockId = blockId;
        this.isFailBolck = isBockFail;
        this.cutDatacenter = cutDatacenter;
    }

    // for REMOVE_BLOCK and FOR REPLICATE_DATAITEM
    public Event(int action, int param1, int param2, int param3, int param4) {

        this.action = action;

        if (action == Event.REMOVE_BLOCK) {
            this.sourceDatacenter = param1;
            this.source = param2;
            this.dataitemId = param3;
            this.blockId = param4;
        }

        if (action == Event.REPLICATE_DATAITEM) {
            this.dataitemId = param1;
            this.sourceDatacenter = param2;
            this.destinationDatacenter = param3;
            this.cutDatacenter = param4;
        }
    }

    // for PENDINGTRANSFER and REPLICATION
    public Event(int datacenterSource, int source, int action, long time, int datacenterDestination,
            int destination, int dataitemId, int blockId, int isBlockFail, int cutDatacenter) {
        this.sourceDatacenter = datacenterSource;
        this.source = source;
        this.action = action;
        this.time = time;
        this.dataitemId = dataitemId;
        this.blockId = blockId;
        this.destinationDatacenter = datacenterDestination;
        this.destination = destination;
        this.isFailBolck = isBlockFail;
        this.cutDatacenter = cutDatacenter;
    }

    //for PENDINGTRANSFERFORUSER
    public Event(long requetId, int datacenterSource, int source, int action, long time, User user,
            int dataitemId, int blockId) {
        this.requestId = requetId;
        this.sourceDatacenter = datacenterSource;
        this.source = source;
        this.action = action;
        this.time = time;
        this.dataitemId = dataitemId;
        this.blockId = blockId;
        this.user = user;

    }

    // for READ and BLOCKREADED
    public Event(long requestid, int action, User user, int dataitemId, int blockId, int sourceDatacenter,
            int sourceId) {
        this.requestId = requestid;
        this.action = action;
        this.user = user;
        this.dataitemId = dataitemId;
        this.blockId = blockId;
        this.sourceDatacenter = sourceDatacenter;
        this.source = sourceId;

    }

    // FOR REPLICATE_DATAITEM
//    public Event(int action, int dataitemId, int sourceDatacenter, int destinationDatacenter) {
//        this.action = action;
//        this.dataitemId = dataitemId;
//        this.sourceDatacenter = sourceDatacenter;
//        this.destinationDatacenter = destinationDatacenter;
//    }
    // FOR REMOVE_DATAITEM
    public Event(int action, int dataitemId, int datacenter) {
        this.action = action;
        this.dataitemId = dataitemId;
        this.sourceDatacenter = datacenter;
    }

    public int getSource() {
        return source;
    }

    public int getAction() {
        return action;
    }

    public long getTime() {
        return time;
    }

    public int getDestination() {
        return destination;
    }

    public int getBlockId() {
        return blockId;
    }

    public int getDestinationDatacenter() {
        return destinationDatacenter;
    }

    public int getSourceDatacenter() {
        return sourceDatacenter;
    }

    public int getDataitemId() {
        return dataitemId;
    }

    public void setIsHandle(boolean isHandle) {
        this.isHandle = isHandle;
    }

    public User getUser() {
        return user;
    }

//    public BlockInfo getBlockInfo() {
//        return blockInfo;
//    }
//
//    public DatanodeInfo getDatanodeInfo() {
//        return datanodeInfo;
//    }
    public int getIsFailBolck() {
        return isFailBolck;
    }

    public void setIsFailBolck(int isFailBolck) {
        this.isFailBolck = isFailBolck;
    }

    public int getCutDatacenter() {
        return cutDatacenter;
    }

    public void setCutDatacenter(int cutDatacenter) {
        this.cutDatacenter = cutDatacenter;
    }

    public long getRequestId() {
        return requestId;
    }

}
