package hdfs.replicationsimulator;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 
 * @author in the main source of this class was not written name of author,
 * this class manipulate by Ali Mortazavi
 */

public class SimTrace {

    public static final int FAILURE_DETECTION = 0;
    public static final int SCHEDULED_FOR_REPAIR = 1;
    public static final int DATA_LOSS = 2;
    public static final int BLOCK_RECEIVED = 3;
    public static final int INFO = 4;
    public static final int BLOCK_READ = 5;
    public static final int DATANODE_REPEARED = 6;

    private long timestamp;
    private long ttr;
    private long failTime;
    private String message;

    private int action;
    private int id;
    private int idBlock;
    private int dataitemId;
    private int datacenterId;
    private int userId;
    private int userDatacenterId;

    public SimTrace(long timestamp, String msg) {
        this.timestamp = timestamp;
        this.message = msg;
    }

    //for DATANODE_REPEARED
    public SimTrace(int action, int datacenterId, int datanodeId, long ttr, long timestamp) {
        this.action = action;
        this.datacenterId = datacenterId;
        this.id = datanodeId;
        this.ttr = ttr;
        this.timestamp = timestamp;
        this.message = "Datanode #" + datacenterId + "_" + datanodeId + " is repeared. TTR is: " + ttr;

    }

    public SimTrace(int action, int id1, int id2, long time) {
        this.action = action;
        this.datacenterId = id1;
        this.id = id2;
        this.timestamp = time;
        this.message = "Namenode #" + datacenterId + " has detected a failure in Datanode #" + datacenterId + "_" + id;

    }

    //for SCHEDULED_FOR_REPAIR
    public SimTrace(int action, int id1, int id2, long time, Long TimeStamp, int datacenterId, int nodeId) {
        this.action = action;
        this.dataitemId = id1;
        this.idBlock = id2;
        this.failTime = time;
        this.timestamp = TimeStamp;
        this.datacenterId = datacenterId;
        this.id = nodeId;
        this.message = "Repearing node #" + datacenterId + "_" + id
                + ": Namenode has scheduled to repair block #" + dataitemId + "_" + idBlock
                + ", FailTime: " + failTime;
    }

    public SimTrace(int action, int id1, int id2) {
        this.timestamp = Node.now();
        this.action = action;

        switch (action) {
//            case FAILURE_DETECTION:
//                this.datacenterId = id1;
//                this.id = id2;
//
//                this.message = "Namenode #" + datacenterId + " has detected a failure in Datanode #" + datacenterId + "_" + id;
//                break;

            case DATA_LOSS:
                this.dataitemId = id1;
                this.idBlock = id2;

                this.message = "Block #" + dataitemId + "_" + idBlock + " is lost";
                break;

        }
    }

    public SimTrace(String info) {
        this.timestamp = Node.now();
        this.action = INFO;
        this.id = 0;
        this.message = info;
    }

    public SimTrace(int action, int datacenteId, int id, int dataitemId, int idBlock, long time, long ttr) {
        this.timestamp = time;
        this.ttr = ttr;
        this.action = action;

        this.datacenterId = datacenteId;
        this.id = id;

        this.dataitemId = dataitemId;
        this.idBlock = idBlock;

        switch (action) {

            case BLOCK_RECEIVED:
                this.message = "Datanode #" + datacenteId + "_" + id + " has received a block #" + dataitemId + "_" + idBlock
                        + ", and TTR is: " + ttr;
                break;
        }
    }

    //for BLOCK_READ
    public SimTrace(int userDatacenterId, int userId, int action, int datacenteId,
            int id, int dataitemId, int idBlock, long time) {
        this.timestamp = time;
        this.userId = userId;
        this.userDatacenterId = userDatacenterId;
        this.action = action;

        this.datacenterId = datacenteId;
        this.id = id;

        this.dataitemId = dataitemId;
        this.idBlock = idBlock;

        switch (action) {

            case BLOCK_READ:
                this.message = "User #" + userDatacenterId + "_" + userId + " has received a block #"
                        + dataitemId + "_" + idBlock
                        + " from datanode #" + datacenteId + "_" + id;
                break;
        }
    }

    // for DATALOSS
//    public SimTrace(int action, int dataitemId, int idBlock) {
//        this.timestamp = Node.now();
//        this.action = action;
//
//        switch (action) {
//
//            case DATA_LOSS:
//                this.dataitemId = dataitemId;
//                this.idBlock = idBlock;
//                this.message = "Block #" + dataitemId + "_" + idBlock + " is lost";
//                break;
//        }
//    }
    public long getTimestamp() {
        return timestamp;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);

        //DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");
        DateFormat formatter = new SimpleDateFormat("hh:mm:ss.SSS");

        return "[" + formatter.format(calendar.getTime()) + "]: " + message + "\n";
    }

    public int getAction() {
        return action;
    }

    public int getId() {
        return id;
    }

    public int getDatacenterId() {
        return datacenterId;
    }

    public int getDataitemId() {
        return dataitemId;
    }

    public boolean isLossEvent() {
        return (action == DATA_LOSS);
    }

    public int getIdBlock() {
        return idBlock;
    }

    public long getTtr() {
        return ttr;
    }

    public long getFailTime() {
        return failTime;
    }

    public void setFailTime(long failTime) {
        this.failTime = failTime;
    }

}
