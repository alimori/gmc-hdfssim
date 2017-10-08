package hdfs.replicationsimulator;

/**
 * a block of a data item. 
 * a data item strip into multiple same size blocks
 * 
 * @author in the main source of this class was not written name of author,
 * this class manipulate by Ali Mortazavi
 */
public class Block {

    private int dataitemId;
    private int id;
    private int size;
    private long failTime = 0L;
    private DatanodeInfo failNodeInfo = null;
   

    Block(int id) {
        this.id = id;
    }

    public Block(int dataitemId, int id) {
        this.dataitemId = dataitemId;
        this.id = id;
    }

    public Block(int dataitemId, int id, int size) {
        this.dataitemId = dataitemId;
        this.id = id;
        this.size = size;
    }

    public int getId() {
        return this.id;
    }

    public int getDataitemId() {
        return dataitemId;
    }

    public void setDataitemId(int dataitemId) {
        this.dataitemId = dataitemId;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getFailTime() {
        return failTime;
    }

    public void setFailTime(long failTime) {
        this.failTime = failTime;
    }

    public DatanodeInfo getFailNodeInfo() {
        return failNodeInfo;
    }

    public void setFailNodeInfo(DatanodeInfo failNodeInfo) {
        this.failNodeInfo = failNodeInfo;
    }
    
    

}
