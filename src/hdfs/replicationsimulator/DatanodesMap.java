package hdfs.replicationsimulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Map to store information about datanodes
 * 
 * @author in the main source of this class was not written name of author,
 * this class manipulate by Ali Mortazavi
 */
public class DatanodesMap {

    private Map<List<Integer>, DatanodeInfo> map;
    private List<DatanodeInfo> heartbeats;

    private int datacenterId;

    Random r;

    /*
     * Constructor (Initialize the blocksMap)
     */
    DatanodesMap(int datacenterId) {

        this.datacenterId = datacenterId;

        this.map = new HashMap<>();
        this.heartbeats = new ArrayList<>();
        r = new Random();
    }

    /*
     * Add a node to the map
     */
    public void addDatanodeInfo(DatanodeInfo DatanodeInfo) {
        //Integer[] key = {DatanodeInfo.getDatacenterId(), DatanodeInfo.getId()};
        //map.put(, DatanodeInfo);

        List<Integer> key = new ArrayList<>(Arrays.asList(DatanodeInfo.getDatacenterId(), DatanodeInfo.getId()));
        map.put(key, DatanodeInfo);
        heartbeats.add(DatanodeInfo);
        //TODO
    }

    /*
     * get a node from the map
     */
    public DatanodeInfo getDatanodeInfo(int datacenterId, int id) throws NullPointerException {
        //Integer[] key = {datacenterId, id};

        List<Integer> key = new ArrayList<>(Arrays.asList(datacenterId, id));
        //System.out.println("key is: " + map.containsKey(key));
        return map.get(key);    //////////////////////////////////////////////////////////////////////////////////////////////////
    }

    public List<DatanodeInfo> getHeartbeats() {
        return heartbeats;
    }

    public void removeDatanode(DatanodeInfo nodeInfo) {
        //Integer[] key = {nodeInfo.getDatacenterId(), nodeInfo.getId()};
        List<Integer> key = new ArrayList<>(Arrays.asList(nodeInfo.getDatacenterId(), nodeInfo.getId()));
        map.remove(key);
        //map.remove(nodeInfo);

        //heartbeats.remove(nodeInfo);
    }

    public DatanodeInfo randomNode() {
        //int datacenterId = r.nextInt(9);
        int nodeId = r.nextInt(heartbeats.size());

        //Integer[]key={datacenterId,nodeId};
        return getDatanodeInfo(datacenterId, nodeId);
    }

    public DatanodeInfo mostRemainCapacieyNode(List<DatanodeInfo> datanodeInfos) {
//        List<DatanodeInfo> infos = new ArrayList<>();
//        infos = heartbeats;
//        infos.removeAll(datanodeInfos);
        DatanodeInfo choosen = null;
        synchronized (datanodeInfos) {
            int max = 0;
            for (DatanodeInfo datanodeInfo : datanodeInfos) {
                if (max < datanodeInfo.getRemainCapacity()) {
                    max = datanodeInfo.getRemainCapacity();
                    choosen = datanodeInfo;
                }
            }
        }
        return choosen;
    }

    public int getDatacenterId() {
        return datacenterId;
    }

    public void setDatacenterId(int datacenterId) {
        this.datacenterId = datacenterId;
    }

}
