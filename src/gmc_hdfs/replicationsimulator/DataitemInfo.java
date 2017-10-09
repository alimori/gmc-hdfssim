/*
 * Copyright 2016 Ali Mortazavi
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gmc_hdfs.replicationsimulator;

import hdfs.replicationsimulator.BlockInfo;
import hdfs.replicationsimulator.DatanodeInfo;
import hdfs.replicationsimulator.Simulator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * It saves all the data information which needs to be stored.
 *
 * @author AliMori
 */
public class DataitemInfo extends Dataitem implements Comparable<DataitemInfo> {

    /**
     * It saves the data center informations which store versions of this data.
     */
    private Map<Integer, Datacenter> datacenters; 
    /**
     * It saves the block informations.
     */
    private List<BlockInfo> blockes; 

    //Map<DatacenterId, Map<BlockInfo, DatanodeInfo>> 
    /**
     * It saves the mapping information between blocks and nodes.
     */
    private Map<Integer, Map<BlockInfo, DatanodeInfo>> blockToDatanodeMap; 

    //map<datacenterId, masternum>
    /**
     * It identifies whether each version of data is original or copy.
     */
    private Map<Integer, String> masterCopy;
    /**
     * It keeps number of read requests in each data center.
     */
    private volatile Map<Integer, Integer> readCountMap;
    
    /**
     * It keeps the traffic for accessing the data in data centers in each 
     * data center.
     */
    private volatile Map<Integer, Integer> readRequestTraffic;

    // <datacenterID(hamooni ke copy ra darad , <datacenterId, traffic>>
    /**
     * It keeps the traffic for accessing the data in data centers for each 
     * of data centers which store a version of this data. 
     */
    private volatile Map<Integer, Map<Integer, Integer>> requestTraffic;

    /**
     * It keeps number of read requests in each data center for each replica
     */
    private volatile Map<Integer, Integer> readReplicaMap;

    /**
     * Capacity mesa the number of requests which a data center answers at 
     * one time. In fact the number of sessions that can be held in one moment.
     */
    volatile Map<Integer, Integer> capacity;

    /**
     * Create a new object of DataitemInfo
     * @param id
     * @param size 
     */
    public DataitemInfo(int id, int size) {
        super(id, size);
        blockes = new ArrayList<>();
        blockToDatanodeMap = new HashMap<>();

        datacenters = new HashMap<>();

        masterCopy = new HashMap<>();

        readReplicaMap = new HashMap<>();

        readCountMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            readCountMap.put(i, 0);
        }

        readRequestTraffic = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            readRequestTraffic.put(i, 0);
        }

        requestTraffic = new HashMap<>();

        capacity = new HashMap<>();
    }

//    public void addDatanode(DatanodeInfo dn) {
//        datanodes.add(dn);
//    }
//
//    boolean removeDataNode(DatanodeInfo DatanodeInfo) {
//        return datanodes.remove(DatanodeInfo);
//    }
    public DatanodeInfo getDatanode(BlockInfo blockInfo, int datacenterId) {
        return blockToDatanodeMap.get(datacenterId).get(blockInfo);
    }

    public Map<Integer, Map<BlockInfo, DatanodeInfo>> getBlockToDatanodeMap() {
        return blockToDatanodeMap;
    }

    public void addBlock(BlockInfo b) {
        blockes.add(b);
    }

    public void removeBlock(BlockInfo b, int datacenterId) {
        blockToDatanodeMap.get(datacenterId).remove(b);
    }

    public boolean removeBlock(BlockInfo b) {
        return blockes.remove(b);
    }

    public List<BlockInfo> getBlockes() {
        return blockes;
    }

    /**
     * Sort information based on popularity of data
     * @param o place of DataitemInfo in sorted map
     * @return 
     */
    @Override
    public int compareTo(DataitemInfo o) {
        if (o.popularity() > this.popularity()) {
            return 1;
        } else {
            return -1;
        }
    }

    /**
     * It is a dependent of the number of simultaneous requests.
     * @return popularity of data
     */
    public int popularity() {
        int sum = 0;
        for (Map.Entry<Integer, Integer> entry : readCountMap.entrySet()) {
            Integer datacenterId = entry.getKey();
            Integer readCount = entry.getValue();
            sum += readCount;
        }
        return sum;
    }

    public int getReadCount(int geograghpicalZone) {
        return readCountMap.get(geograghpicalZone);
    }

    public Map<Integer, Integer> getReadCountList() {
        return readCountMap;
    }

    /**
     * It clears the keeping list information of number of requests.
     */
    public void clearReadCountList() {

        synchronized (readCountMap) {
            for (Map.Entry<Integer, Integer> entry : readCountMap.entrySet()) {
                Integer datacenterId = entry.getKey();
                readCountMap.put(datacenterId, 0);
            }
        }
    }

    /**
     * It adds to the number of requests from the users of one area with the
     * data center by one.
     * @param datacenterId ID of data center
     */
    public void addReadCount(int datacenterId) {
        synchronized (readCountMap) {
            int read = readCountMap.get(datacenterId);
            read++;
            this.readCountMap.put(datacenterId, read);
        }
    }

    public int getReadRequestTraffic(int geograghpicalZone) {
        return readRequestTraffic.get(geograghpicalZone);
    }

    public Map<Integer, Integer> getRequestTraffic(int replicaGeograghpicalZone) {
        return requestTraffic.get(replicaGeograghpicalZone);
    }

    public Map<Integer, Map<Integer, Integer>> getRequestTraffic() {
        return requestTraffic;
    }

    public Map<Integer, Integer> getReadRequestTraffic() {
        return readRequestTraffic;
    }

    /**
     * It clears the keeping list information of traffic
     */
    public void clearreadRequestTrafficList() {
        synchronized (readRequestTraffic) {
            for (Map.Entry<Integer, Integer> entry : readRequestTraffic.entrySet()) {
                Integer datacenterId = entry.getKey();
                readRequestTraffic.put(datacenterId, 0);
            }
        }
    }

    /**
     * * It adds to the number of traffic of one area with the
     * data center by one.
     * @param datacenterId ID of data center
     */
    public void addreadRequestTraffic(int datacenterId) {
        synchronized (readRequestTraffic) {
            int read = readRequestTraffic.get(datacenterId);
            read++;
            this.readRequestTraffic.put(datacenterId, read);
        }
    }

 
    /**
     * In the data center datacenterId2 creates the access traffic for
     * the existing data in data center datacenterID1.
     * @param datacenterId1 first data center
     * @param datacenterId2 second data center
     */
    public void incRequestTraffic(int datacenterId1, int datacenterId2) {
        synchronized (requestTraffic) {
            Map<Integer, Integer> traffic = requestTraffic.get(datacenterId1);
            if (traffic.isEmpty()) {
                for (int i = 0; i < 10; i++) {
                    traffic.put(i, 0);
                }
            }
            int traff = traffic.get(datacenterId2);
            traff++;
            traffic.put(datacenterId2, traff);
            requestTraffic.put(datacenterId1, traffic);
        }
    }

    /**
     * It adds the information of data center d to the list of data centers 
     * which have a version of this data.
     * 
     * @param datacenter input data center
     */
    public void addToDatacentersList(Datacenter datacenter) {
        datacenters.put(datacenter.getId(), datacenter);
    }

    /**
     * It removes the information of data center d to the list of data centers 
     * which have a version of this data.
     * 
     * @param datacenter input data center
     * @return true, if success.
     */
    public Datacenter removeDatacentersList(int datacenter) {
        return datacenters.remove(Simulator.getDatacentersTopology().getDatacenters().get(datacenter).getId());
    }

    public Map<Integer, Datacenter> getDatacenters() {
        return datacenters;
    }

    /**
     * Add information of master or copy data for a data center
     * @param datacenterId ID of data center
     * @param s master or copy
     */
    public void addToMasterCopyMap(int datacenterId, String s) {
        masterCopy.put(datacenterId, s);
    }

    public Map<Integer, String> getMasterCopy() {
        return masterCopy;
    }

    /**
     * This method add information of mapping between blocks and nodes to map
     * @param b a object of BlockInfo
     * @param dn a object of DatanodeInfo
     */
    public void addBlockToDatanodeMap(BlockInfo b, DatanodeInfo dn) {

        if (blockToDatanodeMap.get(dn.getDatacenterId()) == null) {
            blockToDatanodeMap.put(dn.getDatacenterId(), new HashMap<BlockInfo, DatanodeInfo>());
        }
        blockToDatanodeMap.get(dn.getDatacenterId()).put(b, dn);
    }

    /**
     * It investigates whether the data block is lost from the system or not.
     * @param blockInfo a object of BlockInfo
     * @return true, if it lost
     */
    boolean hasFailed(BlockInfo blockInfo) {
        for (Map.Entry<Integer, Map<BlockInfo, DatanodeInfo>> entry : blockToDatanodeMap.entrySet()) {
            Integer datacenterId = entry.getKey();
            Map<BlockInfo, DatanodeInfo> map = entry.getValue();
            if (map.get(blockInfo) != null) {
                return false;
            }
        }
        return true;
    }

     /**
     * This method takes data Block and gives the nodes which is stored  
     * 
     * @param block object of BlockInfo
     * @return Containing node
     */
    public List<DatanodeInfo> getContainingNodes(BlockInfo block) {
        List<DatanodeInfo> containingNods = new ArrayList<>();
        for (Map.Entry<Integer, Map<BlockInfo, DatanodeInfo>> entry : blockToDatanodeMap.entrySet()) {
            Integer datacenterId = entry.getKey();
            Map<BlockInfo, DatanodeInfo> map = entry.getValue();
            if (map.get(block) != null) {
                containingNods.add(map.get(block));
            }
        }
        return containingNods;
    }

    /**
     * This method takes data Block and gives number of replicas for it, 
     * in whole system 
     * 
     * @param block object of BlockInfo
     * @return Number of data block's replicas.
     */
    public int numberOfReplicas(BlockInfo block) {
        int numOfReplicas = 0;
        for (Map.Entry<Integer, Map<BlockInfo, DatanodeInfo>> entry : blockToDatanodeMap.entrySet()) {
            Integer datacenterId = entry.getKey();
            Map<BlockInfo, DatanodeInfo> map = entry.getValue();
            if (map.get(block) != null) {
                numOfReplicas++;
            }
        }
        return numOfReplicas;
    }

    /**
     * Update information of the MasterCopy's list.
     */
    public void updateMasterCopy() {
        List<Integer> l = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : masterCopy.entrySet()) {
            Integer DId = entry.getKey();
            String mastercopy = entry.getValue();
            l.add(DId);
        }
        masterCopy.put(l.get(0), "master");
    }

    public Map<Integer, Integer> getReadReplicaMap() {
        return readReplicaMap;
    }

    /**
     * Add number of read request by one.
     * 
     * @param datacenterId ID of data center
     */
    public void addReplicaRead(int datacenterId) {
        synchronized (readReplicaMap) {
            int read = readReplicaMap.get(datacenterId);
            read++;
            readReplicaMap.put(datacenterId, read);
        }
    }

    /**
     * It clears information of number of read from list
     */
    public void clearReplicaRead() {
        synchronized (readReplicaMap) {
            for (Map.Entry<Integer, Integer> entry : readReplicaMap.entrySet()) {
                Integer dId = entry.getKey();
                readReplicaMap.put(dId, 0);

            }
        }
    }

    /**
     * It adds the num to the arbitrary number of requests.
     * @param datacenterId ID of data center
     * @param num number of requests
     */
    public void addToReplicaReadMap(int datacenterId, int num) {
        readReplicaMap.put(datacenterId, num);
    }

    public Map<Integer, Integer> getCapacity() {
        return capacity;
    }

    public void setCapacity(int datacenterId, int cap) {
        capacity.put(datacenterId, cap);
    }

    /**
     * It increases the capacity of data center datacenterId by one for the 
     * processing of requests. In fact after the request has been answered, 
     * its place will be empty and capacity is freed. This method should be 
     * executed after answering a request.
     * 
     * @param datacenterId ID of data center
     */
    public void incCapacity(int datacenterId) {
        synchronized (capacity) {
            int cap = this.capacity.get(datacenterId);
            cap++;
            capacity.put(datacenterId, cap);
        }
    }

    /**
     * It decreases the capacity of data center datacenterId by one for the 
     * processing of requests.
     * This method should be executed after receiving a request.
     * 
     * @param datacenterId ID of data center
     */
    public void decCapacity(int datacenterId) {
        synchronized (capacity) {
            int cap = this.capacity.get(datacenterId);
            cap--;
            capacity.put(datacenterId, cap);
        }
    }

    /** 
     * Remove data center's information from capacity list
     * @param datacenterId ID of data center
     */
    public void removeCapacity(int datacenterId) {
        synchronized (capacity) {
            capacity.remove(datacenterId);
        }
    }

    /**
     * It shows that the data center have overload or not. When the capacity 
     * is full and another request is received overload occurs.
     * @param datacenterId ID of data center
     * @return true, if it overloaded
     */
    public boolean overload(int datacenterId) {
        synchronized (capacity) {
            if (capacity.get(datacenterId) <= 0) {
                return true;
            } else {
                return false;
            }
        }
    }

    public int getReplicaNumber() {
        return datacenters.size();
    }

    /**
     * Add data center's traffic's information to map
     * @param datacenterId ID of data center
     * @param map MAP which store traffic's information
     */
    public void addToRequestTraffic(int datacenterId, Map<Integer, Integer> map) {
        requestTraffic.put(datacenterId, map);
    }

}
