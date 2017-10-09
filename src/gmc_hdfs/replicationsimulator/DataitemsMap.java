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
import hdfs.replicationsimulator.SimTrace;
import hdfs.replicationsimulator.Simulator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements A map for storing information about data items 
 *
 * @author AliMori
 */
public class DataitemsMap {

    /**
     * A map for storing information about data items 
     */
    private Map<Integer, DataitemInfo> map;
    /**
     * ID of data center
     */
    private int datacenterId;

    /**
     * Takes a data center's ID and create a object of DataitemsMap for it
     * 
     * @param datacenterId ID of data center
     */
    public DataitemsMap(int datacenterId) {
        this.datacenterId = datacenterId;
        this.map = new ConcurrentHashMap<>();
    }

    /**
     * This method identifies whether the map has the data with daraitemId or not.
     * 
     * @param dataitemId ID of data item
     * @return true, if map contain data item which identified by dataitemId
     * else return false
     */
    public boolean contain(int dataitemId) {
        return map.containsKey(dataitemId);
    }

    /**
     * For each read request for data from the users for the store data,
     * it increases the number of requests.
     * 
     * @param user User who sent request
     * @param dataitemId ID of data item
     */
    public void addReadInfo(User user, int dataitemId) {
        map.get(dataitemId).addReadCount(user.getDatacenterId());
    }

    /**
     * Add information about data to map
     * @param info 
     */
    public void addDataitem(DataitemInfo info) {
        map.put(info.getId(), info);
    }

    /**
     * This method shows number of data item in map
     * @return number of data item in map
     */
    public int size() {
        return map.size();
    }

    public Set<Map.Entry<Integer, DataitemInfo>> entrySet() {
        return map.entrySet();
    }

    public DataitemInfo getDataitemInfo(int dataitemId) {
        return map.get(dataitemId);
    }

    /**
     * Remove information of data item 
     * 
     * @param dataitemId ID of data item
     */
    public void removeDataitem(int dataitemId) {
        map.remove(dataitemId);
    }

    //****************************************************
    /**
     * It removes the node information from the set of block b storing nodes. 
     * This occurs when the data migrates from the node of it is deleted of 
     * the node is damaged.
     * 
     * @param b BlockInfo
     * @param node DatanodeInfo
     * @return true, if success
     */
    public boolean removeNode(BlockInfo b, DatanodeInfo node) {

        BlockInfo info = b;
        if (info == null) {
            System.out.println("BlockInfo is null.");
            return false;
        }

        int datacenterID = node.getDatacenterId();
        int dataitemId = info.getDataitemId();

        DataitemInfo dataitemInfo = this.getDataitemInfo(dataitemId);
        dataitemInfo.getBlockToDatanodeMap().get(datacenterID).put(info, null);

        if (dataitemInfo.hasFailed(b)) {
            dataitemInfo.removeBlock(b);
            Simulator.addTrace(new SimTrace(SimTrace.DATA_LOSS, info.getDataitemId(), info.getId()));
        }
        return true;
    }

}
