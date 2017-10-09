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

import hdfs.replicationsimulator.Simulator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class a map for storing information about replication
 *
 * @author AliMori
 */
public class ReplicationMap {

    /**
     *  This map stores information about replication
     */
    private Map<Integer, List<DataitemInfo>> map;

    /**
     * create an object of ReplicationMap
     */
    public ReplicationMap() {
        this.map = new HashMap<>();
    }

    /**
     * Add DataitemInfo's information to map
     * @param dataitemInfo an object of DataitemInfo
     */
    void addDataitemInfo(DataitemInfo dataitemInfo) {

        if (map.get(dataitemInfo.getId()) == null) {
            map.put(dataitemInfo.getId(), new ArrayList<DataitemInfo>());
        }
        map.get(dataitemInfo.getId()).add(dataitemInfo);
    }

    /**
     * The information of queries are extracted from each Dataiteminfo for a 
     * data center and are integrated in a map structure and returned. Each 
     * query has the information from a default request.
     * @return  The information of queries are extracted from each Dataiteminfo
     * for a data center
     */
    public Map<Integer, Map<Integer, Integer>> convertToQueryMap() {
        Map<Integer, Map<Integer, Integer>> readMapsMap = new HashMap<>();

        for (Map.Entry<Integer, List<DataitemInfo>> entry : map.entrySet()) {
            Integer key = entry.getKey(); //dataitemId
            List<DataitemInfo> list = entry.getValue();

            //List<Integer> readList = new ArrayList<>();
            Map<Integer, Integer> readMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                readMap.put(i, 0);
            }
//            for (DataitemInfo dataitemInfo : list) {
//                sumLists(readTrafficMap, dataitemInfo.getReadCountList());
//            }
            for (int i = 0; i < 10; i++) {
                readMap.put(i, list.get(0).getReadCount(i));
                list.get(0).getReadCountList().put(i, 0);
            }

            readMapsMap.put(key, readMap);
        }
        return readMapsMap;
    }
    
    /**
     * The information of traffic are extracted from each Dataiteminfo for a 
     * data center and are integrated in a map structure and returned.
     * @return  The information of traffic are extracted from each Dataiteminfo 
     * for a data center
     */
    /////////////////////////////////////////////////////////////////////////
    public Map<Integer, Map<Integer, Integer>> convertToTrafficMap() {
        Map<Integer, Map<Integer, Integer>> readTrafficMapsMap = new HashMap<>();

        for (Map.Entry<Integer, List<DataitemInfo>> entry : map.entrySet()) {
            Integer key = entry.getKey(); //dataitemId
            List<DataitemInfo> list = entry.getValue();

            //List<Integer> readList = new ArrayList<>();
            Map<Integer, Integer> readTrafficMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                readTrafficMap.put(i, 0);
            }
//            for (DataitemInfo dataitemInfo : list) {
//                sumLists(readTrafficMap, dataitemInfo.getReadCountList());
//            }
            for (int i = 0; i < 10; i++) {
                int traffic = list.get(0).getReadRequestTraffic(i);
                readTrafficMap.put(i, traffic);
                list.get(0).getReadRequestTraffic().put(i, 0);
            }

            readTrafficMapsMap.put(key, readTrafficMap);
        }
        return readTrafficMapsMap;
    }
    ///@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

    /**
     * The information of traffic are extracted from each Dataiteminfo for all 
     * data centers and are integrated in a map structure and returned. 
     * @return  The information of traffic are extracted from each Dataiteminfo 
     * for all data centers
     */
    public Map<Integer, Map<Integer, Map<Integer, Integer>>> convertToTrafficMaps() {
        Map<Integer, Map<Integer, Map<Integer, Integer>>> readTrafficMapsMap = new HashMap<>();

        for (Map.Entry<Integer, List<DataitemInfo>> entry : map.entrySet()) {
            Integer key = entry.getKey(); //dataitemId
            List<DataitemInfo> list = entry.getValue();
            DataitemInfo dataitemInfo = list.get(0);

            //List<Integer> readList = new ArrayList<>();
            Map<Integer, Map<Integer, Integer>> readTrafficMap = new HashMap<>();

            for (Map.Entry<Integer, Map<Integer, Integer>> entry1 : dataitemInfo.getRequestTraffic().entrySet()) {
                Integer datacenterHoldReplica = entry1.getKey();
                Map<Integer, Integer> trafficMap = entry1.getValue();

                Map<Integer, Integer> traffMap = new HashMap<>();
                for (Map.Entry<Integer, Integer> entry2 : trafficMap.entrySet()) {
                    Integer region = entry2.getKey();
                    Integer traffic = entry2.getValue();

                    traffMap.put(region, traffic);
                    dataitemInfo.getRequestTraffic(datacenterHoldReplica).put(region, 0);
                }
                readTrafficMap.put(datacenterHoldReplica, traffMap);
            }
//            for (int i = 0; i < 10; i++) {
//                readTrafficMap.put(i, 0);
//            }
////            for (DataitemInfo dataitemInfo : list) {
////                sumLists(readTrafficMap, dataitemInfo.getReadCountList());
////            }
//            for (int i = 0; i < 10; i++) {
//                readTrafficMap.put(i, list.get(0).getReadRequestTraffic(i));
//                list.get(0).getReadRequestTraffic().put(i, 0);
//            }

            readTrafficMapsMap.put(key, readTrafficMap);
        }
        return readTrafficMapsMap;
    }

    /**
     * This method receives two map and adds the values peer to peer and puts 
     * them in the first map. It used in other methods.
     * @param m1 first map
     * @param m2 second map
     */
    public void sumLists(Map<Integer, Integer> m1, Map<Integer, Integer> m2) {
        for (int i = 0; i < m2.size(); i++) {
            //l1.set(i, l1.get(i) + l2.get(i));
            m1.put(i, m1.get(i) + m2.get(i));
        }
    }

    public Map<Integer, List<DataitemInfo>> getMap() {
        return map;
    }

    public List<DataitemInfo> getDataitemInfo(int dataitemId) {
        return map.get(dataitemId);
    }

    //***********************************************************************************
//    public float replicaUtilization(int dataitemId) {
//        DataitemInfo dataitemInfo = map.get(dataitemId).get(0);
//        
//        int masterRead = 0;
//        for (Map.Entry<Integer, Integer> entry : dataitemInfo.getReadReplicaMap().entrySet()) {
//            Integer regeion = entry.getKey();
//            Integer numOfRead = entry.getValue();
//            if ("master".equals(dataitemInfo.getMasterCopy().get(regeion))) {
//                if (numOfRead == 0) {
//                    masterRead = 1;
//                } else {
//                    masterRead = numOfRead;
//                }
//            }
//        }
//        int numofCopy = 0;
//        float totalCopy = 0;
//        for (Map.Entry<Integer, Integer> entry : dataitemInfo.getReadReplicaMap().entrySet()) {
//            float replicaUtilization;
//            int copyRead;
//            Integer regeion = entry.getKey();
//            Integer numOfRead = entry.getValue();
//            if ("copy".equals(dataitemInfo.getMasterCopy().get(regeion))) {
//                copyRead = numOfRead;
//                replicaUtilization = (float) copyRead / masterRead;
//                totalCopy += replicaUtilization;
//                numofCopy++;
//            }
//        }
//        float averageReplicaUtilization = (float) totalCopy / numofCopy;
//        return averageReplicaUtilization;
//    }
    
    /**
     * This method calculate replication utilization rate. 
     * replication utilization rate is a metric for evaluation replication algorithms
     * @return value of replication utilization rate
     */
    public float replicaUtilizationRate() {
        float totalReplicatUtilization = 0;
        int numOfDataitems = 0;
        int nodeProcessCapacity = Simulator.getProcessCapacity();

        for (Map.Entry<Integer, List<DataitemInfo>> entry : map.entrySet()) {
            Integer dataitemId = entry.getKey();
            DataitemInfo dataitemInfo = entry.getValue().get(0);

            float u = 0;
            int replicaNum = 0;
            for (Map.Entry<Integer, Integer> entry1 : dataitemInfo.getReadReplicaMap().entrySet()) {
                Integer datacenterId = entry1.getKey();
                Integer numOfRead = entry1.getValue();

                if (numOfRead >= nodeProcessCapacity) {
                    u += 1;
                    replicaNum++;
                } else if (numOfRead <= 0) {
                    replicaNum++;
                } else if (numOfRead < nodeProcessCapacity && numOfRead > 0) {
                    u += ((float) numOfRead / nodeProcessCapacity);
                    replicaNum++;
                }
            }
            u = (float) u / replicaNum;
            totalReplicatUtilization += u;
            numOfDataitems++;
        }

        return ((float) totalReplicatUtilization / numOfDataitems);
    }

    ///////////////////////////////////////////////////////
    /**
     * This method calculate system availability. 
     * system availability is a metric for evaluation replication algorithms
     * @return value of system availability
     */
    public float systemAvailability() {
        double availability = 0;
        int dataitemNum = 0;
        float failureRate = Simulator.getFailureRate();

        for (Map.Entry<Integer, List<DataitemInfo>> entry : map.entrySet()) {
            Integer dataitemId = entry.getKey();
            DataitemInfo dataitemInfo = entry.getValue().get(0);

            int replicaNum = dataitemInfo.getReplicaNumber();
            //int blockNumber = dataitemInfo.getBlockes().size();
            int blockNumber = Simulator.getDataitemSize() / Simulator.getBlockSize();

//            double avai = 1 - (((Math.pow(failureRate, replicaNum)) * blockNumber)
//                    - ((Math.pow(failureRate, (replicaNum * 2))) * (selection(2, blockNumber))));
            double avai = dataAvalability(blockNumber, replicaNum, failureRate);

            availability += avai;
            dataitemNum++;
        }
        return (float) availability / dataitemNum;
    }

    /**
     * This method returns number of replica for each data in a map.
     * @return number of replica for each data in a map.
     */
    public Map<Integer, Integer> replicaNaumbers() {
        Map<Integer, Integer> replicaNum = new HashMap<>();
        for (Map.Entry<Integer, List<DataitemInfo>> entry : map.entrySet()) {
            Integer dataitemId = entry.getKey();
            DataitemInfo dataitemInfo = entry.getValue().get(0);

            replicaNum.put(dataitemId, dataitemInfo.getReplicaNumber());
        }
        return replicaNum;
    }

    /**
     * This method Clears the map
     */
    public void clear() {
        map.clear();
    }

    
    /**
     * It is the combination formula. It is a mathematical relationship.
     * This method is used for calculating data availability.
     * @param a a number
     * @param b b number
     * @return C(a , b)
     */
    public int selection(int a, int b) {
        return factorial(b) / (factorial(a) * factorial(b - a));
    }

    /**
     * factorial formula
     * @param a a number
     * @return a!
     */
    public int factorial(int a) {
        int f = 1;
        for (int i = 1; i <= a; i++) {
            f *= i;
        }
        return f;
    }

    /**
     * This method calculate data availability. 
     * data availability is a metric for evaluation replication algorithms
     * 
     * @param blocknumber number of blocks
     * @param replicanumber number of replicas
     * @param failurerate failure rate
     * @return value of data availability
     */
    public double dataAvalability(int blocknumber, int replicanumber, float failurerate) {

        double unava = 0;
        for (int j = 1; j <= blocknumber; j++) {
            unava += ((Math.pow(-1, (j + 1))) * selection(j, blocknumber)) * Math.pow(failurerate, replicanumber);
        }
        double ava = 1 - unava;
        return ava;
    }
}
