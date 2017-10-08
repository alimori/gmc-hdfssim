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

import hdfs.replicationsimulator.Daemon;
import hdfs.replicationsimulator.Event;
import hdfs.replicationsimulator.Simulator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;


/**
 * This class is a component which handle replication work.
 * It contained a thread namely ReplicationMonitor that is executed periodically.
 * According to the present situation which is obtained from the namenodes of 
 * each data center, it is assumed as a replication and executed by commands 
 * to the namenodes.
 * Number of replicas and placement are determined in this class.
 * You have to write Replication algorithm in this class
 *
 * @author AliMori
 */
public class ReplicationManager {

    /**
     * It stores all information about all data in all data centers.
     */
    List<DataitemsMap> dataitemsMapList; 
    /**
     * It stores information about replication work 
     */
    ReplicationMap replicationMap;
    //private List<Queue<Event>> toNamenodeList;

     /**
     * When the value of isRunning parameter is set to true, 
     * the replication manager starts.
     */
    boolean isRunning = false;

    /**
     * It is a thread for running the replication algorithm which is referred 
     * to the ReplicationManager class.
     */
    public Daemon replicationThread = null;

    /**
     * Replication period (millisecond)
     */
    long replicationRecheckInterval = 600 * 1000L; 
    /**
     * It stores traffic consumption. It's used for evaluation replication algorithm.
     */
    long trafficConsumption = 0L;

    //<period, dataitemid, numofread, numofreplica>
    

    //////////////////////////////////////////////////////////////////
     /**
     * It stores the space consumption information.
     */
    Map<Integer, Long> spaceConsumptionMap = new HashMap<>();
    /**
     * It stores the bandwidth usage information which is the result of movement
     * of data between data centers in each period.
     */
    Map<Integer, Long> trafficConsumptionMap = new HashMap<>();
     /**
     * It stores availability. It's used for evaluation replication algorithm.
     */
    Map<Integer, Float> availabilityMap = new HashMap<>();

    /**
     * The structure is in the shape of slider window which stores the 
     * information of the requests in the present and past periods. 
     * For estimating the future workload and new replication is used.
     */
    Map<Integer, Map<Integer, Map<Integer, Integer>>> querySlidingWindow = new HashMap<>();
     /**
     * The structure is in the shape of slider window which stores the 
     * information of the traffic in the present and past periods. 
     */
    Map<Integer, Map<Integer, Map<Integer, Integer>>> trafficSlidingWindow = new HashMap<>();
    //List<Map<Integer, Map<Integer, Map<Integer, Integer>>>> trafficSlidingWindow2 = new ArrayList<>();

    {
        for (int i = 0; i < 2; i++) {
            querySlidingWindow.put(i, new HashMap<Integer, Map<Integer, Integer>>());
            trafficSlidingWindow.put(i, new HashMap<Integer, Map<Integer, Integer>>());
            //trafficSlidingWindow2.add(i, new HashMap<Integer, Map<Integer, Map<Integer, Integer>>>());
        }
    }

    int periodCounter = 0;

//    float BETA = 2.0F;
//    float ALFA = 0.2F;
//    float GAMA = 1.5F;
//    float FI = 0.2F;
//    float MU = 1.0F;

    /**
     * Create an object of replication manager
     */
    public ReplicationManager() {
        replicationThread = new Daemon(new ReplicationMonitor());
        dataitemsMapList = new ArrayList<>();
        replicationMap = new ReplicationMap();
    }

    /**
     * This method starts replication manager
     */
    public void start() {
        isRunning = true;
        replicationThread.start();
    }

    /**
     * This is a thread which runs replication work periodically.
     */
    class ReplicationMonitor implements Runnable {

        /**
         * It stores information about number of queries for each data in current period.
         */
        Map<Integer, Map<Integer, Integer>> queryMap = new HashMap<>();
        /**
         * It stores information about number of queries for each data in previous period.
         */
        Map<Integer, Map<Integer, Integer>> previousQueryMap = new HashMap<>();
          /**
         * It stores information about amount of traffic for each data in current period.
         */
        Map<Integer, Map<Integer, Integer>> trafficMap = new HashMap<>();
          /**
         * It stores information about amount of traffic for each data in previous period.
         */
        Map<Integer, Map<Integer, Integer>> previousTrafficMap = new HashMap<>();
        //Map<Integer, Map<Integer, Map<Integer, Integer>>> traffisMaps = new HashMap<>();

        /**
         * The ReplicationMonitor thread is executed with this method and runs
         * the replication algorithm periodically.
         * The replication algorithm is written inside the 
         * processReplicationWork method.
         */
        @Override
        public void run() {

            try {
                Thread.sleep(10 * 1000L);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }

            while (isRunning) {

                System.out.println(" . ");
                System.out.println(" . ");
                System.out.println(" . ");
                System.out.println(" . ");
                System.out.println(" . . . . . REPLICATION SYSTEM PROCCESS THE RECENT WORKLOAD . . . . . ");
                System.out.println(" . ");
                System.out.println(" . ");
                System.out.println(" . ");
                System.out.println(" . ");

                proccessReplicationWork();
                periodCounter++;

                try {
                    Thread.sleep(replicationRecheckInterval);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }

        /**
         * Write your Replication algorithm in this method.
         */
        void proccessReplicationWork() {
            
            // write your replication algorithm here . . .

        }


        /**
         * It prints the system status including data about the present 
         * situation in each data center.
         * @param dataitemsMaps map which stores information of system state
         */
        void printSystemstate(List<DataitemsMap> dataitemsMaps) {

            System.out.println(" ");
            System.out.println("############ The sysyem state in recent period is : ");
            System.out.println(" ");
            long totalUseSpace = 0;
            int i = 0;
            synchronized (dataitemsMaps) {
                for (DataitemsMap dataitemsMap : dataitemsMaps) {

                    System.out.println(" Datacenter #" + i + " has : ");

                    int useSpace = 0;
                    synchronized (dataitemsMap) {
                        for (Map.Entry<Integer, DataitemInfo> entry : dataitemsMap.entrySet()) {
                            Integer dataitemId = entry.getKey();
                            DataitemInfo dataitemInfo = entry.getValue();

                            //System.out.println("Dataitem #" + dataitemId);
                            useSpace += dataitemInfo.getSize();
                        }
                    }
                    System.out.println(" Total consumption space in Datacenter #" + i + "is : " + useSpace);
                    System.out.println(" ");
                    System.out.println("##############################");
                    i++;
                    totalUseSpace += useSpace;
                }
            }
            System.out.println(" ");
            System.out.println("Total use space in system is : " + totalUseSpace);
            System.out.println("The use space percentage is : " + (float) totalUseSpace
                    / (Simulator.getNumberofdataitems() * Simulator.getDataitemSize() * 10));
            System.out.println("###################### finish the system state informations . ");
            System.out.println(" ");

            spaceConsumptionMap.put(periodCounter, totalUseSpace);

            //###########################################################
        }

        /**
         * This method prints the state of system, different by last method
         * @param dataitemsMaps map which stores information of system state
         */
        void printSystemState2(List<DataitemsMap> dataitemsMaps) {

            for (DataitemsMap dataitemsMap : dataitemsMaps) {

                for (Map.Entry<Integer, DataitemInfo> entry : dataitemsMap.entrySet()) {
                    Integer dataitemId = entry.getKey();
                    DataitemInfo dataitemInfo = entry.getValue();

                    System.out.println("Dataitem #" + dataitemId + "  ---->  "
                            + dataitemInfo.getDatacenters().size());
                }

            }
        }

        /**
         * This method prints number of read requests for each data from each regions
         * @param map 
         */
        void printReadsState(Map<Integer, Map<Integer, Integer>> map) {
            for (Map.Entry<Integer, Map<Integer, Integer>> entry : map.entrySet()) {
                Integer dataitemId = entry.getKey();
                Map<Integer, Integer> readMap = entry.getValue();

                System.out.println(" ");
                System.out.println("Dataitem #" + dataitemId + " : ");
                System.out.println("Regions : " + 0 + "     " + 1 + "     " + 2 + "     " + 3 + "     "
                        + 4 + "     " + 5 + "     " + 6 + "     " + 7 + "     " + 8 + "     " + 9 + "     ");
                System.out.print("Reads :   ");

                int num = 0;
                for (Map.Entry<Integer, Integer> entry1 : readMap.entrySet()) {
                    Integer datacenterId = entry1.getKey();
                    Integer numberOfRead = entry1.getValue();

                    System.out.print(numberOfRead + "     ");
                    num += numberOfRead;
                }
                System.out.println(" ");
                System.out.println("Total Reads is : " + num);
            }
        }


        //##################################################################################################

        /**
         * This method implements the basic and typical owner based replication algorithm
         * @param ownerDatacenterId ID of data center holding the primary data
         * @param minReplica minimum number of replica
         * @return 
         */
        Map<Integer, Datacenter> ownerBasedPlacement(int ownerDatacenterId, int minReplica) {

            // answer is save in this
            Map<Integer, Datacenter> choosenDatacenters = new HashMap<>();

            Datacenter ownerDatacenter = Simulator.getDatacentersTopology().getDatacenters().
                    get(ownerDatacenterId);

            //choosenDatacenters.put(ownerDatacenterId, ownerDatacenter);
            Queue<Datacenter> queue = new LinkedList<>();
            queue.add(ownerDatacenter);
            Datacenter element;
            while (choosenDatacenters.size() < minReplica) {
                element = queue.remove();

                Map<Integer, Datacenter> neighbours = new HashMap<>();

                for (Datacenter datacenter : element.getNeighbours()) {
                    if (!choosenDatacenters.containsKey(datacenter.getId())) {
                        neighbours.put(datacenter.getId(), datacenter);
                    }
                }
                while (!neighbours.isEmpty()) {
                    long min = 0;
                    Datacenter choose = null;

                    for (Map.Entry<Integer, Datacenter> entry : neighbours.entrySet()) {
                        Integer datacenterId = entry.getKey();
                        Datacenter datacenter = entry.getValue();
                        if (datacenter.getNeighbours().size() > min) {
                            min = datacenter.getNeighbours().size();
                            choose = datacenter;
                        }
                    }
                    queue.add(choose);
                    neighbours.remove(choose.getId());
                }
                choosenDatacenters.put(queue.peek().getId(), queue.peek());
            }
            return choosenDatacenters;
        }


 

        //##########################################################################
        /**
         * It returns current data centers which hold a replica of data
         * @param dataitemInfos List of DataitemInfos
         * @return current data centers which hold a replica of data
         */
        Map<Integer, Datacenter> getSourceDatacenters(List<DataitemInfo> dataitemInfos
        ) {
            Map<Integer, Datacenter> sources = new HashMap<>();
            for (Map.Entry<Integer, Datacenter> entry : dataitemInfos.get(0).getDatacenters().entrySet()) {
                Integer datacenterId = entry.getKey();
                Datacenter datacenter = entry.getValue();

                sources.put(datacenterId, datacenter);
            }
            return sources;
        }

        
        //###########################################################################
          /**
         * Using this method data replicate from source data center to destination 
         * data center.
         * For this method, the value of CutDatacenter should get -1.
         * This means that the data isn't removed from the source data center.
         * @param dataitemId ID of data item
         * @param source source data center
         * @param destination destination data center
         * @param CutDatacenter data center which data must be removed from it. set
         * -1 in this method.
         */
        void replicateDataitem(int dataitemId, Datacenter source, Datacenter destination, int CutDatacenter
        ) {
            Simulator.getToNamenode(source.getId()).add(
                    new Event(Event.REPLICATE_DATAITEM, dataitemId, source.getId(), destination.getId(),
                            CutDatacenter));

            long traffic = Simulator.getDataitemSize() * DatacentersTopology.getDistance(source.getId(),
                    destination.getId());

            System.out.println("DataItem #" + dataitemId + " ---> is going to Replicate from Datacebter #"
                    + source.getId() + " to Datacenter #" + destination.getId() + " ---> use Traffic is : "
                    + traffic);
            trafficConsumption += traffic;
        }

        //###########################################################################
        /**
         * Using this method data migrate from source data center to destination 
         * data center.
         * For this method, the value of CutDatacenter should get the source
         * data center ID. This means that the data is removed from the data 
         * center after being sent and receiving ack.
         * @param dataitemId ID of data item
         * @param source source data center
         * @param destination destination data center
         * @param CutDatacenter data center which data must be removed from it
         */
        void migrateDataitem(int dataitemId, Datacenter source, Datacenter destination, int CutDatacenter
        ) {
            Simulator.getToNamenode(source.getId()).add(
                    new Event(Event.REPLICATE_DATAITEM, dataitemId, source.getId(), destination.getId(),
                            CutDatacenter));

            long traffic = Simulator.getDataitemSize() * DatacentersTopology.getDistance(source.getId(),
                    destination.getId());

            System.out.println("DataItem #" + dataitemId + " ---> is going to Migrate from Datacebter #"
                    + source.getId() + " to Datacenter #" + destination.getId() + " ---> use Traffic is : "
                    + traffic);
            trafficConsumption += traffic;
        }

        //###########################################################################
        /**
         * This method remove data item from a data center
         * @param dataitemId ID of data item
         * @param datacenter ID of data center
         */
        void removeDataitem(int dataitemId, Datacenter datacenter
        ) {
            Simulator.getToNamenode(datacenter.getId()).add(new Event(Event.REMOVE_DATAITEM,
                    dataitemId, datacenter.getId()));
            System.out.println("DataItem #" + dataitemId + " ---> is going to Remove from Datacebter #"
                    + datacenter.getId());
        }
      
    }
    
    
    

    public DataitemInfo getDataitemInfo(int dataitemId) {
        for (DataitemsMap dataitemsMap : dataitemsMapList) {
            if (dataitemsMap.contain(dataitemId)) {
                return dataitemsMap.getDataitemInfo(dataitemId);
            }
        }
        return null;
    }

    public long getTrafficConsumption() {
        return trafficConsumption;
    }

    public void setTrafficConsumption(long trafficConsumption) {
        this.trafficConsumption = trafficConsumption;
    }

   

    ///////////////////////////////////////////////////////////////
    /**
     * It prints information about traffic consumption
     */
    public void printTrafficConsumption() {
        System.out.println("************* TRAFFIC CONSUMPTION ***********************");

        double total = 0;
        int num = 0;

        File responseTimeResult = new File("Results\\TrafficConsumption.log");

        FileWriter fw;
        try {
            fw = new FileWriter(responseTimeResult);
            try (BufferedWriter bw = new BufferedWriter(fw)) {
                for (Map.Entry<Integer, Long> entry : trafficConsumptionMap.entrySet()) {
                    Integer period = entry.getKey();
                    Long traffic = entry.getValue();

                    System.out.println("Period #" + period + " : " + traffic + " MagaByte.");

                    bw.write("Period #" + period + " : " + traffic + " MagaByte.");
                    bw.newLine();

                    if (period != 0) {
                        total += (double) traffic;
                        num++;
                    }
                }
                System.out.println("Total traffic consumption is : " + total + " MagaByte.");
                System.out.println("Average traffic consumption is : " + (double) total / num + " MegaByte.");
                bw.newLine();
                bw.write("Total traffic consumption is : " + total + " MagaByte.");
                bw.newLine();
                bw.write("Average traffic consumption is : " + (double) total / num + " MegaByte.");

                bw.close();
            }

        } catch (IOException ex) {
            System.out.println("ERROR : WRITE IN TRAFFIC CONSUMPTION FILE");
        }
        System.out.println("****************************************************");
    }

      /**
     * It prints information about space consumption
     */
    public void printSpaceConsumption() {
        System.out.println("************* SPACE CONSUMPTION ***********************");

        float persentage = 0;
        double total = 0;
        int num = 0;

        File responseTimeResult = new File("Results\\SpaceConsumption.log");

        FileWriter fw;
        try {
            fw = new FileWriter(responseTimeResult);
            try (BufferedWriter bw = new BufferedWriter(fw)) {
                for (Map.Entry<Integer, Long> entry : spaceConsumptionMap.entrySet()) {
                    Integer period = entry.getKey();
                    Long space = entry.getValue();

                    System.out.println("Period #" + period + " : " + space + " MagaByte.");
                    System.out.println("Persentage : " + (float) space
                            / (Simulator.getNumberofdataitems() * Simulator.getDataitemSize() * 10));

                    bw.write("Period #" + period + " : " + space + " MagaByte.");
                    bw.write("  and Persentage : " + (float) space / (Simulator.getNumberofdataitems()
                            * Simulator.getDataitemSize() * 10) + " %.");
                    bw.newLine();

                    if (period != 0) {
                        persentage += (float) (Simulator.getNumberofdataitems() * Simulator.getDataitemSize() * 10)
                                / space;
                        total += (double) space;
                        num++;
                    }
                }

                System.out.println("Average space consumption is : " + (double) total / num + " MegaByte.");
                System.out.println("Average persentage is : " + (float) num / persentage);
                System.out.println("Average Space utilization is : " + (float) persentage / num);

                bw.newLine();
                bw.write("Average space consumption is : " + (double) total / num + " MegaByte.");
                bw.newLine();
                bw.write("Average persentage is : " + (float) num / persentage);
                bw.newLine();
                bw.write("Average Space utilization is : " + (float) persentage / num);

                bw.close();
            }

        } catch (IOException ex) {
            System.out.println("ERROR : WRITE IN SPACE CONSUMPTION FILE");
        }
        System.out.println("****************************************************");
    }

  

    //############################################
    /**
     * It prints information about system availability
     */
    public void printSystemAvailability() {
        System.out.println("************* SYSTEM AVAILABILITY ***********************");

        float total = 0;
        int num = 0;

        File availability = new File("Results\\Availability.log");

        FileWriter fw;
        try {
            fw = new FileWriter(availability);
            try (BufferedWriter bw = new BufferedWriter(fw)) {
                for (Map.Entry<Integer, Float> entry : availabilityMap.entrySet()) {
                    Integer period = entry.getKey();
                    Float sysAvailability = entry.getValue();

                    System.out.println("Period #" + period + " : " + sysAvailability);

                    bw.write("Period #" + period + " : " + sysAvailability);
                    bw.newLine();

                    if (period != 0) {
                        total += sysAvailability;
                        num++;
                    }
                }
                System.out.println("System Availability is : " + (float) total / num);
                bw.newLine();
                bw.write("System Availability is : " + (float) total / num);
                bw.close();
            }

        } catch (IOException ex) {
            System.out.println("ERROR : WRITE IN AVAILABILITY FILE");
        }
        System.out.println("****************************************************");
    }

 


}
