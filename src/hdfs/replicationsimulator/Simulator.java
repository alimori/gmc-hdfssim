package hdfs.replicationsimulator;

import gmc_hdfs.replicationsimulator.Datacenter;
import gmc_hdfs.replicationsimulator.DatacentersTopology;
import gmc_hdfs.replicationsimulator.Dataitem;
import gmc_hdfs.replicationsimulator.DataitemInfo;
import gmc_hdfs.replicationsimulator.ReplicationManager;
import gmc_hdfs.replicationsimulator.Request;
import gmc_hdfs.distribution.StdRandom;
import gmc_hdfs.replicationsimulator.User;
import gmc_hdfs.replicationsimulator.Workload;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Object used to Initialize the system. Create all the objects and processes.
 * 
 * @author in the main source of this class was not written name of author,
 * this class manipulate by Ali Mortazavi
 */
public class Simulator {


    private volatile static Map<Integer, Map<Integer, Integer>> failDatanodes;

    private static DatacentersTopology datacentersTopology;
    private static ReplicationManager replicationManager;

    public static List<Queue<Event>> toNamenodeList;
    public static List<Queue<Event>> toDatanodesList;
    public static List<Queue<Request>> requestsList;

    private static List<SimTrace> traceList;

    /*
     * Settings (defaults)
     */
    private static int numberofdataitems = 10;
    private static int dataitemSize = 640;
    private static int numberOfUsers = 10;
    private static int numberofDatanodes = 10;
    private static int dataNodeCapacity = 320000;
    private static int bandwidth = 1024;
    private static int heartbeat = 2000;
    private static int timeout = 3;
    private static int blockSize = 64;
    private static int minNumberOfReplica = 2;
    private static long failureInterval = 50 * 1000L;
    private static float failureRate = 0.01F;
    private static int processCapacity = 5;

    private static Random random = new Random();

    private static List<Event> failureEventsSimulation = new ArrayList<>();
    private static List<Request> workloadSimulation = new ArrayList<>();

    public static void init(String configFile) {

        // Read the test.txt file: CHANGE THE NAME!
        try {
            FileReader fr = new FileReader(configFile);
            BufferedReader in = new BufferedReader(fr);
            String data;


            traceList = new ArrayList<>();

            while ((data = in.readLine()) != null) {

                if (data.contains("numberOfDatanodes=")) {
                    numberofDatanodes = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("bandwidth=")) {
                    bandwidth = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("heartbeat=")) {
                    heartbeat = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("timeout=")) {
                    timeout = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("blockSize=")) {
                    blockSize = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("numberOfUsers=")) {
                    numberOfUsers = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("datanodeCapacity=")) {
                    dataNodeCapacity = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("numberOfDataitems=")) {
                    numberofdataitems = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("dataitemSize=")) {
                    dataitemSize = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("failureInterval=")) {
                    failureInterval = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("processCapacity=")) {
                    processCapacity = Integer.parseInt(data.split("=")[1]);
                } else if (data.contains("failureRate=")) {
                    failureRate = Float.valueOf(data.split("=")[1]);
                } else if (data.contains("minNumberOfReplica=")) {
                    minNumberOfReplica = Integer.parseInt(data.split("=")[1]);
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        failDatanodes = new HashMap<>();

        replicationManager = new ReplicationManager();
        //datacentersTopology = new DatacentersTopology();
        toNamenodeList = new ArrayList<>();
        toDatanodesList = new ArrayList<>();
        requestsList = new ArrayList<>();
        datacentersTopology = new DatacentersTopology();
        for (Datacenter d : datacentersTopology.getDatacenters()) {

            initializeDatacenter(datacentersTopology.getDatacenters().indexOf(d), numberofDatanodes);
            initializeUser(datacentersTopology.getDatacenters().indexOf(d), numberOfUsers);
        }

        int firstDataitemId = numberofdataitems;
        for (Datacenter datacenter : datacentersTopology.getDatacenters()) {
            initializeDataitems(datacenter, numberofdataitems, dataitemSize, (firstDataitemId - numberofdataitems));
            firstDataitemId += numberofdataitems;
        }


        failureInterval = (long) ((1 / failureRate) * 1000);
        failureEventsSimulation = failureDistribution(failureInterval);
        //workloadSimulation = workloadGenerateSimulation();

    }

    private static void initializeDatacenter(int id, int numberOfDatanodes) {
        //datacenter = new Datacenter(id);
        System.out.println("***************************************************");
        for (int i = 0; i < numberOfDatanodes; i++) {
            //allDatanodes.addNode(new Datanode(id,i, dataNodeCapacity));

            //datacenter.getAllDatanode().addNode(new Datanode(id, i, dataNodeCapacity)); // khodam
            datacentersTopology.getDatacenters().get(id).getAllDatanode().addNode(new Datanode(id, i, dataNodeCapacity));
            DatanodeInfo datanodeInfo = new DatanodeInfo(id, i, dataNodeCapacity);
            //namenode.addNode(dataitemInfo);
            datacentersTopology.getDatacenters().get(id).getNamenode().addNode(datanodeInfo);
//            System.out.println("Datanode #" + datanodeInfo.getDatacenterId() + "_" + datanodeInfo.getId()
//                    + "     Capacity: " + datanodeInfo.getCapacity());
        }

        System.out.print("In Datacenter #" + id + ", " + numberOfDatanodes + " Datanodes Created.\n");

    }

    private static void initializeUser(int datacenterId, int numberOfUsers) {
        for (int i = 0; i < numberOfUsers; i++) {
            datacentersTopology.getDatacenters().get(datacenterId).getAllUsers().addUser(new User(datacenterId, i));
            //System.out.println("User #" + datacenterId + "_" + i);
        }
        System.out.print("In Datacenter #" + datacenterId + ", " + numberOfUsers + " Users Created.\n");
    }

    // methode zir ba arghame zir 75000 ta block ra dar beyn data node ha pakhsh mikonad
    private static void initializeDataitems(Datacenter datacenter, int dataitemNum,
            int dataitemSize, int dataitemId) {

        System.out.println("################################################################################");
        System.out.println();

        Datacenter neighbor = datacentersTopology.getPrimaryNeighbor(datacenter);
        int currentDN = 0;

        for (int j = 0; j < dataitemNum; j++) {
            Dataitem dataitem = new Dataitem(dataitemId, dataitemSize);
            dataitem.dataitemToBlocks();

            DataitemInfo dataitemInfo = new DataitemInfo(dataitemId, dataitemSize);
            dataitemInfo.addToDatacentersList(datacenter);
            dataitemInfo.addToMasterCopyMap(datacenter.getId(), "master");
            dataitemInfo.addToRequestTraffic(datacenter.getId(), new HashMap<Integer, Integer>());
            dataitemInfo.addToReplicaReadMap(datacenter.getId(), 0);
            dataitemInfo.setCapacity(datacenter.getId(), processCapacity);
            datacenter.getNamenode().addDataitem(dataitemInfo);

            for (Block b : dataitem.getBlocks()) {
                Block block = new BlockInfo(dataitem.getId(), b.getId(), blockSize);
                dataitemInfo.addBlock((BlockInfo) block);

                int idDatanode = currentDN;
                Datanode dn = datacenter.getAllDatanode().getNode(idDatanode);
                dn.addBlock(b);
                datacenter.getNamenode().initAddBlock(dn, (BlockInfo) block);

//                
                currentDN = (currentDN == numberofDatanodes - 1) ? 0 : currentDN + 1;
            }
            dataitemId++;
        }

        System.out.print("In Datacenter #" + datacenter.getId() + ": " + dataitemNum + " Dataitems blocking to "
                + dataitemNum * Math.ceil((double) dataitemSize / (double) blockSize)
                + "and distributed\n");
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    //  methode zir methode tozi failure ha be soorate random dar favasele zamani monazam
    // har 100 sanie yek bar ast
    private static List<Event> failureDistribution(long failureInterval) {
        List<Event> failures = new ArrayList<>();

        // zaman shoro
        long failTime = 10 * 1000L; // millisecond

        // milisecond to second
        double time = (double) failureInterval / 1000;

        while (failTime < Main.getSimulationDuration() - (200 * 1000)) {

            failures.add(new Event(random.nextInt(datacentersTopology.size()), random.nextInt(numberofDatanodes),
                    Event.FAILURE, failTime));

            //failTime += (StdRandom.poisson(time) * 1000);
            failTime += ((time) * 1000);
        }
        return failures;
    }

    private static void startFailure() {
        Thread killer = new Thread(new NodeKiller());
        killer.start();
    }

    //***************************************************** WORKLOAD   :::::
    private static List<Request> workloadGenerateSimulation() {
        List<Request> requests;

        //1- Exprement 1: 
        requests = flashCrowedWorkload();
        //2- Exprement 2:
        //requests = slashdotWorkload();
        return requests;
    }

    ////////////////////////////////////////
    private static List<Request> slashdotWorkload() {
        List<Request> requests = new ArrayList<>();
        long requestId = 0;

        int period = (int) Main.getSimulationDuration() / 3;

        int time = 0;
        User user;
        int dataitemId;
        while (time <= period) {
            time += StdRandom.poisson(50);
            user = datacentersTopology.getDatacenters().get(StdRandom.uniform(0, datacentersTopology.size()
            )).getAllUsers().getUsers().get(StdRandom.uniform(0, numberOfUsers));
            dataitemId = (int) StdRandom.boundedPareto(1.161, 0, numberofdataitems
                    * datacentersTopology.size());

            requests.add(user.readDataitem(requestId++, dataitemId, (long) time));
        }

        while (time <= 2 * period) {
            time += StdRandom.poisson(5);
            user = datacentersTopology.getDatacenters().get(StdRandom.uniform(0, datacentersTopology.size()
            )).getAllUsers().getUsers().get(StdRandom.uniform(0, numberOfUsers));
            dataitemId = (int) StdRandom.boundedPareto(1.161, 0, numberofdataitems
                    * datacentersTopology.size());

            requests.add(user.readDataitem(requestId++, dataitemId, (long) time));
        }

        while (time <= 3 * period) {
            time += StdRandom.poisson(50);
            user = datacentersTopology.getDatacenters().get(StdRandom.uniform(0, datacentersTopology.size()
            )).getAllUsers().getUsers().get(StdRandom.uniform(0, numberOfUsers));
            dataitemId = (int) StdRandom.boundedPareto(1.161, 0, numberofdataitems
                    * datacentersTopology.size());

            requests.add(user.readDataitem(requestId++, dataitemId, (long) time));
        }

        Collections.sort(requests);
        return requests;
    }

    //////////////////////////////////////////
    private static List<Request> flashCrowedWorkload() {
        List<Request> requests = new ArrayList<>();
        long requestId = 0;

        int period = (int) Main.getSimulationDuration() / 2;

        int failTime = 0;
        User user;
        int dataitemId;

        long start = System.currentTimeMillis();
        while (failTime <= period) {
            failTime += StdRandom.poisson(10);
            user = datacentersTopology.getDatacenters().get(StdRandom.uniform(7, 10)).
                    getAllUsers().getUsers().get(StdRandom.uniform(0, numberOfUsers));
            dataitemId = (int) StdRandom.boundedPareto(1.161, 0, numberofdataitems
                    * datacentersTopology.size());

            requests.add(user.readDataitem(requestId++, dataitemId, (long) failTime));
        }

        while (failTime <= (2 * period)) {
            failTime += StdRandom.poisson(10);
            user = datacentersTopology.getDatacenters().get(StdRandom.uniform(1, 3)).
                    getAllUsers().getUsers().get(StdRandom.uniform(0, numberOfUsers));
            dataitemId = (int) StdRandom.boundedPareto(1.161, 0, numberofdataitems
                    * datacentersTopology.size());

            requests.add(user.readDataitem(requestId++, dataitemId, (long) failTime));
        }

//        failTime = 0;
//        while (failTime <= 2 * period) {
//            failTime += StdRandom.poisson(50);
//            user = datacentersTopology.getDatacenters().get(StdRandom.uniform(0, datacentersTopology.size()
//                    - 1)).getAllUsers().getUsers().get(StdRandom.uniform(0, numberOfUsers - 1));
//            dataitemId = (int) StdRandom.boundedPareto(1.161, 0, numberofdataitems
//                    * datacentersTopology.size() - 1);
//
//            requests.add(user.readDataitem(requestId++, dataitemId, (long) failTime));
//        }
        Collections.sort(requests);
        return requests;
    }

    ///////////////////////////////////////////
    private static void startWorkload() {
        Thread workload = new Thread(new Workload());
        workload.start();
    }

    public static void start() {
        datacentersTopology.start();
        replicationManager.start();
        startWorkload();
        startFailure();
    }

    ///////////####################################################### 
    public static int getBandwidth() {
        return bandwidth;
    }

    public static int getHeartbeat() {
        return heartbeat;
    }

    public static int getTimeout() {
        return timeout;
    }

    public static int getBlockSize() {
        return blockSize;
    }

//    public static int getNumberofReplicas() {
//        return numberofReplicas;
//    }
    public static List<Queue<Event>> getToDatanodesList() {
        return toDatanodesList;
    }

    public static List<Queue<Event>> getToNamenodeList() {
        return toNamenodeList;
    }

    public static int getNumberOfUsers() {
        return numberOfUsers;
    }

    public static int getNumberofDatanodes() {
        return numberofDatanodes;
    }

    public static int getMinNumberOfReplica() {
        return minNumberOfReplica;
    }

    public static List<Queue<Request>> getRequestsList() {
        return requestsList;
    }

    public static Queue<Event> getToNamenode(int datacenterId) {
        return toNamenodeList.get(datacenterId);
    }

    public static Queue<Event> getToDatanodes(int datacenterId) {
        return toDatanodesList.get(datacenterId);
    }

    public static Queue<Request> getRequests(int datacenterId) {
        return requestsList.get(datacenterId);
    }

    public static Namenode getNamenode(int datacenterId) {
        return datacentersTopology.getDatacenters().get(datacenterId).getNamenode();
    }

    public static AllDatanode getAllDatanodes(int datacenterId) {
        return datacentersTopology.getDatacenters().get(datacenterId).getAllDatanode();
    }

    public static DatacentersTopology getDatacentersTopology() {
        return datacentersTopology;
    }

    public static ReplicationManager getReplicationManager() {
        return replicationManager;
    }

    public static List<SimTrace> getTraceList() {
        return traceList;
    }

    public static void addTrace(SimTrace st) {
//        if(st.getAction()==SimTrace.BLOCK_RECEIVED){
//            
//        }
        traceList.add(st);
        System.out.println(st.toString());
    }

    public static void printResults() {

        File f = new File("Results\\FailureResults.log");
        FileWriter fw;
        try {
            fw = new FileWriter(f);
            try (BufferedWriter bw = new BufferedWriter(fw)) {

                int dataLosses = 0;

                List<Long> ttrs = new ArrayList<>();
                Map<List<Integer>, Long> failureEvents = new HashMap<>();

                synchronized (traceList) {
                    for (SimTrace st : traceList) {
                        bw.write(st.toString());
                        bw.newLine();

                        if (st.isLossEvent()) {
                            dataLosses++;
                        }

                        if (st.getAction() == SimTrace.FAILURE_DETECTION) {
                            List<Integer> key = new ArrayList<>(Arrays.asList(st.getDatacenterId(), st.getId()));
                            failureEvents.put(key, st.getTimestamp());
                        } else if (st.getAction() == SimTrace.DATANODE_REPEARED) {

                            if (st.getTtr() != 0) {
                                ttrs.add(st.getTtr());
                            }

//                    long failTime = 0;
//                    failTime = failureEvents.get(st.getIdBlock());
//                    
//                    if (failTime != 0) {
//                        long ttr = st.getTimestamp() - failTime;
//                        ttrs.add(ttr);
//                    }
                        }
                    }
                }

                bw.newLine();
                bw.write(processReliability(ttrs));
                bw.newLine();
                bw.write(processAvailability(ttrs));
                bw.newLine();
                bw.write(processMTTR(ttrs));
                bw.newLine();
                bw.write(processDurability(dataLosses));

                bw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    ////////////////////////////////////////////////////////////
    private static String processDurability(int losses) {
        System.out.println("########################  DURABILITY :");
        //double durability = 1 - (losses / numberofBlocks);
        double blockNum = numberofdataitems * Math.ceil((double) dataitemSize / (double) blockSize) * 10;
        double durability = (1 - (losses / blockNum)) * 100;
        System.out.println("Number of losses is : " + losses);
        System.out.println("And Durabillity is : " + durability);

        return "DURABILITY: " + durability;
    }

    private static String processReliability(List<Long> ttrs) {

        System.out.println("########################  RELIABILITY :");
        double e = 2.71828182846;
        double reliability;

        int numOfFailnodes = ttrs.size();
        double downTime = 0;
        for (Long l : ttrs) {
            downTime += l;
        }

        double upTime = (numberofDatanodes * getDatacentersTopology().size()
                * Main.getSimulationDuration()) - downTime;

        double MTTF = upTime / numOfFailnodes;

        double lambda = (double) 1 / MTTF;

        reliability = (Math.pow(e, -lambda * Main.getSimulationDuration())) * 100;

        System.out.println("And Reliability is : " + reliability);
        return "RELIABILITY : " + reliability;
    }

    private static String processAvailability(List<Long> ttrs) {
        // Availability = upTime / TotalTime = MTTF / MTTF + MTTR
        System.out.println("########################  AVAILABILITY :");
        double availability;

        int numOfFailnodes = ttrs.size();
        double downTime = 0;
        for (Long l : ttrs) {
            downTime += l;
        }

        double upTime = (numberofDatanodes * getDatacentersTopology().size()
                * Main.getSimulationDuration()) - downTime;

        double MTTR = downTime / numOfFailnodes;
        double MTTF = upTime / numOfFailnodes;

        availability = (MTTF / (MTTF + MTTR)) * 100;

        System.out.println("UpTime is : " + upTime);
        System.out.println("DownTime is : " + downTime);
        System.out.println("MTTF is : " + MTTF);
        System.out.println("MTTR is : " + MTTR);
        System.out.println("And Availability is : " + availability);

        return "AVALABILITY : " + availability;
    }

    private static String processMTTR(List<Long> ttrs) {

        System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%% MTTR :");
        double sum = 0;
        for (Long l : ttrs) {
            sum += l;
            System.out.println(" ttr: " + l);
        }
        double mttr = sum / ttrs.size();

        System.out.println("##### mttr size is :" + ttrs.size() + ", MTTR: " + mttr);

        return "MTTR: " + mttr;
    }

    public static List<Event> getSimulationFailureEvents() {
        return failureEventsSimulation;
    }

    public static List<Request> getWorkloadSimulation() {
        return workloadSimulation;
    }

    public static void killNode(int datacenterId, int nodeId) {
        datacentersTopology.getDatacenters().get(datacenterId).getAllDatanode().killNode(nodeId);
        addTrace(new SimTrace("Datanode #" + datacenterId + "_" + nodeId + " has failed . . ."));
    }

    public static void addToRequestsList(Request request) {
//        System.out.println(" . ");
//        System.out.println("User #" + request.getUser().getDatacenterId() + "_" + request.getUser().getId()
//                + " Read the Dataitem #" + request.getDataitemID());
        requestsList.get(request.getUser().getDatacenterId()).add(request);
    }

//    public List<Request> sortByTime(List<Request> input) {
//        List<Request> sorted = new ArrayList<>();
//        return sorted;
//    }
    public static int getNumberofdataitems() {
        return numberofdataitems;
    }

    public static int getDataitemSize() {
        return dataitemSize;
    }

    public static int getProcessCapacity() {
        return processCapacity;
    }

    public static float getFailureRate() {
        return failureRate;
    }

    public static Map<Integer, Map<Integer, Integer>> getFailDatanodes() {
        return failDatanodes;
    }

    public static boolean isRepearDatanode(DatanodeInfo datanodeInfo) {
        if (failDatanodes.get(datanodeInfo.getDatacenterId()).get(datanodeInfo.getId()) == 0) {
            return true;
        }
        return false;
    }

    public static void addFailDatanode(DatanodeInfo datanodeInfo, int numOfFailBlocks) {
        Map<Integer, Integer> failed = new HashMap<>();
        failed.put(datanodeInfo.getId(), numOfFailBlocks);

        synchronized (failDatanodes) {
            failDatanodes.put(datanodeInfo.getDatacenterId(), failed);
        }
    }

    public static void blockRepeared(DatanodeInfo datanodeInfo) {
        synchronized (failDatanodes) {
            Map<Integer, Integer> failed = failDatanodes.get(datanodeInfo.getDatacenterId());
            int num = failed.get(datanodeInfo.getId());
            num--;
            failed.put(datanodeInfo.getId(), num);
            failDatanodes.put(datanodeInfo.getDatacenterId(), failed);
        }
    }

}
