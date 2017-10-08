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

import static gmc_hdfs.replicationsimulator.DatacentersTopology.RoutingTables;
import hdfs.replicationsimulator.AllDatanode;
import hdfs.replicationsimulator.Event;
import hdfs.replicationsimulator.Namenode;
import hdfs.replicationsimulator.Simulator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The data center is a resource for distributed storing systems such as cloud 
 * storages which integrates several storing nodes. These nodes are connected 
 * in the data center. The data is stored in the nodes.
 *
 * @author AliMori
 */
public class Datacenter {

    /**
     * When the value of isRunning parameter is set to true, the data center starts.
     */
    boolean isRunning = false;
    /**
     * id for data center
     */
    private int id;
    /**
     * The data center is geographically is the area which geographicalZone shows.
     */
    private GeographicalZone geographicalZone;
    /**
     * At the edge of each data center, there is a router which sends the 
     * requests or the answer to the requests to the other data centers.
     */
    private Router router;
    /**
     * The coordinating node of data center saves the metadata of its cluster such 
     * as the information about the mapping between data blocks and storing nodes.
     */
    private Namenode namenode;
    /**
     * AllDatanode integrates the existing nodes in the data center.
     */
    private AllDatanode allDatanode;
    /**
     * The users who are close to the data center are placed in the geographical 
     * region which AllUsers integrates them in one set.
     */
    private AllUsers allUsers;
    /**
     * Each data center is directly connected to one or more data centers 
     * that are its neighbors.
     */
    private List<Datacenter> neighboureDatacenters;

   // private int bandwith = 1024 * 1024; // bit/s
  //  private int transferTime;

    /**
     * Create a new object of Datacenter
     * @param id id for data center
     */
    public Datacenter(int id) {
        System.out.println("******** Datacenter #" + id + " *********");
        this.id = id;

        Simulator.toNamenodeList.add(id, new ConcurrentLinkedQueue<Event>());
        Simulator.toDatanodesList.add(id, new ConcurrentLinkedQueue<Event>());
        Simulator.requestsList.add(id, new ConcurrentLinkedQueue<Request>());

        this.namenode = new Namenode(id);

        this.allDatanode = new AllDatanode(id);

        this.allUsers = new AllUsers(id);

        this.neighboureDatacenters = new ArrayList<>();

        this.router = new Router(id);

        System.out.print("Datacenter #" + id + " Created.\n");
        System.out.println();

    }

    /**
     * Create a new object of Datacenter
     * 
     * @param id id for data center
     * @param geographicalZone The data center is geographically is the area which geographicalZone shows.
     */
    public Datacenter(int id, GeographicalZone geographicalZone) {
        this.id = id;
        this.geographicalZone = geographicalZone;
         System.out.println("******** Datacenter #" + id + " *********");
     

        Simulator.toNamenodeList.add(id, new ConcurrentLinkedQueue<Event>());
        Simulator.toDatanodesList.add(id, new ConcurrentLinkedQueue<Event>());
        Simulator.requestsList.add(id, new ConcurrentLinkedQueue<Request>());

        this.namenode = new Namenode(id);

        this.allDatanode = new AllDatanode(id);

        this.allUsers = new AllUsers(id);

        this.neighboureDatacenters = new ArrayList<>();

        this.router = new Router(id);

        System.out.print("Datacenter #" + id + " Created.\n");
        System.out.println();

    }

    /**
     * This method starts data center 
     */
    public void start() {
        isRunning = true;
        allDatanode.start();
        namenode.start();
        router.start();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Namenode getNamenode() {
        return namenode;
    }

    public void setNamenode(Namenode namenode) {
        this.namenode = namenode;
    }

    public AllDatanode getAllDatanode() {
        return allDatanode;
    }

    public void setAllDatanode(AllDatanode allDatanode) {
        this.allDatanode = allDatanode;
    }

    public AllUsers getAllUsers() {
        return allUsers;
    }

    public GeographicalZone getGeographicalZone() {
        return geographicalZone;
    }

    public void setGeographicalZone(GeographicalZone geographicalZone) {
        this.geographicalZone = geographicalZone;
    }

    public List<Datacenter> getNeighbours() {
        return neighboureDatacenters;
    }

    public void setNeighbours(List<Datacenter> linked) {
        this.neighboureDatacenters = linked;
    }

    public void addToNeighbourDatacenterList(Datacenter datacenter) {
        neighboureDatacenters.add(datacenter);
    }

    public Router getRouter() {
        return router;
    }

    //***************** class router **************************************
    
    /**
     * The routers do the routing in the data centers. They are used for the 
     * routing of the requests and their answers and displacement of data 
     * between data centers.
     */
    public class Router {

        /**
         * ID for router
         */
        int id;
        /**
         * The routing table has two columns and contains the ID of the 
         * destination data center and the next data center ID.
         */
        Map<Integer, Integer> routingTable;  // <distination datacenter Id , next datacenter Id>
        
        /**
         * When the value of isRunning parameter is set to true, the router starts.
         */
        boolean isRunning = false;

        /**
         * Create a new object of Router
         * @param id ID for router
         */
        public Router(int id) {
            this.id = id;
            routingTable = new HashMap<>();
        }

        public Map<Integer, Integer> getRoutingTable() {
            return routingTable;
        }

        /**
         * This method starts the router
         */
        public void start() {
            isRunning = true;
            for (int i = 0; i < 10; i++) {
                routingTable.put(i, DatacentersTopology.RoutingTables()[id][i]);
            }

        }

        /**
         * This method takes the destination data center as its input and 
         * routes to the destination.
         * 
         * @param distination Destination data center
         * @return route to Destination data center
         */
        public int route(Datacenter distination) {
            return routingTable.get(distination.getId());
        }
//######################################################################################

        /**
         * The router has a mechanism that takes the data unit ID and returns 
         * the closest data center which has a version of this data. In order 
         * to find the destination data center for accessing the data we need 
         * this method.
         * 
         * @param dataitemInfoId ID of Data item
         * @return ID of the closest data center includes a version of input data unit.
         */
        public int findDataitemPlace(int dataitemInfoId) {
            int min = 10;
            int datacenterId = id;
            for (int i = 0; i < Simulator.getDatacentersTopology().size(); i++) {

                if (Simulator.getDatacentersTopology().getDatacenters().get(i).getNamenode().getDataitemsMap().
                        contain(dataitemInfoId) && DatacentersTopology.getDistance(id, i) < min) {
                    min = DatacentersTopology.getDistance(id, i);
                    datacenterId = i;
                }
            }
            return datacenterId;
        }

        /**
         * This method takes ID of source data center and destination data center
         * and gives the route between those two.
         * 
         * @param sourceId ID of source data center 
         * @param destinationId ID of destination data center
         * @return  route between source data center and destination data center
         */
        public Queue<Integer> getRoutingPath(int sourceId, int destinationId) {
            Queue<Integer> path = new LinkedList<>();

            while (RoutingTables()[sourceId][destinationId] != destinationId) {
                path.add(RoutingTables()[sourceId][destinationId]);
                sourceId = RoutingTables()[sourceId][destinationId];
            }
            return path;
        }
        
        /**
         * This method takes ID of source data center and destination data center
         * and gives the next hop in routing path.
         * 
         * @param sourceId ID of source data center 
         * @param destinationId ID of destination data center
         * @return Next hop in routing path
         */
        public int nextHop(int sourceId, int destinationId){
            return RoutingTables()[sourceId][destinationId];
        }

//#####################################################################################
        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }

}
