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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * The topology of the data center network is identified in this class. 
 * It contains a list of data center which are connected via links with 
 * each intended topology of data centers.
 * The number of data centers and their networking is adjustable.
 *
 * @author AliMori
 */
public class DatacentersTopology {

     /**
     * When the value of isRunning parameter is set to true, DatacentersTopology starts.
     */
    boolean isRunning = false;
    
    /**
     * determine number of data centers 
     */
    private int numOfDatacenters = 10;

    /**
     * stores list of data centers
     */
    private List<Datacenter> datacenters = new ArrayList<>();

    /**
     * stores links between data centers
     */
    List<Link> links = new ArrayList<>();

    //public static List<Link> ls = new ArrayList<>(9);
    
    /**
     * A storing system of several data centers with arbitrary topology is created.
     */
    public DatacentersTopology() {

        {
            for(int i=0 ;i<numOfDatacenters;i++){
                datacenters.add(new Datacenter(i));
            }
        }
        
        {
            links.add(new Link(datacenters.get(0), datacenters.get(1)));
            links.add(new Link(datacenters.get(0), datacenters.get(2)));
            links.add(new Link(datacenters.get(0), datacenters.get(3)));
            links.add(new Link(datacenters.get(3), datacenters.get(4)));
            links.add(new Link(datacenters.get(3), datacenters.get(5)));
            links.add(new Link(datacenters.get(5), datacenters.get(6)));
            links.add(new Link(datacenters.get(5), datacenters.get(7)));
            links.add(new Link(datacenters.get(5), datacenters.get(9)));
            links.add(new Link(datacenters.get(6), datacenters.get(8)));
        }

    }

    /**
     * It takes a data center and returns its main neighbor. The function of
     * this method is for the data repetition algorithm with the positioning 
     * strategy based on the owner which copies the data in its neighbor.
     * 
     * @param datacenter input data center
     * @return neighbor of input data center
     */
    public Datacenter getPrimaryNeighbor(Datacenter datacenter) {
        Datacenter neighbor = datacenters.get(0);

        switch (datacenter.getId()) {
            case 0:
                neighbor = datacenters.get(1);
                break;
            case 1:
                neighbor = datacenters.get(0);
                break;
            case 2:
                neighbor = datacenters.get(0);
                break;
            case 3:
                neighbor = datacenters.get(4);
                break;
            case 4:
                neighbor = datacenters.get(3);
                break;
            case 5:
                neighbor = datacenters.get(7);
                break;
            case 6:
                neighbor = datacenters.get(8);
                break;
            case 7:
                neighbor = datacenters.get(5);
                break;
            case 8:
                neighbor = datacenters.get(6);
                break;
            case 9:
                neighbor = datacenters.get(5);
                break;

        }
        return neighbor;
    }

//    public static void main(String[] args) {
//
//        //System.out.println("*");
//        DatacentersTopology topology = new DatacentersTopology();
//
//        System.out.println("topology is :");
//
//        for (Link link : topology.links) {
//            //System.out.println("*");
//            System.out.println(link.getDatacenter_1().getId() + " ----- " + link.getDatacenter_2().getId());
//        }
//        //System.out.println("*");
//        //System.out.println("2");
//
//    }
    public List<Datacenter> getDatacenters() {
        return datacenters;
    }

    /**
     * It starts data center topology
     */
    public void start() {

        isRunning = true;

        datacenters.get(0).start();
        datacenters.get(1).start();
        datacenters.get(2).start();
        datacenters.get(3).start();
        datacenters.get(4).start();
        datacenters.get(5).start();
        datacenters.get(6).start();
        datacenters.get(7).start();
        datacenters.get(8).start();
        datacenters.get(9).start();

    }

    /**
     * return number of data centers in system
     * @return size of data center topology
     */
    public int size() {
        return datacenters.size();
    }

    //**************** matrix mojaverat **************************************
    public int[][] getAdjacencyMatrix(List<Datacenter> datacenters) {

        List<Link> links = new ArrayList<>();
        for (Link link : this.links) {
            if (datacenters.contains(link.getDatacenter_1())
                    && datacenters.contains(link.getDatacenter_2())) {
                links.add(link);
            }
        }

        int[][] adjacencyMatrix = new int[datacenters.size()][datacenters.size()];
        for (Link link : links) {
            adjacencyMatrix[link.getDatacenter_1().getId()][link.getDatacenter_2().getId()] = 1;
            adjacencyMatrix[link.getDatacenter_2().getId()][link.getDatacenter_1().getId()] = 1;
        }
        return adjacencyMatrix;
    }

    //***************************************************************************
    
    /**
     * It takes the data center network and a destination data center
     * and returns the bfs survey.
     * 
     * @param inDs list of data centers
     * @param source source data center
     * @return 
     */
    public List<Datacenter> bfs(List<Datacenter> inDs, Datacenter source) {
        List<Datacenter> outDs = new ArrayList<>();
        Queue<Datacenter> queue = new LinkedList<>();
        State[] visited = new State[this.datacenters.size()];
        for (State state : visited) {
            state = State.Null;
        }

        for (Datacenter d : inDs) {
            visited[d.getId()] = State.Unvisited;
        }

        Datacenter element;

        visited[source.getId()] = State.Visited;
        queue.add(source);

        while (!queue.isEmpty()) {
            element = queue.remove();
            //i = element;
            //System.out.print(i + "\t");
            outDs.add(element);
            for (Datacenter neighbor : element.getNeighbours()) {

                if (visited[neighbor.getId()] == State.Unvisited) {
                    queue.add(neighbor);
                    visited[neighbor.getId()] = State.Visited;
                }

            }
        }
        return outDs;
    }

   
    /**
     * It receives a list of data centers and divides them based on 
     * the existing two data center to two regions. 
     * It is used for the repetition algorithm.
     * 
     * @param d1 first data center
     * @param d2  second data center
     * @param ds list of data centers
     * @return two list of data centers as two zone
     */
    public static List<List<Datacenter>> zoning(Datacenter d1, Datacenter d2, List<Datacenter> ds) {

        // ye liste 2 tayi ke zone 1 va zone 2 ra dar khod ja midahad
        List<List<Datacenter>> zones = new ArrayList<>();
        List<Datacenter> zone1 = new ArrayList<>();
        List<Datacenter> zone2 = new ArrayList<>();

        if (!ds.contains(d1) || !ds.contains(d2)) {
            System.out.println(" Zoning Error . . . ");
        } else {
            zone1.add(d1);
            zone2.add(d2);
            ds.remove(d1);
            ds.remove(d2);

            if (!ds.isEmpty()) {
                Iterator<Datacenter> iterator = ds.iterator();
                while (iterator.hasNext()) {
                    Datacenter datacenter = iterator.next();
                    if (getDistance(d1.getId(), datacenter.getId()) < getDistance(d2.getId(), datacenter.getId())) {
                        zone1.add(datacenter);
                        iterator.remove();
                    }
                    if (getDistance(d1.getId(), datacenter.getId()) > getDistance(d2.getId(), datacenter.getId())) {
                        zone2.add(datacenter);
                        iterator.remove();
                    }
                }
            }

            if (!ds.isEmpty()) {
                Iterator<Datacenter> iterator = ds.iterator();
                if (zone1.size() <= zone2.size()) {
                    while (iterator.hasNext()) {
                        Datacenter datacenter = iterator.next();
                        zone1.add(datacenter);
                        iterator.remove();
                    }
                }
                if (zone1.size() > zone2.size()) {
                    while (iterator.hasNext()) {
                        Datacenter datacenter = iterator.next();
                        zone2.add(datacenter);
                        iterator.remove();
                    }
                }
            }
        }
        zones.add(zone1);
        zones.add(zone2);
        return zones;
    }

    /**
     * This table should be devised based on the data centers networking 
     * and routing algorithms. Here we devise a hypothetical network with 
     * ten data centers manually.
     * 
     * @return routing table
     */
    public static int[][] RoutingTables() {
        int[][] routingTabls
                = {
                    {0, 1, 2, 3, 3, 3, 3, 3, 3, 3},
                    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {0, 0, 0, 0, 4, 5, 5, 5, 5, 5},
                    {3, 3, 3, 3, 0, 3, 3, 3, 3, 3},
                    {3, 3, 3, 3, 3, 0, 6, 7, 6, 9},
                    {5, 5, 5, 5, 5, 5, 0, 5, 8, 5},
                    {5, 5, 5, 5, 5, 5, 5, 0, 5, 5},
                    {6, 6, 6, 6, 6, 6, 6, 6, 0, 6},
                    {5, 5, 5, 5, 5, 5, 5, 5, 5, 0},};

        return routingTabls;
    }

    /**
     * This table should be devised based on the data centers networking and 
     * routing algorithms. Here we devise a hypothetical network with ten 
     * data centers manually.
     * 
     * @return  a matrix with distance indices between the data centers based on the link.
     */
    public static int[][] distancMatrix() {
        int[][] distanc
                = {
                    {0, 1, 1, 1, 2, 2, 3, 3, 4, 3},
                    {1, 0, 2, 2, 3, 3, 4, 4, 5, 4},
                    {1, 2, 0, 2, 3, 3, 4, 4, 5, 4},
                    {1, 2, 2, 0, 1, 1, 2, 2, 3, 2},
                    {2, 3, 3, 1, 0, 2, 3, 3, 4, 3},
                    {2, 3, 3, 1, 2, 0, 1, 1, 2, 1},
                    {3, 4, 4, 2, 3, 1, 0, 2, 1, 2},
                    {3, 4, 4, 2, 3, 1, 2, 0, 3, 2},
                    {4, 5, 5, 3, 4, 2, 1, 3, 0, 3},
                    {3, 4, 4, 2, 3, 1, 2, 2, 3, 0},};

        return distanc;
    }

    //**************************************
    public static int getDistance(int dId1, int dId2) {
        return distancMatrix()[dId1][dId2];
    }

    public enum State {

        Null, Visited, Unvisited;
    }

   

}
