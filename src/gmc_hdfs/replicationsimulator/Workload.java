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


import gmc_hdfs.distribution.StdRandom;
import hdfs.replicationsimulator.Main;
import hdfs.replicationsimulator.Node;
import hdfs.replicationsimulator.SimTrace;
import hdfs.replicationsimulator.Simulator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * workload class is component whose generate workload.
 * It is a component which simulates the work load. Work load is a set 
 * of requests which are sent by the users to the system for accessing the 
 * data and processing them.
 * 
 * @author AliMori
 * @since GMC_HDFSSim Toolkit 1.0
 */
public class Workload implements Runnable {

    /**
     * With executing this method, the workload is implemented to the system 
     * and it continues until the simulation is finished. The type and features 
     * of the workload should be written inside the function. In fact anything 
     * that is in this function will be executed. It is recommended that you 
     * should write functions for the work load and call the function inside 
     * the run function.
     * 
     * Two important methods for creating the work load are written before in 
     * this class which will be introduced in the following.
     */
    @Override
    public void run() {
        Simulator.addTrace(new SimTrace("Started Workload Generator . . ."));

        flashCrowedWorkload();

        //slashdotWorkload();
    }
    
  

    /**
     * This method creates flash crowd workload. This work load is based on
     * the default topology of data centers that is written in this simulator.
     * You can customize it according to your needs.
     */
    /////////////////////////////// FASHCROWED WORKLOAD
    void flashCrowedWorkload() {

        long requestId = 0;
        int period = (int) Main.getSimulationDuration() / 2;
        int requestTime = 0;
        User user;
        int dataitemId;
        long start = System.currentTimeMillis();

        long initialTime = 0;

        Queue<Integer> dataitems = new LinkedList<>();
        while (dataitems.size() < 727000) {
            int dataitemID = (int) StdRandom.boundedPareto(0.1, 1, Simulator.getNumberofdataitems()
                    * Simulator.getDatacentersTopology().size());
            dataitems.add(dataitemID);
        }

        while (Node.now() <= start + period) {

            requestTime += StdRandom.poisson(10);
            user = Simulator.getDatacentersTopology().getDatacenters().get((int) StdRandom.boundedPareto(1.5,
                    0, 10)).getAllUsers().getUsers().get(StdRandom.uniform(0, Simulator.getNumberOfUsers()));
//            dataitemId = (int) StdRandom.boundedPareto(1, 1, Simulator.getNumberofdataitems()
//                    * Simulator.getDatacentersTopology().size());
            dataitemId = dataitems.poll();

            Request request = user.readDataitem(requestId++, dataitemId, (long) requestTime);

            try {
                Thread.sleep(request.getTime() - initialTime);
                initialTime = request.getTime();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                System.out.println("Request " + request.getRequestType() + " from User #" + request.getUser()
                        .getDatacenterId() + "_" + request.getUser().getId() + " for Dataitem #"
                        + request.getDataitemID() + " is inetrrupted . . .");
            }

//            System.out.println("" + request.getTime() + " : Reqest #" + request.getId() + " : User #"
//                    + request.getUser().getDatacenterId()
//                    + "_" + request.getUser().getId() + " for Dataitem #" + request.getDataitemID());
            Simulator.addToRequestsList(request);
        }

        while (Node.now() <= start + (2 * period)) {

            requestTime += StdRandom.poisson(10);
            user = Simulator.getDatacentersTopology().getDatacenters().get(inversRegions(
                    (int) StdRandom.boundedPareto(1.5, 0, 10))).
                    getAllUsers().getUsers().get(StdRandom.uniform(0, Simulator.getNumberOfUsers()));
//            dataitemId = (int) StdRandom.boundedPareto(0.7, 1, Simulator.getNumberofdataitems()
//                    * Simulator.getDatacentersTopology().size());
            dataitemId = dataitems.poll();
            Request request = user.readDataitem(requestId++, dataitemId, (long) requestTime);

            try {
                Thread.sleep(request.getTime() - initialTime);
                initialTime = request.getTime();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                System.out.println("Request " + request.getRequestType() + " from User #" + request.getUser()
                        .getDatacenterId() + "_" + request.getUser().getId() + " for Dataitem #"
                        + request.getDataitemID() + " is inetrrupted . . .");
            }

//            System.out.println("" + request.getTime() + " : Reqest #" + request.getId() + " : User #"
//                    + request.getUser().getDatacenterId()
//                    + "_" + request.getUser().getId() + " for Dataitem #" + request.getDataitemID());
            Simulator.addToRequestsList(request);
        }
    }

    /**
     * This method creates slashdot workload. This work load is based on 
     * the default topology of data centers that is written in this simulator. 
     * You can customize it according to your needs.
     */
    ////////////////////////////// FLASHDOT WORKLOAD
    void slashdotWorkload() {

        long requestId = 0;
        int period = (int) Main.getSimulationDuration() / 3;
        int requestTime = 0;
        User user;
        int dataitemId;
        long start = System.currentTimeMillis();

        long initialTime = 0;

        Queue<Integer> dataitems = new LinkedList<>();
        while (dataitems.size() < 300000) {
            int dataitemID = (int) StdRandom.boundedPareto(0.3, 1, Simulator.getNumberofdataitems()
                    * Simulator.getDatacentersTopology().size());
            dataitems.add(dataitemID);
        }

        while (Node.now() <= start + period) {

            requestTime += StdRandom.poisson(400);
            user = Simulator.getDatacentersTopology().getDatacenters().get(StdRandom.uniform(0, 10)).
                    getAllUsers().getUsers().get(StdRandom.uniform(0, Simulator.getNumberOfUsers()));
//            dataitemId = (int) StdRandom.boundedPareto(1.161, 1, Simulator.getNumberofdataitems()
//                    * Simulator.getDatacentersTopology().size());

            dataitemId = dataitems.poll();

            Request request = user.readDataitem(requestId++, dataitemId, (long) requestTime);

            try {
                Thread.sleep(request.getTime() - initialTime);
                initialTime = request.getTime();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                System.out.println("Request " + request.getRequestType() + " from User #" + request.getUser()
                        .getDatacenterId() + "_" + request.getUser().getId() + " for Dataitem #"
                        + request.getDataitemID() + " is inetrrupted . . .");
            }

//            System.out.println("" + request.getTime() + " : Reqest #" + request.getId() + " : User #"
//                    + request.getUser().getDatacenterId()
//                    + "_" + request.getUser().getId() + " for Dataitem #" + request.getDataitemID());
            Simulator.addToRequestsList(request);
        }

        while (Node.now() <= start + (2 * period)) {

            requestTime += StdRandom.poisson(10);
            user = Simulator.getDatacentersTopology().getDatacenters().get(StdRandom.uniform(0, 10)).
                    getAllUsers().getUsers().get(StdRandom.uniform(0, Simulator.getNumberOfUsers()));
//            dataitemId = (int) StdRandom.boundedPareto(1.161, 1, Simulator.getNumberofdataitems()
//                    * Simulator.getDatacentersTopology().size());

            dataitemId = dataitems.poll();
            Request request = user.readDataitem(requestId++, dataitemId, (long) requestTime);

            try {
                Thread.sleep(request.getTime() - initialTime);
                initialTime = request.getTime();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                System.out.println("Request " + request.getRequestType() + " from User #" + request.getUser()
                        .getDatacenterId() + "_" + request.getUser().getId() + " for Dataitem #"
                        + request.getDataitemID() + " is inetrrupted . . .");
            }

//            System.out.println("" + request.getTime() + " : Reqest #" + request.getId() + " : User #"
//                    + request.getUser().getDatacenterId()
//                    + "_" + request.getUser().getId() + " for Dataitem #" + request.getDataitemID());
            Simulator.addToRequestsList(request);
        }

        while (Node.now() <= start + (3 * period)) {

            requestTime += StdRandom.poisson(400);
            user = Simulator.getDatacentersTopology().getDatacenters().get(StdRandom.uniform(0, 10)).
                    getAllUsers().getUsers().get(StdRandom.uniform(0, Simulator.getNumberOfUsers()));
//            dataitemId = (int) StdRandom.boundedPareto(1.161, 1, Simulator.getNumberofdataitems()
//                    * Simulator.getDatacentersTopology().size());

            dataitemId = dataitems.poll();
            Request request = user.readDataitem(requestId++, dataitemId, (long) requestTime);

            try {
                Thread.sleep(request.getTime() - initialTime);
                initialTime = request.getTime();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                System.out.println("Request " + request.getRequestType() + " from User #" + request.getUser()
                        .getDatacenterId() + "_" + request.getUser().getId() + " for Dataitem #"
                        + request.getDataitemID() + " is inetrrupted . . .");
            }

//            System.out.println("" + request.getTime() + " : Reqest #" + request.getId() + " : User #"
//                    + request.getUser().getDatacenterId()
//                    + "_" + request.getUser().getId() + " for Dataitem #" + request.getDataitemID());
            Simulator.addToRequestsList(request);
        }
    }
    ///////////////////////////////////////

    /**
     * This method is used in flashCrowdedWorkload method for changing the 
     * geographical regions in the traffic change.
     * @param region id of region
     * @return regions far from current regions
     */
    // it is using for change geoghraphical regions to other sides
    int inversRegions(int region) {
        int answer = 0;
        switch (region) {
            case 0: {
                answer = 9;
                break;
            }
            case 1: {
                answer = 8;
                break;
            }
            case 2: {
                answer = 7;
                break;
            }
            case 3: {
                answer = 6;
                break;
            }
            case 4: {
                answer = 5;
                break;
            }
            case 5: {
                answer = 4;
                break;
            }
            case 6: {
                answer = 3;
                break;
            }
            case 7: {
                answer = 2;
                break;
            }
            case 8: {
                answer = 1;
                break;
            }
            case 9: {
                answer = 0;
                break;
            }
        }
        return answer;
    }

}

//        List<Request> requests = Simulator.getWorkloadSimulation();
//        for (Request request : requests) {
//            try {
//                Thread.sleep(request.getTime() - initialTime);
//                initialTime = request.getTime();
//            } catch (InterruptedException ex) {
//                ex.printStackTrace();
//                System.out.println("Request " + request.getRequestType() + " from User #" + request.getUser()
//                        .getDatacenterId() + "_" + request.getUser().getId() + " for Dataitem #"
//                        + request.getDataitemID() + " is inetrrupted . . .");
//            }
//
//            System.out.println("" + request.getTime() + " : Reqest #" + request.getId() + " : User #"
//                    + request.getUser().getDatacenterId()
//                    + "_" + request.getUser().getId() + " for Dataitem #" + request.getDataitemID());
//
//            Simulator.addToRequestsList(request);
//        }
