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

/**
 * This is a user which is identified by an ID.
 * The user is able to operate on the existing data in the system.
 * 
 * @author AliMori
 * @since GMC_HDFSSim Toolkit 1.0
 */
public class User {
    
   /**
    * Each user is directly connected to the data center via communication system.
    * datacenterld identifies the ID of that data center.
    * It is supposed that the user is located at the same geographical region
    * as the data center.
    */
    private int datacenterId;
    /**
     * id for user in data center
     */
    private int id;
    /**
     * It is an object that identifies the geographical region.
     * The system contains several geographical regions which each user
     * is located at them.
     */
    private GeographicalZone geographicalZone;

    final Object requestLock = new Object();

    /**
     * create a new object of user
     *
     *
     * @param datacenterId datacenterld identifies the ID of that data center
     * @param id id of user
     */
    public User(int datacenterId, int id) {
        this.datacenterId = datacenterId;
        this.id = id;
    }

    /**
     * create a new object of user
     * 
     * @param id id of user
     * @param datacenterId datacenterld identifies the ID of that data center
     * @param geographicalZone It is an object that identifies the geographical region.
     */
    public User(int id, int datacenterId, GeographicalZone geographicalZone) {
        this.id = id;
        this.datacenterId = datacenterId;
        this.geographicalZone = geographicalZone;
    }

    /**
     * this method is used for create a new data item or update it.
     *
     *
     * @param dataitemID id of data item
     * @param id id of request (session id)
     * @param time time of submit request
     */
    // write
    public void writeDataitem(long id, int dataitemID, long time) {
//        synchronized (Simulator.getRequests(datacenterId)) {
//            Simulator.getRequests(datacenterId).add(new Request(
//                    this, RequestType.WRITE, dataitem));
//        } 

        Request request = new Request(this, RequestType.WRITE, dataitemID, time, id);
        addToRequestsQueue(request);
    }

     /**
     * this method is used for access to data item and read it.
     *
     *
     * @param dataitemID id of data item
     * @param id id of request (session id)
     * @param time time of submit request
     * 
     * @return a request by type READ
     */
    // read
    public Request readDataitem(long id, int dataitemID, long time) {
        Request request = new Request(this, RequestType.READ, dataitemID, time, id);
        return request;
        //addToRequestsQueue(request);
    }

     /**
     * this method is used for remove data item.
     *
     *
     * @param dataitemID id of data item
     * @param id id of request (session id)
     * @param time time of submit request
     */
    // delete
    public void deleteDataitem(long id, int dataitemID, long time) {
        Request request = new Request(this, RequestType.DELETE, dataitemID, time, id);
        addToRequestsQueue(request);
    }

    /**
     * This method is for sending the request. 
     * In fact it puts the request in the queue which is for the requests of 
     * the data center and namenode takes the request data center from the 
     * queue and processes it.
     * 
     * @param request an object of Request
     * @return if success, return true
     */
    public boolean addToRequestsQueue(Request request) {
        synchronized (requestLock) {
            boolean result = Simulator.getRequests(datacenterId).add(request);
            // System.out.println("added heartbeat to namenode.");
            return result;
        }
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setDatacenterId(int datacenterId) {
        this.datacenterId = datacenterId;
    }

    public int getId() {
        return id;
    }

    public int getDatacenterId() {
        return datacenterId;
    }

    public GeographicalZone getGeographicalZone() {
        return geographicalZone;
    }

    public void setGeographicalZone(GeographicalZone geographicalZone) {
        this.geographicalZone = geographicalZone;
    }

    /**
     * This method identifies if the user is the owner of this data or not?
     * If the user is the owner, it returns true.
     * 
     * @param dataitemId id of data item
     * @return If the user is the owner, it returns true.
     */
    public boolean isOwner(int dataitemId) {
        int lenth = 0;
        int id1 = dataitemId;
        while (id1 > 0) {
            id1 = id1 / ( Simulator.getDatacentersTopology().size());
            lenth++;
        }
        if (lenth == 1) {
            if (id == dataitemId) {
                return true;
            }
        } else {
            if (datacenterId == dataitemId / (int) (Math.pow(( Simulator.getDatacentersTopology().size()), (lenth - 1)))
                    && id == dataitemId % (int) (Math.pow(( Simulator.getDatacentersTopology().size()), (lenth - 1)))) {
                return true;
            }
        }
        return false;
    }

}
