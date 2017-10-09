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

/**
 * Users employ this object for accessing the data and operating on them. 
 * In fact it creates a request for operating on the intended data and sends it 
 * to the data center which is located in the same region as itself.
 * 
 * @author AliMori
 */
public class Request implements Comparable<Request> {

    /**
     * each request is identified by a sessionID
     */
    long sessionID;
    /**
     * user who send this request
     */
    User user;
    /**
     * this object Specifies type of request
     */
    RequestType requestType;
    /**
     * request is sent for accessing this data item
     */
    int dataitemID;
    /**
     * time of submit request. For prioritizing of execution of requests.
     */
    long time;

    /**
     * create a new object of request
     * 
     * @param user user who send this request
     * @param reuestType this object Specifies type of request
     * @param dataitemID request is sent for accessing this data item
     * @param time time of submit request
     * @param sessionId each request is identified by a sessionID
     */
    public Request(User user, RequestType reuestType, int dataitemID, long time, long sessionId) {
        this.user = user;
        this.requestType = reuestType;
        this.dataitemID = dataitemID;
        this.time = time;
        this.sessionID = sessionId;
        //Broker.allRequsets.add(this);
    }

    public int getDataitemID() {
        return dataitemID;
    }

    public void setDataitemID(int dataitemID) {
        this.dataitemID = dataitemID;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(RequestType reuestType) {
        this.requestType = reuestType;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    /**
     * sort requests based on time
     * 
     * @param request 
     * @return 
     */
    @Override
    public int compareTo(Request request) {
        if (this.getTime() <= request.getTime()) {
            return -1;
        }
        return 1;
    }

    public long getId() {
        return sessionID;
    }

    public void setId(long id) {
        this.sessionID = id;
    }
    
    

}
