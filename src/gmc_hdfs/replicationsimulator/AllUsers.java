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
import java.util.List;

/**
 * This class integrates the set of users associated with a data center 
 * directly to the data center.
 *
 * @author AliMori
 */
public class AllUsers {

    /**
     * id of data center 
     */
    int datacenterId;
    
    /**
     * a list that stores users
     */
    private List<User> users; 
//    boolean isRunning = false;

    /**
     * create an object of AllUsers for data center with datacenterId
     * 
     * @param datacenterId id of data center 
     */
    public AllUsers(int datacenterId) {
        this.datacenterId = datacenterId;
        users= new ArrayList<>();
    }

  
    /**
     * add a new user to AllUser
     * 
     * @param user an object of user
     */
    public void addUser(User user){
        users.add(user);
    }
    
      public List<User> getUsers() {
        return users;
    }
    
    public User getUser(int id){
        return users.get(id);
    }
    
    
}
