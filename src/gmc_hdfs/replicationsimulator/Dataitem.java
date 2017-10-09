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

import hdfs.replicationsimulator.Block;
import hdfs.replicationsimulator.Simulator;
import java.util.ArrayList;
import java.util.List;

/**
 * Data units are created by the users and are stored in the nodes. 
 * They are being processed. Each data unit is divided to smaller 
 * data blocks with fixed size.
 *
 * @author AliMori
 */
public class Dataitem {

    /**
     * stores blocks of data
     */
    List<Block> blocks;
    private String name;
    /**
     * Each data item has an ID
     */
    private int id;
    /**
     * size of data item (MB)
     */
    private int size;
    //private List<Integer> readCount; // dar har khane tedad read haye karbaran nahie joghrafiayi mokhtalef
    //private int popularity;
    
    /**
     * It identifies whether the data should be shared or not.
     */
    boolean share = true;
    /**
     * The user ID is the owner of this data. It is an array which its first 
     * slot is data center ID of the neighbor and the second slot is the ID 
     * of data center user. Therefor it is unique in the whole system.
     */
    private int[] ownerId = new int[2];
    /**
     * shows last update time
     */
    private double lastUpdateTime;
    /**
     * shows creation time
     */
    private long creationTime;

    //*********************  costructore ************************
    /**
     * Create a new object of data item
     * 
     * @param id ID for data item
     * @param size Size of data item
     */
    public Dataitem(int id, int size) {//throws ParameterException {
//        if (id <= 0) {
//            throw new ParameterException("Id <= 0");
//        }
//        if (size <= 0) {
//            throw new ParameterException("Size <= 0");
//        }
        this.id = id;
        this.size = size;
        this.creationTime = System.currentTimeMillis();
        // popularity = 1;
        blocks = new ArrayList<>();

        this.share = true;

        //setOwnerforDataitem(id);

    }

    /**
     * This method divided a data item to multiple same size blocks
     */
    public void dataitemToBlocks() {
        for (int i = 0; i < (this.getSize() / 64); i++) {
            Block block = new Block(this.getId(), i, 64);
            blocks.add(i, block);
        }
    }

    //***************** getter and setter *********************
    public List<Block> getBlocks() {
        return blocks;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(double lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int[] getOwner() {
        return ownerId;
    }

    public void setOwner(int[] ownerId) {
        this.ownerId = ownerId;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

//    public void setReadOnly(boolean readOnly) {
//        this.readOnly = readOnly;
//    }

    public void setShare(boolean share) {
        this.share = share;
    }

    public boolean getShare() {
        return share;
    }

    /**
     * Set user who identified by id as owner of data item
     * 
     * @param id ID of data item's owner
     */
    public void setOwnerforDataitem(int id) {
        int lenth = 0;
        int id1 = id;
        while (id1 > 0) {
            id1 = id1 / Simulator.getDatacentersTopology().size();
            lenth++;
        }
        if (lenth == 1) {
            ownerId[0] = 0; // datacenterid
            ownerId[1] = id; // userid
        } else {
            ownerId[0] = id / (int) (Math.pow(Simulator.getDatacentersTopology().size(), (lenth - 1)));
            ownerId[1] = id % (int) (Math.pow(Simulator.getDatacentersTopology().size(), (lenth - 1)));
        }
    }

}
