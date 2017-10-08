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
 * It is a link between two data centers which connects these two.
 * 
 * @author AliMori
 */
public class Link {

    /**
     * first data center
     */
    Datacenter datacenter_1;
    /**
     * second data center
     */
    Datacenter datacenter_2;

    /**
     * It creates a link that connects the two data centers.
     * 
     * @param datacenter_1 first data center
     * @param datacenter_2 second data center
     */
    public Link(Datacenter datacenter_1, Datacenter datacenter_2) {
        this.datacenter_1 = datacenter_1;
        this.datacenter_2 = datacenter_2;
        
        this.datacenter_1.getNamenode().linkToNamenode(this.datacenter_2.getNamenode());

        this.datacenter_1.addToNeighbourDatacenterList(this.datacenter_2);
        this.datacenter_2.addToNeighbourDatacenterList(this.datacenter_1);
    }

    public Datacenter getDatacenter_1() {
        return datacenter_1;
    }

    public void setDatacenter_1(Datacenter datacenter_1) {
        this.datacenter_1 = datacenter_1;
    }

    public Datacenter getDatacenter_2() {
        return datacenter_2;
    }

    public void setDatacenter_2(Datacenter datacenter_2) {
        this.datacenter_2 = datacenter_2;
    }

}
