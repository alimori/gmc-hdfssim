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
 * This enum use for determine type of request.
 * there are three Types: 
 * 1- "WRITE" use for creating or uploading a new data item 
 * or update a data item.
 * 2- "READ" use for access a data item and read or download it.
 * 3- "DELETE" use for remove a data item
 * 
 * @author AliMori
 */
public enum RequestType {
    /**
     * "WRITE", "READ", "DELETE" are three type of request
     */
    WRITE,READ,DELETE;
}
