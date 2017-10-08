package hdfs.replicationsimulator;

import gmc_hdfs.replicationsimulator.DatacentersTopology;
import gmc_hdfs.replicationsimulator.DataitemInfo;
import gmc_hdfs.replicationsimulator.User;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * 
 * @author in the main source of this class was not written name of author,
 * this class manipulate by Ali Mortazavi
 */
public class AllDatanode {

    int datacenterId;

    private List<Datanode> datanodes;
    private boolean isRunning = false;
    private Queue<Event> toNamenode;

    private Queue<Event> toDatanodes;

    public Daemon hbthread = null; // Heartbeatsender thread
    public Daemon commandsthread = null; // commands thread

    private int heartbeatInterval = Simulator.getHeartbeat();// in ms
    private int bandwidth = Simulator.getBandwidth();//in b/s
    private int blockSize = Simulator.getBlockSize();// *1000 in B

    private long transferTime;
    //Communication recheck interval
    private long communicationRecheckInterval = Simulator.getHeartbeat() / 2;
    //Communication hearbeatsender interval
    private long heartbeatRecheckInterval = Simulator.getHeartbeat() / 5;

    // Lock for accessing the namenode command list
    private Object namenodeLock = new Object();

    // Lock for accessing the datanode command list
    private Object datanodeLock = new Object();

    boolean checkingPending = false;
    List<Long> handled;

    public AllDatanode(int datacenterId) {

        this.datacenterId = datacenterId;

        datanodes = new ArrayList<>();
        toNamenode = Simulator.getToNamenode(datacenterId);
        toDatanodes = Simulator.getToDatanodes(datacenterId);

        this.hbthread = new Daemon(new HeartbeatSender());
        System.out.println("AllDatanode #" + datacenterId + " HeartbeatSender is new() . . .");

        this.commandsthread = new Daemon(new CommandsHandler());
        System.out.println("AllDatanode #" + datacenterId + " CommandsHandler is new() . . .");

        this.bandwidth = Simulator.getBandwidth();

        //this.transferTime = (blockSize * 8) / this.bandwidth;
        System.out.println("AllDatanode #" + datacenterId + " created.");

        handled = new ArrayList<>();

    }

    void addNode(Datanode datanode) {
        datanodes.add(datanode);
    }

    Datanode getNode(int id) {
        return datanodes.get(id);
    }

    boolean killNode(int id) {
        return getNode(id).kill();
    }

    int datanodeListSize() {
        return datanodes.size();
    }

    public boolean addToNamenodeQueue(Event e) {
        synchronized (namenodeLock) {
            // System.out.println("adding heartbeat to namenode...");
            boolean result = toNamenode.add(e);
            // System.out.println("added heartbeat to namenode.");
            return result;
        }
    }

    public boolean addToDatanodeQueue(Event e) {
        synchronized (datanodeLock) {
            // System.out.println("adding heartbeat to namenode...");
            boolean result = toDatanodes.add(e);
            // System.out.println("added heartbeat to namenode.");
            return result;
        }
    }

    /**
     * Starts the datanodes management thread
     */
    public void start() {
        isRunning = true;
        hbthread.start();
        commandsthread.start();
    }

    /**
     * Stops the datanodes management thread
     */
    public void stop() {
        isRunning = false;

        try {
            hbthread.join();
            commandsthread.join();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send the heartbeats.
     */
    class HeartbeatSender implements Runnable {

        public void run() {
            while (isRunning) {
                try {
                    checkHearbeat();
                    Thread.sleep(heartbeatRecheckInterval);
                } catch (Exception e) {
                    // FSNamesystem.LOG.error(StringUtils.stringifyException(e));
                }
            }
        }

        private void checkHearbeat() {
            Datanode current;
            for (int i = 0; i < datanodes.size(); i++) {
                current = datanodes.get(i);
                if (!current.hasFailed()
                        && (Node.now() >= heartbeatInterval
                        + current.getLastHB())) {
                    sendHeartbeat(datanodes.get(i).getDatacenterId(), i);
                    // System.out.print("HB from " + current.getId() + "\n");
                    current.setLastHB(Node.now());
                }
            }
        }
    }

    public boolean sendHeartbeat(int datacenterId, int id) {
        return addToNamenodeQueue(new Event(datacenterId, id, Event.HEARTBEAT, Node.now()));
    }

    /**
     * Process the commands received from NameNode
     */
    class CommandsHandler implements Runnable {

        public void run() {
            while (isRunning) {
                try {
                    handleCommands();
                    Thread.sleep(communicationRecheckInterval);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void handleCommands() {
            //System.out.println("11111111");
            while (!toDatanodes.isEmpty()) {
                //System.out.println("22222222");
                Event command = toDatanodes.poll();

                if (command != null) {
                    // Handle it
                    if (command.getAction() == Event.PENDINGTRANSFER) {
                        //System.out.println("22222222");
                        if (!handleTransfer(command.getSourceDatacenter(), command.getSource(),
                                command.getDestinationDatacenter(), command.getDestination(),
                                command.getDataitemId(), command.getBlockId(), command.getIsFailBolck(),
                                command.getCutDatacenter())) {
                            toDatanodes.add(command);
                        }
                    } else if (command.getAction() == Event.REPLICATION) {
                        //System.out.println("3333333");
                        if (!handleReception(command.getSourceDatacenter(), command.getSource(),
                                command.getDestinationDatacenter(), command.getDestination(),
                                command.getDataitemId(), command.getBlockId(), command.getIsFailBolck(),
                                command.getCutDatacenter())) {
                            toDatanodes.add(command);
                        }
                    } else if (command.getAction() == Event.READBLOCK) {

                        if (!handleReadBlock(command.getRequestId(), command.getUser(), command.getDataitemId(),
                                command.getBlockId(), command.getSourceDatacenter(),
                                command.getSource())) {
                            toDatanodes.add(command);
                        }
                    } else if (command.getAction() == Event.REMOVE_BLOCK) {
                        if (!handleRemoveBlock(command.getDataitemId(),
                                command.getBlockId(), command.getSourceDatacenter(),
                                command.getSource())) {
                            toDatanodes.add(command);
                        }
                    } else if (command.getAction() == Event.PENDINGTRANSFERFORUSER) {
                        //System.out.println("22222222");
                        if (!handleTransferForUser(command.getRequestId(), command.getSourceDatacenter(),
                                command.getSource(), command.getUser(),
                                command.getDataitemId(), command.getBlockId())) {
                            toDatanodes.add(command);
                        }
                    }
                }
            }
        }
    }

    private boolean handleTransferForUser(long id, int idDatacenterSource, int idSource, User user,
            int idDataitem, int blockId) {

        Datanode datanode = null;

//            for (Datanode node : datanodes) {
//                if (node.getDatacenterId() == idDatacenterSource && node.getId() == idSource) {
//                    datanode = node;
//                }
//            }
        datanode = getNode(idSource);

        boolean NodesAlive = (!datanode.hasFailed());

        if (Node.now() >= datanode.getDownloadingTime() && NodesAlive) {
            //Send signal to Namenode that the transfer is finished
//                toNamenode.add(new Event(destinationNode.getDatacenterId(), destinationNode.getId(),
//                        Event.BLOCKRECEPTION, Node.now(), idDataitem, blockId));

//            Simulator.addTrace(new SimTrace(user.getDatacenterId(), user.getId(), SimTrace.BLOCK_READ,
//                    idDatacenterSource, idSource, idDataitem, blockId, Node.now()));
            // READ anjam shode ast.   
//            this.toNamenode.add(new Event(id, Event.BLOCKREADED, user, idDataitem, blockId, idDatacenterSource,
//                    idSource));
            if (!isHandled(id)) {
                DataitemInfo dataitemInfo = Simulator.getNamenode(datacenterId).getDataitemsMap()
                        .getDataitemInfo(idDataitem);
                //dataitemInfo.incCapacity(datacenterId);
                addToHandledList(id);
            }

            return true;
        } else if (!NodesAlive) {
            datanode.setDownloadingTime(Node.now());
            //return true;
            return false;
        }/*else {
         return false;
         }*/

        return false;

    }

    private boolean handleRemoveBlock(int dataitmeId, int blockId, int datacenterId, int datanodeId) {

        Datanode datanode = getNode(datanodeId);
        boolean NodeAlive = !datanode.hasFailed();
        if (NodeAlive) {
            Block block = datanode.getBlock(dataitmeId, blockId);
            datanode.removeBlock(block);
            toNamenode.add(new Event(datacenterId, datanodeId, Event.BLOCKREMOVED, Node.now(),
                    dataitmeId, blockId));
            return true;
        }
        return false;
    }

    private boolean handleReadBlock(long id, User user, int dataitmeId, int blockId,
            int datacenterId, int datanodeId) {

        // man dar zir transfer time ra neveshtam dar hali ke fek konam dar inja
        // faghat propagation delay kafi ast va nabayad transmission delay ra hesab konam.
        // pas behtar ast faghat propagation delay ra hesab konam mesle dovomi ke sahih ast.
//        transferTime = ((blockSize * 8) / bandwidth) + ((1 / 60)
//                * (DatacentersTopology.getDistance(user.getDatacenterId(),
//                        datacenterId)));
        //transferTime = (1000 / 60) * (DatacentersTopology.getDistance(user.getDatacenterId(), datacenterId));
        transferTime = (1 / 60) * (DatacentersTopology.getDistance(user.getDatacenterId(), datacenterId));

        Datanode datanode = null;
        for (Datanode node : datanodes) {
            if (node.getDatacenterId() == datacenterId && node.getId() == datanodeId) {
                datanode = node;
            }
        }

        boolean NodeAlive = !datanode.hasFailed();

        if (NodeAlive) {
            boolean ableToTransfer = (Node.now() > datanode.getDownloadingTime());

            if ((datanode != null) && ableToTransfer) {
                long time = Node.now() + transferTime;
                datanode.setDownloadingTime(time);
                toDatanodes.add(new Event(id, datanode.getDatacenterId(), datanode.getId(),
                        Event.PENDINGTRANSFERFORUSER, 0L, user, dataitmeId, blockId));
                return true;
            } else if (!ableToTransfer) {
                return false;
            }
        }
        return false;

//            if (Node.now() >= datanode.getDownloadingTime() && NodeAlive) {
//                return true;
//            } else if (!NodeAlive) {
//                datanode.setDownloadingTime(Node.now());
//                return false;
//            }
    }

    private boolean handleTransfer(int idDatacenterSource, int idSource, int idDatacenterDestination,
            int idDestination, int idDataitem, int blockId, int isBlockFail, int cutDatacenter) {

        // bara REPLICATION
        Datanode sourceNode = null;
        Datanode destinationNode = null;

//            for (Datanode node : datanodes) {
//                if (node.getDatacenterId() == idDatacenterSource && node.getId() == idSource) {
//                    sourceNode = node;
//                } else if (node.getDatacenterId() == idDatacenterDestination && node.getId() == idDestination) {
//                    destinationNode = node;
//                }
//            }
        sourceNode = getNode(idSource);
        destinationNode = Simulator.getDatacentersTopology().getDatacenters().get(idDatacenterDestination)
                .getAllDatanode().getNode(idDestination);

        boolean NodesAlive = (!sourceNode.hasFailed() && !destinationNode.hasFailed());

        if (Node.now() >= destinationNode.getDownloadingTime() && NodesAlive) {

            //Send signal to Namenode that the transfer is finished
            Simulator.getDatacentersTopology().getDatacenters().get(destinationNode.getDatacenterId())
                    .getAllDatanode().toNamenode.add(new Event(destinationNode.getDatacenterId(),
                                    destinationNode.getId(), Event.BLOCKRECEPTION, Node.now(),
                                    idDataitem, blockId, isBlockFail, cutDatacenter));

//                toNamenode.add(new Event(destinationNode.getDatacenterId(), destinationNode.getId(),
//                        Event.BLOCKRECEPTION, Node.now(), idDataitem, blockId));
            return true;
        } else if (!NodesAlive) {
            sourceNode.setUploadingTime(Node.now());
            destinationNode.setDownloadingTime(Node.now());
            //return true;
            return false;
        }/*else {
         return false;
         }*/

        return false;
    }

    public boolean handleReception(int idDatacenterSource, int idSource, int idDatacenterDestination,
            int idDestination, int idDataitem, int idBlock, int isBlockFail, int cutDatacenter) {

        Datanode sourceNode = null;
        Datanode destinationNode = null;

        if ((idDatacenterSource != idDatacenterDestination) || (idDatacenterSource == idDatacenterDestination
                && idSource != idDestination)) {

            int found = 0;

            sourceNode = getNode(idSource);
            found++;

            destinationNode = Simulator.getDatacentersTopology().getDatacenters().get(idDatacenterDestination)
                    .getAllDatanode().getNode(idDestination);
            found++;

//            transferTime = ((blockSize * 8 *1000 ) / bandwidth) + ((1000/ 60)
//                    * (DatacentersTopology.getDistance(sourceNode.getDatacenterId(),
//                            destinationNode.getDatacenterId()))); // chon milisecond ast *1000 mishavad
            transferTime = ((blockSize * 8) / bandwidth) + ((1 / 60)
                    * (DatacentersTopology.getDistance(sourceNode.getDatacenterId(),
                            destinationNode.getDatacenterId())));

            if (found < 2) {//We haven't found our nodes. (means that there is an error, so the pending request cannot succeed.
                //return true;
                return false; // bayad false bashad.
            }

            boolean NodesAlive = (!sourceNode.hasFailed() && !destinationNode.hasFailed());

            if (NodesAlive) {

                boolean ableToTransfer = (Node.now() > sourceNode
                        .getUploadingTime())
                        && (Node.now() > destinationNode.getDownloadingTime());

                if ((sourceNode != null) && (destinationNode != null)
                        && ableToTransfer) {
                    long time = Node.now() + transferTime;
                    sourceNode.setUploadingTime(time);
                    destinationNode.setDownloadingTime(time);

                    toDatanodes.add(new Event(sourceNode.getDatacenterId(), sourceNode.getId(),
                            Event.PENDINGTRANSFER, 0L, destinationNode.getDatacenterId(),
                            destinationNode.getId(), idDataitem, idBlock, isBlockFail, cutDatacenter));
                    return true;
                } else if (!ableToTransfer) {
                    return false;
                } else {
                    return false;
                }
            }
        }
        return false;
        //return true;
    }

    public void clearHandledList() {
        synchronized (handled) {
            handled.clear();
        }
    }

    public void addToHandledList(long id) {
        synchronized (handled) {
            handled.add(id);
        }
    }

    public boolean isHandled(long id) {
        synchronized (handled) {
            if (handled.contains(id)) {
                return true;
            } else {
                return false;
            }
        }
    }
}
