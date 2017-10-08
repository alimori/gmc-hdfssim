package hdfs.replicationsimulator;


/**
 *
 * Launches the simulator
 *
 * @author peteratt
 * this class manipulate by Ali Mortazavi
 * @version 0.1
 */
public class Main {

    /**
     * simulation duration
     */
      private static long simulationDuration = 720 * 1000L;
    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {

        long start = System.currentTimeMillis();

        String configFile;
 
        if (args.length < 1) {
            configFile = "config1.txt";
        } else {
            configFile = args[0];
        }
        Simulator.init(configFile);
        long end = System.currentTimeMillis();
        System.out.println("Execution Init System Time: " + (end - start));
        System.out.println("Init System END . . .");
        System.out.println();

        Simulator.start();

//		while (!isEnded) wait();
        Thread.sleep(simulationDuration);
        System.out.println("END");
  
        // print results
        Simulator.getReplicationManager().printSpaceConsumption();
        Simulator.getReplicationManager().printTrafficConsumption();
        Simulator.getReplicationManager().printSystemAvailability();

        Simulator.printResults();
        System.exit(0);
    
    }

    public static long getSimulationDuration() {
        return simulationDuration;
    }

}
