package hdfs.replicationsimulator;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * @author in the main source of this class was not written name of author,
 * this class manipulate by Ali Mortazavi
 */
public class NodeKiller implements Runnable {

    @Override
    public void run() {
        try {
            Thread.sleep(40 * 1000);
        } catch (InterruptedException ex) {
            Logger.getLogger(NodeKiller.class.getName()).log(Level.SEVERE, null, ex);
        }

        Simulator.addTrace(new SimTrace("Started NodeKiller . . ."));

        long initialTime = 0;
        List<Event> failures = Simulator.getSimulationFailureEvents();

        for (Event e : failures) {

            try {
                Thread.sleep(e.getTime() - initialTime);
                initialTime = e.getTime();
            } catch (InterruptedException ex) {
                // TODO Auto-generated catch block
                ex.printStackTrace();
                System.out.println("Event #" + e.getAction() + "   Datanode #" + e.getSourceDatacenter()
                        + "_" + e.getSource() + " is inetrrupted . . .");
            }
            //Simulator.getAllDatanodes(0).killNode(e.getSource());
            Simulator.killNode(e.getSourceDatacenter(), e.getSource());
        }

    }

}
