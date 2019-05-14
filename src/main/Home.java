
package main;

import dbtransformationphase.TransformatinPhase;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Ashrin Rose Jose
 */
public class Home {

    public static long minUtility = 50;
    public static long minUtility2=10;
    public static int K = 2;

    public void execute() {

        try {

            String[] jobSet = {"Phase1", "/home/user/ashrin/db2.txt", "/home/user/ashrin/phase1db.txt"};

            System.out.println("\n MapReduce Counting Phase Has Started");

            /**
             * Phase 1.
             */
            ToolRunner.run(new Configuration(), new JobRunner(), jobSet);

            String[] jobSet2 = {"/home/user/ashrin/db2.txt", "/home/user/ashrin/phase1db.txt",
                "/home/user/ashrin/phase2db.txt"};
            /**
             * Removing low TWU itemsets can be done at counting phase to make
             * it faster.
             */
            /**
             * Phase 2.
             */
            TransformatinPhase.execute(jobSet2, minUtility);

            /**
             * Phase 3.
             */
            String[] jobSet3 = {"Phase3", "/home/user/ashrin/phase2db.txt", "/home/user/ashrin/phase3db.txt",
                String.valueOf(minUtility2), String.valueOf(K)};

            ToolRunner.run(new Configuration(), new JobRunner(), jobSet3);

            BufferedReader br = new BufferedReader(new FileReader(jobSet3[2]));

            HashSet HUIs = new HashSet();

            String line;
            while ((line = br.readLine()) != null) {
                String HUI = line.split("\\s+")[0];
                HUIs.add(HUI);
            }
            System.out.println("HUI = " + HUIs);

        } catch (Exception ex) {
            Logger.getLogger(Home.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void main(String[] args) throws Exception {
        Home h = new Home();
        h.execute();
    }

}
