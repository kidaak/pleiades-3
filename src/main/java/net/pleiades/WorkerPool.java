/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

import java.io.IOException;
import java.util.Properties;
import jline.ConsoleReader;
import net.pleiades.simulations.selection.EqualProbabilitySelector;
import net.pleiades.tasks.executor.TaskExecutor;

public class WorkerPool {

    private TaskExecutor[] workers;
    private ConsoleReader con;
    private boolean quiet;

    public WorkerPool(Properties properties, int count, boolean quiet) {
        try {
            this.workers = new TaskExecutor[count];
            this.con = new ConsoleReader();

            for (int i = 0; i < count; i++) {
                workers[i] = new TaskExecutor(properties, new EqualProbabilitySelector());
            }

            this.quiet = quiet;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void execute() {
        try {
            for (int i = 0; i < workers.length; i++) {
                new Thread(workers[i]).start();
            }

            if (!quiet) {
                new Thread(new Reader()).start();

                while (true) {
                    Utils.sleep(200);

                    con.clearScreen();
                    System.out.println(Utils.header);
                    for(int i = 0; i < workers.length; i++) {
                        System.out.println("Worker " + i + ": " + workers[i].getStateString());
                    }
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private class Reader implements Runnable {
        @Override
        public void run() {
            while(true) {
                try {
                    if (con.getInput().available() != 0) {
                        int key = con.readVirtualKey();
                    }
                    
                    Utils.sleep(10);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
