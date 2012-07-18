/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.Transaction;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import jcifs.smb.SmbFileInputStream;
import net.pleiades.simulations.Simulation;
import net.pleiades.simulations.creator.SimulationsCreator;
import net.pleiades.simulations.creator.XMLSimulationsCreator;

public class User {

    public static void showDetails(String user) {
        int sims = 0;
        int running = 0;
        int waiting = 0;
        int completed = 0;

        Map<String, Simulation> completedMap = Hazelcast.getMap(Config.completedQueue);
        Collection<Simulation> simulations = completedMap.values();

        sims = simulations.size();

        for (Simulation s : simulations) {
            running += 0;
            waiting += (s.getSamples() - s.getResults().size());
        }

        System.out.println("\n=====================Status for " + user + "=====================");
        System.out.println("Active simulations: " + sims);
        System.out.println("Running tasks:" + running);
        System.out.println("Waiting tasks:" + waiting);

        Map<String, List<Simulation>> simulationsMap = Hazelcast.getMap(Config.jobsQueue);

        Set<String> keys = simulationsMap.keySet();

        for (String k : keys) {
            Lock lock = Hazelcast.getLock(k);
            lock.unlock();
        }
    }

    public static void uploadJob(Properties properties, String input, String jar, String user, String userEmail) {
        File run = null;
        FileInputStream runInputStream = null;
        byte[] runBytes = null;

        try {
            if (jar != null) {
                System.out.println("Info: Using custom jar file.");
                run = new File(jar);
            } else {
                System.out.println("Info: Using default jar file.");
                SmbFileInputStream smbJar = Utils.getJarFile(properties.getProperty("ci_jar"));

                run = new File("pleiades.run");
                run.deleteOnExit();
                FileOutputStream fout = new FileOutputStream(run);
                byte[] b = new byte[8192];
                int n;

                while ((n = smbJar.read(b)) > 0) {
                    fout.write(b, 0, n);
                }
                fout.close();
            }

            runInputStream = new FileInputStream(run);
            runBytes = new byte[runInputStream.available()];
            runInputStream.read(runBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (run == null) {
            System.out.println("Error: jar file not specified! Check your Pleiades configuration or use --jar to specify a custom jar file.");
            System.exit(1);
        }

        File inputFile = new File(input);
        String absPath = inputFile.getAbsolutePath();
        String jobName = absPath.substring(absPath.lastIndexOf("/") + 1, absPath.lastIndexOf("."));
        String fileKey = user + "_" + jobName;
        System.out.println("Creating job from file (" + input + ")...");
        System.out.println("Job name set to: " + jobName + "...");
        SimulationsCreator creator = new XMLSimulationsCreator(user, userEmail, user, jobName);
        List<Simulation> simulations = creator.createSimulations(inputFile, fileKey);//runBytes);

        System.out.print("Your job contains " + simulations.size() + " simulations.\nAquiring lock. Please be patient...");

        Lock jLock = Hazelcast.getLock(Config.jobsQueue);
        Lock fLock = Hazelcast.getLock(Config.fileQueue);
        Lock cLock = Hazelcast.getLock(Config.completedQueue);

        jLock.lock();
        fLock.lock();
        cLock.lock();

        Map<String, List<Simulation>> simulationsMap = Hazelcast.getMap(Config.jobsQueue);
        Map<String, byte[]> fileQueue = Hazelcast.getMap(Config.fileQueue);
        IMap<String, Simulation> completedMap = Hazelcast.getMap(Config.completedQueue);

        Transaction txn = Hazelcast.getTransaction();
        txn.begin();

        int tasks = 0;
        try {

            System.out.print("Lock Aquired.\nUploading tasks to cluster...");
            //System.out.println("1");
            fileQueue.put(fileKey, runBytes);
            //System.out.println("2");
            List<Simulation> jobsList = simulationsMap.remove(user);

            if (jobsList == null) {
                jobsList = new LinkedList<Simulation>();
            }

            for (Simulation s : simulations) {
                //System.out.println("*");
                s.generateTasks();
                //System.out.print("1");
                tasks += s.getSamples();
                //System.out.print("2");
                jobsList.add(s);
                //System.out.print("3");
                completedMap.put(s.getID(), s.emptyClone());
                //System.out.println("*");
            }
            simulationsMap.put(user, jobsList);
            txn.commit();
        } catch (Throwable e) {
            e.printStackTrace();
            txn.rollback();
        } finally {
            cLock.unlock();
            fLock.unlock();
            jLock.unlock();
        }

        System.out.println(tasks + " tasks uploaded.\n");
        System.out.println("Your job was created successfully.\nYou will be notified via email (" + userEmail + ") when your results are available.\nThis client will now terminate.\n");

        Hazelcast.getLifecycleService().shutdown();
        System.exit(0);
    }
}
