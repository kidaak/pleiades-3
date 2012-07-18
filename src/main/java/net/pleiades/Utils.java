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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import net.pleiades.database.DBCommunicator;
import net.pleiades.database.MySQLCommunicator;
import net.pleiades.simulations.Simulation;

public class Utils {
    
    public static DBCommunicator connectToDatabase(Properties p) {
        DBCommunicator db = new MySQLCommunicator(p);

        if(!db.connect()) {
            System.out.println("Error: Unable to connect to database.");
            System.exit(1);
        }

        return db;
    }

    public static SmbFileInputStream getJarFile(String jarPath) {
        SmbFileInputStream in = null;
        SmbFile jar = null;

        try {
            NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(null, "CiClops", "ciclops5813x");
            jar = new SmbFile(jarPath, auth);
            in = new SmbFileInputStream(jar);
        } catch (SmbException e) {
            System.out.println("Error: Unable to locate jar file!\n" + e.getMessage());
            System.exit(1);
        } catch (UnknownHostException e) {
            System.out.println("Error: Host not found when trying to get jar file!\n" + e.getMessage());
            System.exit(1);
        } catch (MalformedURLException e) {
            System.out.println("Error: Malformed URL!\n" + e.getMessage());
            System.exit(1);
        }

        return in;
    }

    public static void emailUser(Simulation simulation, Properties p) {
        String owner = simulation.getOwner();
        String email = simulation.getOwnerEmail();
        String link = "link";
        StringBuilder message = new StringBuilder();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(p.getProperty("gather_email_template"))));

            while (reader.ready()) {
                message.append(reader.readLine()
                        .replaceAll("\\$user", owner)
                        .replaceAll("\\$link", link)
                        .replaceAll("\\$job", simulation.getJobName()));
                message.append("\n");
            }

            System.out.println("email: " + email);
            
            Process shell = new ProcessBuilder("python", p.getProperty("gather_email_script"), email, message.toString()).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void unlockAll() {
        Map<String, List<Simulation>> simulationsMap = Hazelcast.getMap(Config.jobsQueue);

        Set<String> keys = simulationsMap.keySet();

        for (String k : keys) {
            Lock lock = Hazelcast.getLock(k);
            lock.unlock();
        }
    }

    public static DBCommunicator authenticate(Properties properties, String user) {
        DBCommunicator database = Utils.connectToDatabase(properties);
        int guesses = 3;

        while (!database.authenticateUser(user)) {
            sleep(1000);
            System.out.println("Authentication failed!");
            guesses--;

            if (guesses == 0) {
                System.exit(1);
            }
        }

        return database;
    }

    public static void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String header =
            "  _____  _      _           _          \n" +
            " |  __ \\| |    (_)         | |          \n" +
            " | |__) | | ___ _  __ _  __| | ___ ___ \n" +
            " |  ___/| |/ _ \\ |/ _` |/ _` |/ _ | __| * Cluster *\n" +
            " | |    | |  __/ | (_| | (_| |  __|__ \\\n" +
            " |_|    |_|\\___|_|\\__,_|\\__,_|\\___|___/\n";
}
