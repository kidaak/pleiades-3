/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.results;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Transaction;
import com.hazelcast.query.SqlPredicate;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.pleiades.Config;
import net.pleiades.simulations.Simulation;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author bennie
 */
public class CilibGatherer implements Gatherer {
    
    private Properties properties;

    public CilibGatherer(Properties properties) {
        this.properties = properties;
    }

    @Override
    public Simulation gatherResults(Simulation s) {
        Lock cLock = Hazelcast.getLock(Config.completedQueue);
        cLock.lock();

        IMap<String, Simulation> completedMap = Hazelcast.getMap(Config.completedQueue);

        Transaction txn = Hazelcast.getTransaction();
        txn.begin();

        try {
//            Collection<Simulation> remove = completedMap.values(new SqlPredicate("ID = " + s.getID()));
//            Simulation removed = completedMap.remove(((Simulation)remove.toArray()[0]).getID());
            completedMap.remove(s.getID());
            completedMap.forceUnlock(s.getID());
            txn.commit();
        } catch (Throwable e) {
            txn.rollback();
        } finally {
            cLock.unlock();
        }

        List<String> results;
        
        try {
            results = s.getResults();
            String someResult = results.get(0);

            int colums = getColumnCount(someResult);
            String[] measurements = getMeasurements(someResult, colums - 1);

            StringBuilder resultsBuilder = new StringBuilder();

            resultsBuilder.append(constructHeaders(measurements, results.size()));

            int linesPerSample = StringUtils.countMatches(someResult.substring(someResult.lastIndexOf("(0)") + 3), "\n") - 1;

            for (String r : results) {
                resultsBuilder.append(r.substring(r.lastIndexOf("(0)") + 3));
            }

            String jobName = s.getJobName();
            String jobPath = s.getOutputPath().substring(0, s.getOutputPath().lastIndexOf("/") + 1);
            String path = properties.getProperty("gather_results_folder") + s.getOwner() + "/" + jobName + "/" + jobPath;
            File temp = new File(path);
            temp.mkdirs();
            path = path + s.getID() + ".tmp";
            temp = new File(path);
            
            BufferedWriter writer = new BufferedWriter(new FileWriter(temp));
            writer.append(resultsBuilder);
            writer.close();

            List<String> command = new LinkedList<String>();
            command.add("python");
            command.add(properties.getProperty("gather_script"));
            command.add(properties.getProperty("gather_results_folder") + s.getOwner() + "/" + jobName + "/" + s.getOutputPath());
            command.add(path);
            command.add(String.valueOf(results.size()));
            command.add(String.valueOf(linesPerSample));
            command.add(String.valueOf(measurements.length + 1));

            System.out.print("Executing gather script...");
            
            Process shell = new ProcessBuilder(command).start();
            String line;
            InputStream inputStream = shell.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            System.out.print("Exit code: " + shell.waitFor());

            temp.delete();

            System.out.println("\nGatherer: Results saved to " + properties.getProperty("gather_results_folder") + s.getOwner() + "/" + jobName + "/" + s.getOutputPath());
        } catch (Throwable e) {
            e.printStackTrace();
        }

        return s;
    }

//    @Override
//    public Simulation resultsAvailable() {
//        IMap<String, Simulation> completed = Hazelcast.getMap(Config.completedQueue);
////        Collection<Simulation> simulations = completed.values();
//
////        for (Simulation s : simulations) {
////            if (s.isComplete()) {
////                return completed.remove(s.getID());
////            }
////        }
//
//        Collection<Simulation> completedSimulations = (Set<Simulation>) completed.values(new SqlPredicate("complete"));
//
//        return (completedSimulations.isEmpty() ? null : (Simulation)completedSimulations.toArray()[0]);
//    }

    private int getColumnCount(String result) {
        Pattern p = Pattern.compile("[0-9]+\\s#");
        Matcher m = p.matcher(new StringBuilder(result).reverse());

        m.find();

        String cols = new StringBuilder(m.group()).reverse().toString();

        return Integer.valueOf(cols.substring(2, cols.length())) + 1;
    }

    private String constructHeaders(String[] measurements, int samples) {
        StringBuilder headers = new StringBuilder();

        headers.append("# 0 - Iterations\n");

        for (int i = 0; i < measurements.length; i++) {
            for (int j = 0; j < samples; j++) {
                headers.append("# ");
                headers.append((measurements.length * i) + i + j + 1);
                headers.append(" - ");
                headers.append(measurements[i]);
                headers.append(" (");
                headers.append(j);
                headers.append(")\n");
            }
        }

        return headers.toString();
    }

    private String[] getMeasurements(String result, int measurementCount) {
        String[] measurements = new String[measurementCount];
        String m;

        Pattern p = Pattern.compile("#\\s[1-9]+\\s-.*\\s\\(");
        Matcher match = p.matcher(result);
        
        for (int i = 0; i < measurementCount; i++) {
            match.find();
            m = match.group();
            m = m.substring(m.indexOf("- ") + 2, m.length() - 2);

            measurements[i] = m;
        }

        return measurements;
    }

}
