/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import java.util.Properties;
import net.pleiades.cluster.HazelcastCommunicator;
import net.pleiades.results.CilibGatherer;
import net.pleiades.results.Gatherer;
import net.pleiades.simulations.Simulation;


public class ResultsListener implements EntryListener {

    private Gatherer gatherer;
    private Properties properties;

    public ResultsListener(Properties properties) {
        Utils.authenticate(properties, "admin");

        this.gatherer = new CilibGatherer(properties);
        this.properties = properties;
    }

    public void execute() {
        HazelcastCommunicator cluster = new HazelcastCommunicator();
        cluster.connect();
        System.out.println("Now connected to Pleiades Cluster.\nWaiting for results...");

        Hazelcast.<String, Simulation>getMap(Config.completedQueue).addEntryListener(this, true);
    }

    public void entryAdded(EntryEvent event) {
        System.out.println("Entry added key=" + event.getKey() + ", value=" + event.getValue() + "\n");
    }

    public void entryRemoved(EntryEvent event) {
        System.out.println("Entry removed key=" + event.getKey() + ", value=" + event.getValue() + "\n");
    }

    public void entryUpdated(EntryEvent event) {
        Simulation simulation = (Simulation)event.getValue();

        if (simulation.isComplete()) {
            System.out.println("Gathering: " + simulation.getID());
            System.out.println(simulation.getResults().size() + " tasks completed.");

            gatherer.gatherResults(simulation);

            if (simulation.jobComplete()) {
                Utils.emailUser(simulation, properties);
            }
        }
    }

    public void entryEvicted(EntryEvent event) {
        System.out.println("Entry evicted key=" + event.getKey() + ", value=" + event.getValue() + "\n");
    }
}
