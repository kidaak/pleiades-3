/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.simulations.selection;

import java.util.List;
import java.util.Map;
import java.util.Random;
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public class EqualProbabilitySelector implements SimulationSelector {

    @Override
    public String getKey(Map<String, List<Simulation>> jobs) {
        //RandomProvider random = new MersenneTwister();
        Random random = new Random();

        Object[] keySet = jobs.keySet().toArray();
        int keys = jobs.keySet().size();
        double rand = random.nextDouble();

        int selected = (int)Math.ceil(rand * keys) - 1;

        //System.out.println("\nJobs: " + jobs.size() + "\nKeys: " + keys + "\nSelected: " + selected + "(" + (String)keySet[selected] + ")");
        
        return (String)keySet[selected];
    }
}
