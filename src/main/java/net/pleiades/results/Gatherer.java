/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.results;

import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public interface Gatherer {
    //Simulation resultsAvailable();
    Simulation gatherResults(Simulation s);
}
