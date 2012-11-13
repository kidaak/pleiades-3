/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.simulations.creator;

import java.io.File;
import java.util.List;
import net.pleiades.simulations.Simulation;

/**
 * Defines an interface to generate individual jobs from some input file
 * ex: xml or DSL script
 *
 * @author bennie
 */
public interface SimulationsCreator {

    List<Simulation> createSimulations(File input, String fileKey);
    
}
