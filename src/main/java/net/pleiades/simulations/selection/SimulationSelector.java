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
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public interface SimulationSelector {
    String getKey(Map<String, List<Simulation>> jobs);
}
