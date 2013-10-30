/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.simulations.selection;

import java.io.Serializable;
import net.pleiades.database.SimulationsMapStore;

/**
 *
 * @author bennie
 */
public interface SimulationSelector extends Serializable {
    String getKey(SimulationsMapStore simulationsDB);
}
