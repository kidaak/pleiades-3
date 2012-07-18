/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.tasks;

import java.util.Properties;
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public interface Task {
    String execute(Properties p);
    String getResults();
    String getProgress();
    String getId();
    void writeFile();
    void deleteFile();
    Simulation getParent();
}
