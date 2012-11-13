/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.tasks.executor;

/**
 *
 * @author bennie
 */
public interface Executor {
    void requestNewTask();
    void stop();
    void start();
    void toggle();
    String getStateString();
}
