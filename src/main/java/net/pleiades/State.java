/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

public enum State {
    COMPLETING,
    COMPLETED,
    EXECUTING,
    REQUESTING,
    REQUEST_SENT,
    JOB_RECEIVED,
    IDLE,
    PREPARING,
    CHECKING,
    WORKING,
    PAUSED
}
