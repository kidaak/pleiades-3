/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.simulations;

import java.net.InetSocketAddress;
import java.util.List;
import net.pleiades.tasks.Task;

/**
 *
 * @author bennie
 */
public interface Simulation {
    Task getUnfinishedTask();
    int getSamples();
    int addUnfinishedTask();
    int unfinishedCount();
    boolean hasUnfinishedTasks();
    boolean addToMembersList(InetSocketAddress inetSocketAddress);
    boolean removeFromMembersList(InetSocketAddress inetSocketAddress);
    boolean isComplete();
    boolean jobComplete();
    boolean generateTasks();
    boolean completeTask(Task t);
    String getID();
    String getJobID();
    String getOwner();
    String getOwnerEmail();
    String getOutputFileName();
    String getOutputPath();
    String getJobName();
    List<InetSocketAddress> getMembers();
    void writeBinary(String fileName);
    void deleteBinary(String fileName);
    List<String> getResults();
    Simulation emptyClone();
    List<InetSocketAddress> checkMembers();
}
