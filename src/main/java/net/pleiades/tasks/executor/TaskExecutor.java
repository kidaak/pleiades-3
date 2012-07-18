/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.tasks.executor;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Transaction;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import net.pleiades.Config;
import net.pleiades.State;
import net.pleiades.Utils;
import net.pleiades.simulations.Simulation;
import net.pleiades.simulations.selection.SimulationSelector;
import net.pleiades.tasks.Task;

/**
 *
 * @author bennie
 */
public class TaskExecutor implements Executor, Runnable {
    protected State state;
    protected SimulationSelector simulationSelector;
    protected Task currentTask;
    protected boolean running;
    protected Properties properties;

    public TaskExecutor(Properties properties, SimulationSelector jobSelector) {
        this.state = State.IDLE;
        this.simulationSelector = jobSelector;
        this.running = false;
        this.properties = properties;
    }

    @Override
    public boolean executeNextTask() {
        state = State.ACQUIRING;
        currentTask = getTask();

        if (currentTask != null) {
            state = State.PREPARING;
            String bin = currentTask.getId() + ".run";
            currentTask.getParent().writeBinary(bin);
            currentTask.writeFile();
            state = State.EXECUTING;
            currentTask.execute(properties);
            currentTask.getParent().deleteBinary(bin);
            currentTask.deleteFile();
            
            return completeTask(currentTask);
        }

        return false;
    }

    private Task getTask() {
        Task t = null;

        Lock sLock = Hazelcast.getLock(Config.jobsQueue);
        
        try {
            if (sLock.tryLock(10, TimeUnit.SECONDS) == false) {
                Utils.sleep(10000);
                return null;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        Transaction txn = Hazelcast.getTransaction();
        txn.begin();

        Map<String, List<Simulation>> simulations = Hazelcast.getMap(Config.jobsQueue);

        // No sims
        if (simulations.size() == 0) {
            state = State.IDLE;
            txn.rollback();
            sLock.unlock();
            return null;
        }

//        for (Simulation s : simulations.values()) {
//            s.checkMembers();
//        }
        
        try {
            String key = simulationSelector.getKey(simulations);
            List<Simulation> collection = simulations.remove(key);

            if (collection == null) {
                state = State.IDLE;
                txn.rollback();
                sLock.unlock();
                return null;
            }

            for (Simulation s : collection) {
                if (s.hasUnfinishedTasks()) {
                    t = s.getUnfinishedTask();
                    //s.addToMembersList(Hazelcast.getCluster().getLocalMember().getInetSocketAddress());
                    break;
                }
            }

//            for (Simulation s : collection) {
//                if (s.hasUnfinishedTasks()) {
//                    simulations.put(key, s);
//                }
//            }

            Iterator<Simulation> iter = collection.iterator();

            while (iter.hasNext()) {
                if (!iter.next().hasUnfinishedTasks()) {
                    iter.remove();
                }
            }

            simulations.put(key, collection);

            /*if (t != null) {
                MultiMap<InetSocketAddress, Task> busy = Hazelcast.getMultiMap(Config.runningQueue);
                InetSocketAddress address = Hazelcast.getCluster().getLocalMember().getInetSocketAddress();
                busy.lock(address);
                busy.put(address, t);
                busy.unlock(address);
            }*/
            
            txn.commit();
        } catch (Throwable e) {
            e.printStackTrace();
            txn.rollback();
        } finally {
            sLock.unlock();
        }

        return t;
    }

    public boolean completeTask(Task t) {
        boolean completed = false;

        state = State.COMPLETING;

        Lock cLock = Hazelcast.getLock(Config.completedQueue);
        //Lock jLock = Hazelcast.getLock(Config.jobsQueue);
        cLock.lock();
        //jLock.lock();

        Map<String, Simulation> completedMap = Hazelcast.getMap(Config.completedQueue);
        //MultiMap<String, Simulation> jobsMap = Hazelcast.getMultiMap(Config.jobsQueue);
        
        Transaction txn = Hazelcast.getTransaction();
        txn.begin();
        
        try {
            Simulation cSimulation = completedMap.remove(t.getParent().getID());
            completed = cSimulation.completeTask(t);
            completedMap.put(t.getParent().getID(), cSimulation);

//            ArrayList<Simulation> collection = (ArrayList)jobsMap.get(t.getParent().getOwner());
//            Simulation jSimulation = collection.get(collection.indexOf(t.getParent()));
//            jSimulation.removeFromMembersList(Hazelcast.getCluster().getLocalMember().getInetSocketAddress());

            /*MultiMap<InetSocketAddress, Task> busy = Hazelcast.getMultiMap(Config.runningQueue);
            InetSocketAddress address = Hazelcast.getCluster().getLocalMember().getInetSocketAddress();
            busy.lock(address);
            if (!busy.remove(address, t)) {
                System.out.println("Could not do the thingy with the doodah");
            }
            busy.unlock(address);*/

            txn.commit();
        } catch (Throwable e) {
            e.printStackTrace();
            txn.rollback();
            completed = false;
        } finally {
            //jLock.unlock();
            cLock.unlock();
        }

        state = State.COMPLETED;
        
        return completed;
    }

    public State getState() {
        return state;
    }

    @Override
    public void run() {
        this.running = true;

        //execute jobs from distributed map
        while (running) {
            Utils.unlockAll();
            if (!executeNextTask()) {
                Utils.sleep(1000);
            }
        }
    }

    @Override
    public String getStateString() {
        String taskName, progress;
        if (currentTask == null || state == State.ACQUIRING || state == State.IDLE) {
            taskName = "(No task)";
            progress = "(No progress)";

        } else {
            taskName = currentTask.getId();
            progress = currentTask.getProgress();
        }
        
        return state.name() + " " + taskName + " " + progress;
    }

    @Override
    public void kill() {
        this.running = false;
    }
}
