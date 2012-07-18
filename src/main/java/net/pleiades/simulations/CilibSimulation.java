/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.simulations;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.pleiades.Config;
import net.pleiades.tasks.CilibXMLTask;
import net.pleiades.tasks.Task;

/**
 *
 * @author bennie
 */
public class CilibSimulation implements Simulation, Serializable {
    private static final long serialVersionUID = 778767591595073953L;
    
    private String cilibInput, fileKey;
    private byte[] jar;
    private String outputFileName, outputPath;
    private int samples;
    private String owner, ownerEmail, id, jobName;
    private boolean tasksCreated;
    private Task simulationTask;
    private int unfinishedTasks;

    //private List<Task> unfinishedTasks;
    private List<InetSocketAddress> members;
    private List<String> results;

    public CilibSimulation(String cilibInput, String fileKey/*byte[] jar*/, String outputFileName, String outputPath, int samples,
            String owner, String ownerEmail, String id, String name) {
        this.owner = owner;
        this.ownerEmail = ownerEmail;
        this.jobName = name;
        this.cilibInput = cilibInput;
        //this.jar = jar;
        this.fileKey = fileKey;
        this.outputFileName = outputFileName;
        this.outputPath = outputPath;
        this.samples = samples;
        this.tasksCreated = false;

        //this.unfinishedTasks = new LinkedList<Task>();
        this.members = new LinkedList<InetSocketAddress>();
        this.results = new LinkedList<String>();

        if (!createID(id)) {
            System.out.println("Error: You may not run more than " + Config.MaxJobsPerUser + " jobs at a time.");
            System.exit(1);
        }
    }

    public CilibSimulation(CilibSimulation other) {
        this.owner = other.owner;
        this.ownerEmail = other.ownerEmail;
        this.jobName = other.jobName;
        this.samples = other.samples;
        this.id = other.id;
        this.tasksCreated = true;
        this.outputFileName = other.outputFileName;
        this.outputPath = other.outputPath;
        this.results = new LinkedList<String>();
        this.fileKey = other.fileKey;

        this.cilibInput = null;
        //this.jar = null;
        //this.unfinishedTasks = null;
        this.members = null;
    }

    @Override
    public Simulation emptyClone() {
        return new CilibSimulation(this);
    }

    @Override
    public boolean generateTasks() {
        if (tasksCreated) return true;

//        for (int i = 0; i < samples; i++) {
//            File f = new File(String.valueOf("sim" + i));
//            f.deleteOnExit();
//
//            unfinishedTasks.add(new CilibXMLTask(cilibInput, id + "_" + i, this));
//        }

        simulationTask = CilibXMLTask.of(cilibInput, id + "_", this);
        unfinishedTasks = samples;

        return tasksCreated = true;
    }

    @Override
    public boolean isComplete() {
        return (samples == results.size());
    }

    @Override
    public boolean jobComplete() {
        Map<String, Simulation> completedMap = Hazelcast.getMap(Config.completedQueue);
        Collection<Simulation> simulations = completedMap.values();

        for (Simulation s : simulations) {
            if (s.getJobID().equals(getJobID())) return false;
        }

        return true;
    }

    private boolean createID(String id) {
        Map<String, Simulation> completedMap = Hazelcast.getMap(Config.completedQueue);

        Collection<String> collection = completedMap.keySet();

        String myId = "";

        boolean validId = true;
        for (int i = 0; i < Config.MaxJobsPerUser; i++) {
            myId = id.replaceAll("_", "_" + i + "_");

            validId = true;
            for (String s : collection) {
                //System.out.println("sim id: " + s.substring(0, s.lastIndexOf("_")));
                //System.out.println("my id: " + myId.substring(0, myId.lastIndexOf("_")));
                if (s.substring(0, s.lastIndexOf("_")).equals(myId.substring(0, myId.lastIndexOf("_")))) {
                    validId = false;
                    break;
                }
            }

            if (validId) {
                this.id = myId;
                return true;
            }

        }
        
        return false;
    }

    @Override
    public String getID() {
        return id;
    }

    @Override
    public String getJobID() {
        return id.substring(0, id.lastIndexOf("_"));
    }

    @Override
    public String getOwner() {
        return owner;
    }

    @Override
    public String getOwnerEmail() {
        return ownerEmail;
    }

    @Override
    public String getJobName() {
        return jobName;
    }

    @Override
    public boolean completeTask(Task t) {
        return results.add(t.getResults());
    }

    @Override
    public boolean hasUnfinishedTasks() {
        if (unfinishedTasks > 0) {
            return true;
        }
        return false;
    }

    @Override
    public Task getUnfinishedTask() {
        return CilibXMLTask.assignNewIdTo((CilibXMLTask)simulationTask, simulationTask.getId() + String.valueOf(samples - unfinishedTasks--));
    }

    @Override
    public boolean addToMembersList(InetSocketAddress inetSocketAddress) {
        return members.add(inetSocketAddress);
    }

    @Override
    public boolean removeFromMembersList(InetSocketAddress inetSocketAddress) {
        System.out.println("Me:" + inetSocketAddress + "\nMembers List [");

        for (InetSocketAddress i : members) {
            System.out.print("*");
            System.out.println(i);
        }

        System.out.println("]");
        
        return members.remove(inetSocketAddress);
    }

    @Override
    public void writeBinary(String fileName) {
        try {
            File binary = new File(fileName);
            binary.setExecutable(true, false);
            FileOutputStream writer = new FileOutputStream(binary);

            Map<String, byte[]> fileQueue = Hazelcast.getMap(Config.fileQueue);
            byte[] bytes = fileQueue.get(fileKey);

            writer.write(bytes);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteBinary(String fileName) {
        File binary = new File(fileName);
        binary.delete();
    }

    @Override
    public List<InetSocketAddress> checkMembers() {
        Set<Member> cluster = Hazelcast.getCluster().getMembers();

        List<InetSocketAddress> deadNodes = new ArrayList<InetSocketAddress>();
        boolean dead;

        //System.out.println("Members: " + members.size());

        for (InetSocketAddress i : members) {
            //System.out.println(i);
            dead = true;
            for (Member m : cluster) {
                if (m.getInetSocketAddress().equals(i)) {
                    dead = false;
                    break;
                }
            }

            if (dead) {
                deadNodes.add(i);
                members.remove(i);
                unfinishedTasks++;
            }
        }

        return deadNodes;
    }

    @Override
    public List<String> getResults() {
        return results;
    }

    @Override
    public int getSamples() {
        return samples;
    }

    @Override
    public String getOutputFileName() {
        return outputFileName;
    }

    @Override
    public String getOutputPath() {
        return outputPath;
    }

    @Override
    public int addUnfinishedTask() {
        return ++unfinishedTasks;
    }

    @Override
    public int unfinishedCount() {
        return unfinishedTasks;
    }

    @Override
    public List<InetSocketAddress> getMembers() {
        return members;
    }
}
