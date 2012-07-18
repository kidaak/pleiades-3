/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.hazelcast.core.*;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.Set;
import java.util.List;
import jline.ConsoleReader;
import net.pleiades.simulations.Simulation;

public class Monitor implements MembershipListener, com.hazelcast.core.EntryListener {

    private PrintStream out;
    private Screen tab;
    private ConsoleReader con;

    private Set<Member> members;
    private Set<Entry<String, List<Simulation>>> jobs;
    private Set<Entry<String, Simulation>> completed;

    private enum Screen {
        Members,
        Pending,
        Running,
        Completing
    }

    public Monitor() {
        try {
            con = new ConsoleReader();
            out = System.out;            
            tab = Screen.Members;

            clearScreen();
            out.println("Connecting...");

            addListeners();
            updateSets();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void addListeners() {
        Hazelcast.<String, List<Simulation>>getMap(Config.jobsQueue).addEntryListener(this, true);
        Hazelcast.<String, Simulation>getMap(Config.completedQueue).addEntryListener(this, true);
        Hazelcast.getCluster().addMembershipListener(this);
    }

    private void printHeader() {
        out.println(Utils.header);
    }

    private void printTabs() {
        out.print("|");
        for (Screen s : Screen.values()) {
            if (tab == s) {
                out.print(" *" + s.name() + "* |");
            } else {
                out.print("  " + s.name() + "  |");
            }
        }
        out.println("\n");
    }

    private void clearScreen() {
        try {
            con.clearScreen();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void printMembersScreen() {
        int i = 0;
        for (Member m : members) {
            out.print("Member " + ++i + ": " + m.getInetSocketAddress());
            if (Hazelcast.getCluster().getLocalMember().getInetSocketAddress().equals(m.getInetSocketAddress())) {
                out.println(" [this]");
            } else {
                out.println();
            }
        }
    }

    private void printJobsScreen() {
//        if (jobs.isEmpty()) {
//            out.println("No jobs pending execution");
//        }
//
//        Set<String> done = Sets.newHashSet();
//        for (Entry<String, Simulation> u : jobs) {
//            if (done.contains(u.getKey())) {
//                continue;
//            }
//
//            done.add(u.getKey());
//            out.println("User " + u.getKey());
//
//            Set<String> jobDone = Sets.newHashSet();
//            for (Entry<String, Simulation> v : jobs) {
//                if (u.getKey().equals(v.getKey())) {
//                    if (jobDone.contains(v.getValue().getJobName())) {
//                        continue;
//                    }
//
//                    jobDone.add(v.getValue().getJobName());
//                    int taskCount = 0;
//                    int sims = 0;
//                    for (Entry<String, Simulation> w : jobs) {
//                        if (v.getValue().getJobName().equals(w.getValue().getJobName())) {
//                            taskCount += w.getValue().unfinishedCount();
//                        }
//                    }
//
//                    for (Entry<String, Simulation> w : completed) {
//                        if (v.getValue().getJobName().equals(w.getValue().getJobName())) {
//                            sims++;
//                        }
//                    }
//
//                    out.println(" - Job " + v.getValue().getJobName() + ": " + taskCount + " remaining task(s)");
//                    out.println("         " + Strings.repeat(" ", v.getValue().getJobName().length()) + sims + " remaining simulation(s)");
//                }
//            }
//        }
    }

    private void printRunningScreen() {
//        Set<String> done = Sets.newHashSet();
//        for (Entry<String, Simulation> u : completed) {
//            if (done.contains(u.getValue().getOwner())) {
//                continue;
//            }
//
//            done.add(u.getValue().getOwner());
//            out.println("User " + u.getValue().getOwner());
//
//            Set<String> jobDone = Sets.newHashSet();
//            for (Entry<String, Simulation> v : completed) {
//                if (u.getKey().equals(v.getKey())) {
//                    if (jobDone.contains(v.getValue().getJobName())) {
//                        continue;
//                    }
//
//                    jobDone.add(v.getValue().getJobName());
//                    int runningCount = 0;
//
//                    for (Entry<String, Simulation> w : jobs) {
//                        if (v.getValue().getJobName().equals(w.getValue().getJobName())) {
//                            runningCount += w.getValue().getMembers().size();
//                        }
//                    }
//
////                    for (Entry<String, Simulation> c : completed) {
////                        if (v.getValue().getJobName().equals(c.getValue().getJobName())) {
////                            runningCount += c.getValue().getSamples() - c.getValue().getResults().size();
////                        }
////                    }
////
////                    for (Entry<String, Simulation> w : jobs) {
////                        if (v.getValue().getJobName().equals(w.getValue().getJobName())) {
////                            runningCount -= w.getValue().unfinishedCount();
////                        }
////                    }
//
//                    out.println(" - Job " + v.getValue().getJobName() + ": " + runningCount + " running task(s)");
//                }
//            }
//        }
    }

    private void printCompletedScreen() {
        if (completed.isEmpty()) {
            out.println("No completed tasks");
        }
        
        Set<String> done = Sets.newHashSet();
        for (Entry<String, Simulation> u : completed) {
            if (done.contains(u.getValue().getOwner())) {
                continue;
            }

            done.add(u.getValue().getOwner());
            out.println("User " + u.getValue().getOwner());

            Set<String> jobDone = Sets.newHashSet();
            for (Entry<String, Simulation> v : completed) {
                if (u.getValue().getOwner().equals(v.getValue().getOwner())) {
                    if (jobDone.contains(v.getValue().getJobName())) {
                        continue;
                    }

                    jobDone.add(v.getValue().getJobName());                    
                    int doneCount = 0;
                    
                    for (Entry<String, Simulation> w : completed) {
                        if (v.getValue().getJobName().equals(w.getValue().getJobName())) {
                            doneCount += w.getValue().getResults().size();
                        }
                    }
                    
                    out.println(" - Job " + v.getValue().getJobName() + ": " + doneCount + " completed task(s)");
                }
            }
        }
    }

    private void printScreen() {
        clearScreen();
        printHeader();
        printTabs();

        switch (tab) {
            case Members:
                printMembersScreen();
                break;
            case Pending:
                printJobsScreen();
                break;
            case Running:
                printRunningScreen();
                break;
            case Completing:
                printCompletedScreen();
                break;
        }
    }

    public void execute() {
        try {
            while(true) {
                if (con.getInput().available() != 0) {
                    int key = con.readVirtualKey();
                    int index = 0;

                    for (Screen s : Screen.values()) {
                        if (s == tab) {
                            break;
                        }
                        index++;
                    }

                    switch (key) {
                        case 2:
                            tab = Screen.values()[(--index + Screen.values().length) % Screen.values().length];
                            break;
                        case 6:
                            tab = Screen.values()[++index % Screen.values().length];
                            break;
                        default:
                            out.print(key);
                    }

                    updateSets();
                    printScreen();
                }

                Utils.sleep(10);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void updateSets() {
        members = Sets.newHashSet(Hazelcast.getCluster().getMembers());
        jobs = Hazelcast.<String, List<Simulation>>getMap(Config.jobsQueue).entrySet();
        completed = Hazelcast.<String, Simulation>getMap(Config.completedQueue).entrySet();

        printScreen();
    }

    @Override
    public void memberAdded(MembershipEvent me) {
        updateSets();
    }

    @Override
    public void memberRemoved(MembershipEvent me) {
        updateSets();
    }

    @Override
    public void entryAdded(EntryEvent ee) {
        updateSets();
    }

    @Override
    public void entryRemoved(EntryEvent ee) {
        updateSets();
    }

    @Override
    public void entryUpdated(EntryEvent ee) {
        updateSets();
    }

    @Override
    public void entryEvicted(EntryEvent ee) {
        updateSets();
    }
}
