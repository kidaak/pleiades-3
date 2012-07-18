/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import net.pleiades.utility.PleiadesConfiguration;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class Config {

    public static String jobsQueue = "simulationsMap";
    public static String runningQueue = "busyMap";
    public static String completedQueue = "completedMap";
    public static String fileQueue = "fileQueue";
    public static final int MaxJobsPerUser = 3;

    public static Options createOptions() {
        Options o = new Options();

        Option help = new Option("h", "help", false, "Display Pleiades usage information");

        Option user = OptionBuilder.withArgName("username")
                .hasArg()
                .withDescription("Pleiades username")
                .withLongOpt("user")
                .create("u");

        Option file = OptionBuilder.withArgName("input file")
                .hasArg()
                .withDescription("Cilib input file")
                .withLongOpt("input")
                .create("i");

        Option config = OptionBuilder.withArgName("config file")
                .hasArg()
                .withDescription("Pleiades configuration file (default: \"pleiades.conf\")")
                .withLongOpt("config")
                .create("c");

        Option worker = OptionBuilder.withArgName("worker count quietMode")
                .withDescription("Start Pleiades member in worker mode (ignores all other options)")
                .hasOptionalArg()
                .withLongOpt("worker")
                .create("w");

        Option jar = OptionBuilder.withArgName("jar file")
                .hasArg()
                .withDescription("Custom Cilib jar file")
                .withLongOpt("jar")
                .create("j");

        Option register = OptionBuilder.withDescription("Register a new user (ignores all other options)")
                .withLongOpt("register")
                .create("r");

        Option gatherer = OptionBuilder.withDescription("Start a Pleiades member in gatherer mode (ignores all other options)")
                .withLongOpt("gatherer")
                .create("g");

        Option monitor = OptionBuilder.withDescription("Start a Pleiades member in monitor mode (ignores all other options)")
                .withLongOpt("monitor")
                .create("m");

        o.addOption(help);
        o.addOption(user);
        o.addOption(file);
        o.addOption(config);
        o.addOption(worker);
        o.addOption(jar);
        o.addOption(register);
        o.addOption(gatherer);
        o.addOption(monitor);

        return o;
    }

    public static Properties getConfiguration(String file) {
        Properties p = new Properties();

        try {
            p.load(new FileInputStream(file));
        } catch (IOException e) {
            System.out.print("Warning: Unable to load Pleiades configuration from file \""+ file + "\". Create new configuration? (yes/no): ");
            Scanner scanner = new Scanner(System.in);
            String input = scanner.next();

            if (input.equalsIgnoreCase("yes") || input.equalsIgnoreCase("y")) {
                p = PleiadesConfiguration.configure(file);
                System.out.println("Pleiades configuration complete.");
            } else {
                System.out.println("Error: No Pleiades configuration found.");
                System.exit(1);
            }
        }

        return p;
    }
    
}
