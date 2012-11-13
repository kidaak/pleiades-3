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

    public static String simulationsMap = "simulationsMap";
    public static String runningMap = "runningMap";
    public static String completedMap = "completedMap";
    public static String fileMap = "fileMap";
    public static String errorQueue = "errorQueue";

    public static String requestTopic = "requestTopic";
    public static String tasksTopic = "tasksTopic";
    public static String resultsTopic = "resultsTopic";
    public static String errorTopic = "errorTopic";
    public static String continueTopic = "continueTopic";
    public static String heartBeatTopic = "heartBeatTopic";
    public static String distributor = "distributor";

    public static final int MaxJobsPerUser = Integer.MAX_VALUE;

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
        
        Option cont = OptionBuilder.withDescription("Attempts to continue a job that did not complete correctly")
                .withLongOpt("continue")
                .create();

        Option register = OptionBuilder.withDescription("Register a new user (ignores all other options)")
                .withLongOpt("register")
                .create("r");

        Option gatherer = OptionBuilder.withDescription("Start a Pleiades member in gatherer mode (ignores all other options)")
                .withLongOpt("gatherer")
                .create("g");

        Option monitor = OptionBuilder.withDescription("Start a Pleiades member in monitor mode (ignores all other options)")
                .withLongOpt("monitor")
                .create("m");

        Option verbose = OptionBuilder.withDescription("Hides the Pleiades text output to allow normal output stream to be displayed")
                .withLongOpt("quiet")
                .create("q");

        o.addOption(help);
        o.addOption(user);
        o.addOption(file);
        o.addOption(config);
        o.addOption(worker);
        o.addOption(jar);
        o.addOption(register);
        o.addOption(gatherer);
        o.addOption(monitor);
        o.addOption(verbose);
        o.addOption(cont);

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
