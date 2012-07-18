/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.utility;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author bennie
 */
public class PleiadesConfiguration {

    public static Properties configure(String file) {
        Properties p = new Properties();

        try {
            p.setProperty("db_url", new jline.ConsoleReader().readLine("Enter user database url: "));
            p.setProperty("db_user", new jline.ConsoleReader().readLine("Enter database username: "));
            p.setProperty("db_pass", new jline.ConsoleReader().readLine("Enter database password: "));
            p.setProperty("ci_jar", new jline.ConsoleReader().readLine("Enter url where default Cilib jar file is located: "));

            p.setProperty("java_exec_command", "java_exec_command=java -server -Xms1000M -Xmx2000M -cp $jar net.sourceforge.cilib.simulator.Main $file -textprogress");

            p.store(new FileOutputStream(file), null);

            System.out.println("java_exec_command set to 'java_exec_command=java -server -Xms1000M -Xmx2000M -cp $jar net.sourceforge.cilib.simulator.Main $file -textprogress'.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return p;
    }

}
