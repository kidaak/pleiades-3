/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.database;

/**
 *
 * @author bennie
 */
public interface UserDBCommunicator {
    boolean connect();
    boolean userExists(String user);
    boolean authenticateUser(String user);
    boolean silentAuthenticateUser(String user, String password);
    String registerNewUser();
    String getUserEmail(String user);
}
