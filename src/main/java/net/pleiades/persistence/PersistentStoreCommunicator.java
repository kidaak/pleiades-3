/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.persistence;

import com.mongodb.DBObject;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public interface PersistentStoreCommunicator {
    boolean connect();
    void store(DBObject o);
    void storeAll(Map<String, Simulation> map);
    void delete(String k);
    void deleteAll(Collection<String> clctn);
    Simulation load(String k);
    Map<String, Simulation> loadAll(Collection<String> clctn);
    Set<String> loadAllKeys();
}
