/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.cluster;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Instance;
import com.hazelcast.core.InstanceEvent;
import com.hazelcast.core.InstanceListener;
import java.util.Collection;

/**
 *
 * @author bennie
 */
public class HazelcastCommunicator implements ClusterCommunicator, InstanceListener {
    Collection<Instance> instances;

    @Override
    public void connect() {
        instances = Hazelcast.getInstances();
    }

    @Override
    public void instanceCreated(InstanceEvent event) {
        Instance instance = event.getInstance();
	System.out.println("Created " + instance.getInstanceType() + "," + instance.getId());
    }

    @Override
    public void instanceDestroyed(InstanceEvent event) {
        Instance instance = event.getInstance();
	System.out.println("Destroyed " + instance.getInstanceType() + "," + instance.getId());
    }
}
