package edu.umass.cs.reconfiguration.interfaces;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;

/**
 * @author arun
 * 
 *         This interface defines the API available to
 *         {@link AbstractDemandProfile} implementations to get information
 *         about the application as is pertinent to the reconfiguration policy.
 *
 */
public interface ReconfigurableAppInfo {
	/**
	 * @param serviceName
	 * @return The current replica group for {@code serviceName} if replicated
	 *         locally; if {@code serviceName} is not replicated locally, this
	 *         method will return null.
	 */
	public Set<String> getReplicaGroup(String serviceName);

	/**
	 * @param serviceName
	 * @return The current snapshot of {@code serviceName}'s state as a String.
	 *         This method internally invokes
	 *         {@link Replicable#checkpoint(String)} in order to obtain a
	 *         snapshot of the state. This method is useful when the
	 *         reconfiguration decision depends on the value of
	 *         {@code serviceName}'s current state.
	 */
	public String snapshot(String serviceName);

	/**
	 * @return The current set of all active replica servers. This set is
	 *         guaranteed only to be eventually consistent but may be
	 *         intermittently stale when active replica servers are being added
	 *         to or removed from the system.
	 */
	public Map<String, InetSocketAddress> getAllActiveReplicas();
}
