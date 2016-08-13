package edu.umass.cs.gigapaxos.interfaces;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * @author arun
 *
 */
public interface NearestServerSelector {
	/**
	 * @param addresses
	 * @return The socket address from among {@code addresses} that is the
	 *         closest to oneself. "Closest" unless otherwise defined is with
	 *         respect to the network distance or ping latency.
	 */
	public InetSocketAddress getNearest(Set<InetSocketAddress> addresses);
}
