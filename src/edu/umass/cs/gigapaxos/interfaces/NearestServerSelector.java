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
	 * @return Nearest socket address
	 */
	public InetSocketAddress getNearest(Set<InetSocketAddress> addresses);
}
