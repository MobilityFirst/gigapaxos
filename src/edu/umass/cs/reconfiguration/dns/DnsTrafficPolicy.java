package edu.umass.cs.reconfiguration.dns;

import java.net.InetAddress;
import java.util.Set;

/**
 * Interface to 
 * 
 * @author gaozy
 *
 */
public interface DnsTrafficPolicy {
	
	/**
	 *  
	 * @param addresses
	 * @param source
	 * @return a new set of addresses generated from original set of available addresses
	 */
	public Set<InetAddress> getAddresses(Set<InetAddress> addresses, InetAddress source);
	
}
