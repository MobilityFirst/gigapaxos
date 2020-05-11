package edu.umass.cs.reconfiguration.dns;

import java.net.InetAddress;
import java.util.Set;

/**
 * @author gaozy
 *
 */
public class NoopDnsTrafficPolicy implements DnsTrafficPolicy {

	@Override
	public Set<InetAddress> getAddresses(Set<InetAddress> addresses, InetAddress source) {
		// return original address set without modification 
		return addresses;
	}
}
