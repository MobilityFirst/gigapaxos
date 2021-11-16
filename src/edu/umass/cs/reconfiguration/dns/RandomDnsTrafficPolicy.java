package edu.umass.cs.reconfiguration.dns;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author gaozy
 *
 */
public class RandomDnsTrafficPolicy implements DnsTrafficPolicy {
	
	private static Random rand = new Random();
	
	@Override
	public Set<InetAddress> getAddresses(Set<InetAddress> addresses, InetAddress source) {
		// return a random address from the original address set
		Set<InetAddress> result = new HashSet<InetAddress>();
		List<InetAddress> targetList = new ArrayList<>(addresses);
		result.add(targetList.get(rand.nextInt(targetList.size())));
		return result;
	}
}
