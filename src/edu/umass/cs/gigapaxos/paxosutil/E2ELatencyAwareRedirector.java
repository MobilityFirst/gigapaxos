package edu.umass.cs.gigapaxos.paxosutil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.NearestServerSelector;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class E2ELatencyAwareRedirector implements NearestServerSelector {

	/**
	 * The fraction of requests that are sent to a random replica as probes in
	 * order to maintain an up-to-date map of latencies to different replicas.
	 */
	public static final double PROBE_RATIO = 0.05;
	static final long MIN_PROBE_TIME = 10 * 1000; // 10s
	private static final int MAX_ENTRIES = 256;

	@SuppressWarnings("serial")
	final LinkedHashMap<InetSocketAddress, Double> e2eLatencies = new LinkedHashMap<InetSocketAddress, Double>() {
		protected boolean removeEldestEntry(
				@SuppressWarnings("rawtypes") Map.Entry eldest) {
			return size() > MAX_ENTRIES;
		}
	};
	@SuppressWarnings("serial")
	final LinkedHashMap<InetSocketAddress, Long> lastProbed = new LinkedHashMap<InetSocketAddress, Long>() {
		protected boolean removeEldestEntry(
				@SuppressWarnings("rawtypes") Map.Entry eldest) {
			return size() > MAX_ENTRIES;
		}
	};
	final InetSocketAddress myAddress;
	private double probeRatio = PROBE_RATIO;
	private long minProbeTime = MIN_PROBE_TIME;

	/**
	 * 
	 */
	public E2ELatencyAwareRedirector() {
		this(null);
	}

	/**
	 * @param myAddress
	 */
	public E2ELatencyAwareRedirector(InetSocketAddress myAddress) {
		this.myAddress = myAddress;
	}

	@Override
	public InetSocketAddress getNearest(Set<InetSocketAddress> addresses) {
		InetSocketAddress closest = null;
		Double closestLatency = null;
		boolean probe = Math.random() < this.probeRatio;
		ArrayList<InetSocketAddress> shuffled = null;

		Collections.shuffle(shuffled = new ArrayList<InetSocketAddress>(
				addresses));
		for (InetSocketAddress address : shuffled) {
			// probe with probability probeRatio
			if (probe && !this.recentlyProbed(address)) {
				// assumes that querier will use this address
				this.lastProbed.put(address, System.currentTimeMillis());
				return address;
			}
			if (closest == null)
				closest = address;
			Double latency = this.e2eLatencies.get(address);
			closestLatency = this.e2eLatencies.get(closest);
			// else pick latency if lower or current address unknown
			if (latency != null
			// will pick last (random) address initially
					&& (closestLatency != null && latency < closestLatency)) {
				closest = address;
				closestLatency = latency;
			}
		}
		return closest;
	}

	private boolean recentlyProbed(InetSocketAddress isa) {
		Long lastProbedTime = null;
		return (lastProbedTime = this.lastProbed.get(isa)) != null
				&& System.currentTimeMillis() - lastProbedTime > this.minProbeTime;
	}

	private static final double ALPHA = 1.0 / 8;

	/**
	 * @param isa
	 * @param latency
	 */
	public void learnSample(InetSocketAddress isa, double latency) {
		Double historical;
		assert (isa != null);
		if ((historical = e2eLatencies.putIfAbsent(isa, latency)) != null)
			e2eLatencies.put(isa,
					Util.movingAverage(latency, historical, ALPHA));
	}

	/**
	 * @param addr1
	 * @param addr2
	 * @return Prefix match length between addr1 and addr2
	 */
	public static final int prefixMatch(InetAddress addr1, InetAddress addr2) {
		byte[] baddr1 = addr1.getAddress();
		byte[] baddr2 = addr2.getAddress();
		int match = 0;
		int i = 0;
		byte xor = 0;
		for (i = 0; i < 4; i++)
			if ((xor = (byte) (baddr1[i] ^ baddr2[i])) == 0)
				match += 8;
			else
				break;
		int j = 0;
		if (xor != 0)
			for (j = 1; j < 8; j++)
				if ((xor >> j) == 0) {
					match += 8 - j;
					break;
				}
		return match;
	}

	/**
	 * @param time
	 */
	public void setMinProbeTime(long time) {
		this.minProbeTime = time;
	}

	/**
	 * @param ratio
	 */
	public void setProbeRatio(double ratio) {
		this.probeRatio = ratio;
	}

	/**
	 * @return Test class for E2ELatencyAwareRedirector
	 * @TODO Commented because of iOS refactoring. Will be moved to a test path.
	 */
	/*
	public Class<?> getTestClass() {

		return E2ELatencyAwareRedirectorTest.class;
	}
	*/

	public String toString() {
		return this.e2eLatencies.toString();
	}

	/**
	 * @param args
	 * @TODO Commented because of iOS refactoring. Test will be moved to a new test path
	 */
	/*

	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		Result result = JUnitCore
				.runClasses(E2ELatencyAwareRedirectorTest.class);
		for (Failure failure : result.getFailures())
			System.out.println(failure.toString());
	}
	*/
}
