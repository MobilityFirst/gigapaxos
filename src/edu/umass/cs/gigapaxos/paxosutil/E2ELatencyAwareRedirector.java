package edu.umass.cs.gigapaxos.paxosutil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.interfaces.NearestServerSelector;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationClient;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class E2ELatencyAwareRedirector implements NearestServerSelector {

	static final double PROBE_RATIO = 0.05;

	final ConcurrentHashMap<InetSocketAddress, Double> e2eLatencies = new ConcurrentHashMap<InetSocketAddress, Double>();
	final InetSocketAddress myAddress;
	private double probeRatio = PROBE_RATIO;

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
			if (probe /* && !this.e2eLatencies.containsKey(address) */)
				return address;
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

	/**
	 * @param isa
	 * @param latency
	 */
	public void learnSample(InetSocketAddress isa, double latency) {
		Double historical;
		assert (isa != null);
		if ((historical = e2eLatencies.putIfAbsent(isa, latency)) != null)
			e2eLatencies.put(isa, Util.movingAverage(latency, historical));
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
	 *
	 */
	public static class E2ELatencyAwareRedirectorTest extends DefaultTest {
		final E2ELatencyAwareRedirector redirector;

		/**
		 * 
		 */
		public E2ELatencyAwareRedirectorTest() {
			redirector = new E2ELatencyAwareRedirector();
		}

		/**
		 * 
		 */
		@Test
		public void test_PrefixMatchLength() {
			try {
				InetAddress addr1 = InetAddress.getByName("128.119.245.38");
				Assert.assertEquals(32, prefixMatch(addr1, addr1));
				Assert.assertEquals(
						27,
						prefixMatch(addr1,
								InetAddress.getByName("128.119.245.48")));
				Assert.assertEquals(
						28,
						prefixMatch(addr1,
								InetAddress.getByName("128.119.245.47")));
				Assert.assertEquals(
						12,
						prefixMatch(addr1,
								InetAddress.getByName("128.120.245.47")));
				Assert.assertEquals(
						1,
						prefixMatch(addr1,
								InetAddress.getByName("255.120.245.47")));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		/**
		 * 
		 */
		@Test
		public void test_Learning() {
			ConcurrentHashMap<InetSocketAddress, Long> groundTruth = new ConcurrentHashMap<InetSocketAddress, Long>();
			ConcurrentHashMap<InetSocketAddress, Integer> redirections = new ConcurrentHashMap<InetSocketAddress, Integer>();
			int n = 10;
			int base = 2000;
			// set up ground truth
			for (int i = 0; i < n; i++)
				groundTruth.put(new InetSocketAddress("127.0.0.1", base + i),
						(long) (4 * i + 5));
			// learn
			int samples = 100;
			for (int j = 0; j < samples; j++) {
				InetSocketAddress isa = this.redirector.getNearest(groundTruth
						.keySet());
				redirections
						.put(isa,
								redirections.containsKey(isa) ? redirections
										.get(isa) + 1 : 0);

				this.redirector.learnSample(isa, groundTruth.get(isa)
						* (1 + 0.1 * (Math.random() < 0.5 ? 1 : -1)));
			}
			for (InetSocketAddress isa : groundTruth.keySet())
				System.out.println(isa
						+ ": truth="
						+ groundTruth.get(isa)
						+ "; learned="
						+ (this.redirector.e2eLatencies.containsKey(isa) ? Util
								.df(this.redirector.e2eLatencies.get(isa))
								: "null") + "; samples="
						+ redirections.get(isa));
		}

		/**
		 * 
		 */
		@Test
		public void test_ProbeAndClosest() {
			int n = 10;
			int base = 2000;
			InetSocketAddress[] addresses = new InetSocketAddress[n];
			for (int i = 0; i < n; i++)
				addresses[i] = new InetSocketAddress("127.0.0.1", base + i);
			Set<InetSocketAddress> set1 = new HashSet<InetSocketAddress>(
					Arrays.asList(addresses[1], addresses[2], addresses[3]));
			redirector.learnSample(addresses[1], (addresses[1].getPort()-base)*10);
			InetSocketAddress closest;
			System.out.println("closest="
					+ (closest = redirector.getNearest(set1)) + " "
					+ redirector.e2eLatencies.get(closest) + "ms");
			int tries = 1000, yesCount = 0;
			for (int i = 0; i < tries; i++) {
				InetSocketAddress isa = null;
				yesCount += ((isa = redirector.getNearest(set1)).equals(addresses[1])) ? 1 : 0;
				redirector.learnSample(isa, (isa.getPort()-base)*10);
			}
			// factor 2 is for some safety but can still be exceeded
			String result = "expected=" + tries * (1 - PROBE_RATIO)
					+ "; found=" + yesCount;
			String allOK = " [all okay]";
			String failure = " [difference is too big, which is possible but unlikely]";
			Assert.assertTrue(result + failure, (yesCount > tries
					* (1 - 2 * PROBE_RATIO)));
			System.out.println(result + allOK);
		}
	}

	/**
	 * @return Test class for E2ELatencyAwareRedirector
	 */
	public Class<?> getTestClass() {
		return E2ELatencyAwareRedirectorTest.class;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		Result result = JUnitCore
				.runClasses(E2ELatencyAwareRedirectorTest.class);
		for (Failure failure : result.getFailures())
			System.out.println(failure.toString());
	}
}
