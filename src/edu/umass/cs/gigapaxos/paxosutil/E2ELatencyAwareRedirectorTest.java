package edu.umass.cs.gigapaxos.paxosutil;

import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class E2ELatencyAwareRedirectorTest extends DefaultTest {
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
            Assert.assertEquals(32, E2ELatencyAwareRedirector.prefixMatch(addr1, addr1));
            Assert.assertEquals(
                    27,
                    E2ELatencyAwareRedirector.prefixMatch(addr1,
                            InetAddress.getByName("128.119.245.48")));
            Assert.assertEquals(
                    28,
                    E2ELatencyAwareRedirector.prefixMatch(addr1,
                            InetAddress.getByName("128.119.245.47")));
            Assert.assertEquals(
                    12,
                    E2ELatencyAwareRedirector.prefixMatch(addr1,
                            InetAddress.getByName("128.120.245.47")));
            Assert.assertEquals(
                    1,
                    E2ELatencyAwareRedirector.prefixMatch(addr1,
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
        redirector.learnSample(addresses[1],
                (addresses[1].getPort() - base) * 10);
        InetSocketAddress closest;
        System.out.println("closest="
                + (closest = redirector.getNearest(set1)) + " "
                + redirector.e2eLatencies.get(closest) + "ms");
        int tries = 1000, yesCount = 0;
        for (int i = 0; i < tries; i++) {
            InetSocketAddress isa = null;
            yesCount += ((isa = redirector.getNearest(set1))
                    .equals(addresses[1])) ? 1 : 0;
            redirector.learnSample(isa, (isa.getPort() - base) * 10);
        }
        // factor 2 is for some safety but can still be exceeded
        String result = "expected=" + tries * (1 - E2ELatencyAwareRedirector.PROBE_RATIO)
                + "; found=" + yesCount;
        String allOK = " [all okay]";
        String failure = " [difference is too big, which is possible but unlikely]";
        Assert.assertTrue(result + failure, (yesCount > tries
                * (1 - 2 * E2ELatencyAwareRedirector.PROBE_RATIO)));
        System.out.println(result + allOK);
    }
}
