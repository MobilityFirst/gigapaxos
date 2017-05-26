package edu.umass.cs.nio.nioutils;

import edu.umass.cs.utils.DefaultTest;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * RTTEstimator test class.
 */
public class RTTEstimatorTest extends DefaultTest {
    /**
     * @throws UnknownHostException
     */
    @Test
    public void testToInt() throws UnknownHostException {
        InetAddress addr = InetAddress.getByName("128.119.245.38");
        System.out.print((addr) + ": toInt=" + RTTEstimator.addrToInt(addr)
                + " ; toPrefixInt=" + RTTEstimator.addrToPrefixInt(addr));

    }

    /**
     * @throws UnknownHostException
     */
    @Test
    public void testRecord() throws UnknownHostException {
        InetAddress addr = InetAddress.getByName("128.119.245.38");
        RTTEstimator.record(addr, 2);
        Assert.assertEquals(2, RTTEstimator.getRTT(addr));
        RTTEstimator.record(addr, 4);
        Assert.assertEquals(2, RTTEstimator.getRTT(addr));
        RTTEstimator.record(addr, 10);
        Assert.assertEquals(4, RTTEstimator.getRTT(addr));
        System.out.print(RTTEstimator.getRTT(addr) + " ");

        RTTEstimator.record(addr, 10);
        System.out.print(RTTEstimator.getRTT(addr) + " ");

        RTTEstimator.record(addr, 10);
        System.out.print(RTTEstimator.getRTT(addr) + " ");

        RTTEstimator.record(addr, 10);
        System.out.print(RTTEstimator.getRTT(addr) + " ");
        Assert.assertEquals(7, RTTEstimator.getRTT(addr));
    }
}
