package edu.umass.cs.nio.nioutils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class RTTEstimator {
	private static final int SIZE = 1024 * 1024 * 16;
	// private static final int SCALE = 4;
	private byte[] rtts = new byte[SIZE];

	/**
	 * @param address
	 * @param rtt
	 * @return {@code rtt} in quartets.
	 */
	public long record(InetAddress address, long rtt) {
		int index = addrToPrefixInt(address);
		return rtts[index] = (byte) Util.movingAverage(rtt,
				rtts[index] != 0 ? (rtts[index] < 0 ? rtts[index] + 255
						: rtts[index]) : rtt);
	}

	/**
	 * @param address
	 * @return RTT recorded for /24 prefix of {@code address}.
	 */
	public long getRTT(InetAddress address) {
		return rtts[addrToPrefixInt(address)];
	}

	/**
	 * @param address
	 * @return Integer form of address
	 */
	public static int addrToInt(InetAddress address) {
		byte[] aBytes = address.getAddress();
		return (aBytes[3] & 0xff) + ((aBytes[2] & 0xff) << 8)
				+ ((aBytes[1] & 0xff) << 16) + ((aBytes[0] & 0xff) << 24);
	}

	/**
	 * @param address
	 * @return Integer form of /24 prefix
	 */
	public static int addrToPrefixInt(InetAddress address) {
		return addrToInt(address) >>> 8; // logical, not artihmetic, shift
	}

	/**
	 * RTTEstimator test class.
	 */
	static public class RTTEstimatorTest extends DefaultTest {
		/**
		 * @throws UnknownHostException
		 */
		@Test
		public void testToInt() throws UnknownHostException {
			InetAddress addr = InetAddress.getByName("128.119.245.38");
			System.out.print((addr) + ": toInt=" + addrToInt(addr)
					+ " ; toPrefixInt=" + addrToPrefixInt(addr));

		}

		/**
		 * @throws UnknownHostException
		 */
		@Test
		public void testRecord() throws UnknownHostException {
			InetAddress addr = InetAddress.getByName("128.119.245.38");
			RTTEstimator estimator = new RTTEstimator();
			estimator.record(addr, 2);
			Assert.assertEquals(1, estimator.getRTT(addr));
			estimator.record(addr, 4);
			Assert.assertEquals(1, estimator.getRTT(addr));
			estimator.record(addr, 10);
			Assert.assertEquals(2, estimator.getRTT(addr));
			System.out.print(estimator.getRTT(addr) + " ");

			estimator.record(addr, 10);
			System.out.print(estimator.getRTT(addr) + " ");

			estimator.record(addr, 10);
			System.out.print(estimator.getRTT(addr) + " ");

			estimator.record(addr, 10);
			System.out.print(estimator.getRTT(addr) + " ");
			Assert.assertEquals(3, estimator.getRTT(addr));
		}
	}

	/**
	 * @param args
	 * @throws UnknownHostException
	 */
	public static void main(String[] args) throws UnknownHostException {
		byte[] abytes = InetAddress.getByName("128.119.245.38").getAddress();
		System.out.println(abytes[3] & 0xff);
		System.out.println((abytes[2] & 0xff) << 8);
		System.out.println((abytes[1] & 0xff) << 16);
		System.out.println((abytes[0] & 0xff) << 24);
		int n = -2139622106;
		System.out.println(n >>> 8);
		System.out.println(Math.ceil(0.5));
		System.out.println((byte) (255 + 1));
	}
}
