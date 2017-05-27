package edu.umass.cs.nio.nioutils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class RTTEstimator {
	private static final int SIZE = 1024 * 1024 * 16;
	// private static final int SCALE = 4;
	private static final double ALPHA = 0.25;
	/**
	 * byte will take us only up to 255ms. This array is static because it might
	 * as well be shared by all nodes on a single physical machine and certainly
	 * within a JVM.
	 */
	private static byte[] rtts = new byte[SIZE];
	private static BitSet bitset = new BitSet(SIZE);

	private static Timer timer = new Timer(true);

	/**
	 * Relies on Java implementation of (typically) ICMP echo.
	 * 
	 * @param address
	 */
	public static void record(InetAddress address) {
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					record(address, ping(address));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}, 0);
	}

	/**
	 * @param address
	 * @param rtt
	 * @return {@code rtt} in quartets.
	 */
	public static long record(InetAddress address, long rtt) {
		if (rtt <= 0)
			return rtt;
		int index = addrToPrefixInt(address);
		bitset.set(index);
		insertTestMap(address, rtt); // for testing only
		return rtts[index] = (byte) Util.movingAverage(rtt,
				rtts[index] != 0 ? (rtts[index] < 0 ? rtts[index] + 255
						: rtts[index]) : rtt, ALPHA);
	}

	/**
	 * @param address
	 * @return RTT recorded for /24 prefix of {@code address} if any, else -1.
	 */
	public static long getRTT(InetAddress address) {
		int index = addrToPrefixInt(address);
		if (bitset.get(index))
			return rtts[addrToPrefixInt(address)];
		else
			return -1;
	}

	private static final int PING_TIMEOUT = 1000; // milliseconds

	/**
	 * @param address
	 * @return RTT to address via a blocking ping. The implementation is JVM and
	 *         platform dependent.
	 * @throws IOException
	 */
	public static long ping(InetAddress address) throws IOException {
		long t = System.currentTimeMillis();
		address.isReachable(PING_TIMEOUT);
		long delay = System.currentTimeMillis() - t;
		if (delay < PING_TIMEOUT)
			return delay;
		return -1;
	}

	/**
	 * Utility methods to fetch Amazon EC2 information although that isn't used
	 * by the estimation methods in this class.
	 */
	private static final String AWS_IP_RANGES = "https://ip-ranges.amazonaws.com/ip-ranges.json";
	private static final String AWS_PREFIXES_KEY = "prefixes";
	private static final String AWS_IP_PREFIX_KEY = "ip_prefix";
	private static final String AWS_REGION_KEY = "region";
	private static JSONObject awsIPRanges = null;
	private static ConcurrentHashMap<String, String> awsIPToRegion = new ConcurrentHashMap<String, String>();

	/**
	 * @return AWS IP ranges as a JSON object
	 * @throws IOException
	 * @throws JSONException
	 */
	public static JSONObject getAmazonIPToRegionFile() throws IOException,
			JSONException {
		URL url = new URL(AWS_IP_RANGES);
		InputStream is = url.openStream();
		int ptr = 0;
		StringBuffer buffer = new StringBuffer();
		while ((ptr = is.read()) != -1)
			buffer.append((char) ptr);
		if (buffer.length() > 0)
			return awsIPRanges = new JSONObject(buffer.toString());
		return null;
	}

	/**
	 * @return IP-as-string to region map.
	 */
	protected static ConcurrentHashMap<String, String> populateAWSIPToRegionMap() {
		if (awsIPRanges == null)
			try {
				getAmazonIPToRegionFile();
			} catch (IOException | JSONException e) {
				e.printStackTrace();
			}
		if (awsIPRanges != null) {
			try {
				JSONArray jarray = awsIPRanges.has(AWS_PREFIXES_KEY) ? awsIPRanges
						.getJSONArray(AWS_PREFIXES_KEY) : null;
				for (int i = 0; i < jarray.length(); i++)
					awsIPToRegion.put((jarray.getJSONObject(i)
							.getString(AWS_IP_PREFIX_KEY)), jarray
							.getJSONObject(i).getString(AWS_REGION_KEY));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		return awsIPToRegion;
	}

	/**
	 * An IPv4 address and a netmask length in the standard CIDR notation.
	 */
	public static class IPv4Prefix {
		final InetAddress address;
		final int mask;

		/**
		 * @param address
		 * @param mask
		 */
		public IPv4Prefix(InetAddress address, int mask) {
			this.address = address;
			this.mask = mask;
		}

		/**
		 * @param prefix
		 * @throws UnknownHostException
		 */
		public IPv4Prefix(String prefix) throws UnknownHostException {
			String strAddr = prefix.replaceAll("/.*", "");
			this.address = InetAddress.getByName(strAddr);
			this.mask = Integer.valueOf(prefix.replaceAll(".*/", ""));
		}

		public String toString() {
			return address.getHostAddress() + "/" + mask;
		}
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

	private static LinkedHashMap<InetAddress, Long> testMap = new LinkedHashMap<InetAddress, Long>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		protected boolean removeEldestEntry(Map.Entry<InetAddress, Long> eldest) {
			if (this.size() >= MAX_ENTRIES)
				return true;
			return false;
		}
	};

	private static void insertTestMap(InetAddress address, Long delay) {
		byte[] bytes = address.getAddress();
		bytes[3] = 0;
		try {
			testMap.put(InetAddress.getByAddress(bytes), delay);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return String representation of bounded-size map; used only for
	 *         instrumentation.
	 */
	public static String print() {
		return "[RTTs(ms): " + testMap.toString() + "]";
	}

	private static final int MAX_ENTRIES = 100;

	private static LinkedHashMap<Integer, LinkedHashSet<InetAddress>> closest = new LinkedHashMap<Integer, LinkedHashSet<InetAddress>>() {
		private static final long serialVersionUID = 1L;

		protected boolean removeEldestEntry(
				Map.Entry<Integer, LinkedHashSet<InetAddress>> eldest) {
			return size() > MAX_ENTRIES;
		}
	};

	/**
	 * @param sender
	 * @param nearestMap
	 */
	public static void closest(InetSocketAddress sender,
			Map<InetAddress, Long> nearestMap) {
		closest.put(new Integer(addrToPrefixInt(sender.getAddress())),
				new LinkedHashSet<InetAddress>(nearestMap.keySet()));
	}

	/**
	 * @param clientAddr
	 * @return The closest servers to {@code clientAddr} if any recorded.
	 */
	public static Set<InetAddress> getClosest(InetAddress clientAddr) {
		return closest.get(addrToPrefixInt(clientAddr));
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
