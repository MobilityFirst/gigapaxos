/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. */
package edu.umass.cs.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 *
 *         Various generic static utility methods.
 */
@SuppressWarnings("javadoc")
public class Util {

	private static Logger log = Logger.getLogger(Util.class.getName());

	public static final DecimalFormat decimalFormat = new DecimalFormat("#.#");
	public static final double ALPHA = 0.05; // sample weight

	public static final String df(double d) {
		return decimalFormat.format(d);
	}

	public static final String ms(double d) {
		return decimalFormat.format(d) + "ms";
	} // milli to microseconds

	public static final String mu(double d) {
		return decimalFormat.format(d * 1000) + "us";
	} // milli to microseconds

	public static final String nmu(double d) {
		return decimalFormat.format(d / 1000.0) + "us";
	} // milli to microseconds

	public static final double movingAverage(double sample,
			double historicalAverage, double alpha) {
		return (1 - alpha) * ((double) historicalAverage) + alpha
				* ((double) sample);
	}

	public static final double movingAverage(double sample,
			double historicalAverage) {
		return movingAverage(sample, historicalAverage, ALPHA);
	}

	public static final double movingAverage(long sample,
			double historicalAverage) {
		return movingAverage((double) sample, historicalAverage);
	}

	public static final double movingAverage(long sample,
			double historicalAverage, double alpha) {
		return movingAverage((double) sample, historicalAverage, alpha);
	}

	public static boolean oneIn(int n) {
		return Math.random() < 1.0 / Math.max(1, n) ? true : false;
	}

	public static int roundToInt(double d) {
		return (int) Math.round(d);
	}

	public static void assertAssertionsEnabled() {
		boolean assertOn = false;
		// *assigns* true if assertions are on.
		assert assertOn = true;
		if (!assertOn) {
			throw new RuntimeException(
					"Asserts not enabled; enable assertions using the '-ea' JVM option");
		}
	}

	public static String prefix(String str, int prefixLength) {
		if (str == null || str.length() <= prefixLength) {
			return str;
		}
		return str.substring(0, prefixLength);
	}

	public static Set<Integer> arrayToIntSet(int[] array) {
		TreeSet<Integer> set = new TreeSet<Integer>();
		for (int i = 0; i < array.length; i++) {
			set.add(array[i]);
		}
		return set;
	}

	public static int[] filter(int[] array, int member) {
		for (int a : array)
			if (a == member) {
				int[] filtered = new int[array.length - 1];
				int j = 0;
				for (int b : array)
					if (b != member)
						filtered[j++] = b;
				return filtered;
			}
		return array;
	}

	public static Set<String> setToStringSet(Set<?> set) {
		Set<String> result = new HashSet<String>();
		for (Object id : set) {
			result.add(id.toString());
		}
		return result;
	}

	// will throw exception if any string is not parseable as integer
	public static Set<Integer> stringSetToIntegerSet(Set<String> set) {
		Set<Integer> intIDs = new HashSet<Integer>();
		for (String s : set)
			intIDs.add(Integer.valueOf(s));
		return intIDs;
	}

	public static int[] setToIntArray(Set<Integer> set) {
		int[] array = new int[set.size()];
		int i = 0;
		for (int id : set) {
			array[i++] = id;
		}
		return array;
	}

	public static Object[] setToNodeIdArray(Set<?> set) {
		Object[] array = new Object[set.size()];
		int i = 0;
		for (Object id : set) {
			array[i++] = id;
		}
		return array;
	}

	public static Integer[] setToIntegerArray(Set<Integer> set) {
		Integer[] array = new Integer[set.size()];
		int i = 0;
		for (Integer id : set) {
			array[i++] = id;
		}
		return array;
	}

	public static int[] stringToIntArray(String string) {
		string = string.replaceAll("\\[", "").replaceAll("\\]", "")
				.replaceAll("\\s", "");
		String[] tokens = string.split(",");
		int[] array = new int[tokens.length];
		for (int i = 0; i < array.length; i++) {
			array[i] = Integer.parseInt(tokens[i]);
		}
		return array;
	}

	public static Set<String> stringToStringSet(String string)
			throws JSONException {
		JSONArray jsonArray = new JSONArray(string);
		Set<String> set = new HashSet<String>();
		for (int i = 0; i < jsonArray.length(); i++)
			set.add(jsonArray.getString(i));
		return set;
	}

	// to test the json-smart parser
	public static JSONObject toJSONObject(String s) throws JSONException {
		net.minidev.json.JSONObject sjson = (net.minidev.json.JSONObject) net.minidev.json.JSONValue
				.parse(s);
		JSONObject json = new JSONObject();
		for (String key : sjson.keySet()) {
			Object obj = sjson.get(key);
			if (obj instanceof Collection<?>)
				json.put(key, new JSONArray(obj.toString()));
			else
				json.put(key, obj);
		}
		return json;
	}

	public static Integer[] intToIntegerArray(int[] array) {
		if (array == null) {
			return null;
		} else if (array.length == 0) {
			return new Integer[0];
		}
		Integer[] retarray = new Integer[array.length];
		int i = 0;
		for (int member : array) {
			retarray[i++] = member;
		}
		return retarray;
	}

	public static Set<String> arrayOfIntToStringSet(int[] array) {
		Set<String> set = new HashSet<String>();
		for (Integer member : array) {
			set.add(member.toString());
		}
		return set;
	}

	public static String arrayOfIntToString(int[] array) {
		String s = "";
		for (int i = 0; i < array.length; i++) {
			s += array[i];
			s += (i < array.length - 1 ? "," : "");
		}
		return "[" + s + "]";
	}

	public static boolean contains(int member, int[] array) {
		for (int i = 0; i < array.length; i++) {
			if (array[i] == member) {
				return true;
			}
		}
		return false;
	}

	public static Set<String> arrayOfObjectsToStringSet(Object[] array) {
		Set<String> set = new HashSet<String>();
		for (Object member : array) {
			set.add(member.toString());
		}
		return set;
	}

	// FIXME: Is there a sublinear method to return a random member from a set?
	public static Object selectRandom(Collection<?> set) {
		int random = (int) (Math.random() * set.size());
		Iterator<?> iterator = set.iterator();
		Object randomNode = null;
		for (int i = 0; i <= random && iterator.hasNext(); i++) {
			randomNode = iterator.next();
		}
		return randomNode;
	}

	public static InetSocketAddress getInetSocketAddressFromString(String s) {
		// remove anything upto and including the first slash
		// handles this: "10.0.1.50/10.0.1.50:24404"
		s = s.replaceAll(".*/", "");
		String[] tokens = s.split(":");
		if (tokens.length < 2) {
			return null;
		}
		return new InetSocketAddress(tokens[0], Integer.valueOf(tokens[1]));
	}

	// assumes strict formatting and is more efficient
	public static InetSocketAddress getInetSocketAddressFromStringStrict(
			String s) {
		String[] tokens = s.split(":");
		if (tokens.length < 2) {
			return null;
		}
		return new InetSocketAddress(tokens[0], Integer.valueOf(tokens[1]));
	}

	public static InetAddress getInetAddressFromString(String s)
			throws UnknownHostException {
		return InetAddress.getByName(s.replaceFirst(".*/", ""));
	}

	public static String toJSONString(Collection<?> collection) {
		JSONArray jsonArray = new JSONArray(collection);
		return jsonArray.toString();
	}

	public static String[] jsonToStringArray(String jsonString)
			throws JSONException {
		JSONArray jsonArray = new JSONArray(jsonString);
		String[] stringArray = new String[jsonArray.length()];
		for (int i = 0; i < jsonArray.length(); i++) {
			stringArray[i] = jsonArray.getString(i);
		}
		return stringArray;
	}

	public static String toJSONString(int[] array) {
		return toJSONString(Util.arrayOfIntToStringSet(array));
	}

	public static ArrayList<Integer> JSONArrayToArrayListInteger(
			JSONArray jsonArray) throws JSONException {
		ArrayList<Integer> list = new ArrayList<Integer>();
		for (int i = 0; i < jsonArray.length(); i++) {
			list.add(jsonArray.getInt(i));
		}
		return list;
	}

	public void assertEnabled() {
		try {
			assert (false);
		} catch (Exception e) {
			return;
		}
		throw new RuntimeException("Asserts not enabled; exiting");
	}

	/* The methods below return an Object with a toString method so that the
	 * string won't actually get created until its toString method is invoked.
	 * This is useful to optimize logging. */

	public static Object truncate(final Object obj, final int size) {
		return new Object() {
			@Override
			public String toString() {
				String str = obj.toString();
				return str == null || str.length() <= size ? str
						: str != null ? str.substring(0, size)
								+ "[..truncated]" : null;
			}
		};
	}

	public static Object truncate(final Object obj, final int prefixSize,
			final int suffixSize) {
		if (obj == null)
			return null;
		final int size = prefixSize + suffixSize;
		return new Object() {
			@Override
			public String toString() {
				String str = obj.toString();
				return str == null || str.length() < size ? str
						: str != null ? str.substring(0, prefixSize) + "[...]"
								+ str.substring(str.length() - suffixSize)
								: null;
			}
		};
	}

	public static byte[] getAlphanumericAsBytes() {
		int low = '0', high = 'z';
		byte[] bytes = new byte[high - low + 1];
		for (int i = 0; i < bytes.length; i++)
			bytes[i] = (byte) (low + i);
		return bytes;
	}

	public static byte[] getRandomAlphanumericBytes() {
		return getRandomAlphanumericBytes(1024);
	}

	public static byte[] getRandomAlphanumericBytes(int size) {
		byte[] an = Util.getAlphanumericAsBytes();
		byte[] msg = new byte[size];
		for (int i = 0; i < msg.length; i++)
			msg[i] = an[(int) (Math.random() * an.length)];
		return msg;
	}

	private static Collection<?> truncate(Collection<?> list, int size) {
		if (list.size() <= size)
			return list;
		ArrayList<Object> truncated = new ArrayList<Object>();
		int i = 0;
		for (Object o : list)
			if (i++ < size)
				truncated.add(o);
			else
				break;
		return truncated;
	}

	public static Object truncatedLog(final Collection<?> list, final int size) {
		return new Object() {
			public String toString() {
				return truncate(list, size).toString()
						+ (list.size() <= size ? "" : "...");
			}
		};
	}

	public static Object suicide(Logger logger, String error) {
		logger.severe(error);
		new RuntimeException(error).printStackTrace();
		System.exit(1);
		return null; // will never come here
	}

	public static Object suicide(String error) {
		return suicide(log, error);
	}
	
	public static String getCommaSeparated(List<?> list) {
		return list.toString().replaceAll("^\\[", "").replaceAll("\\]$", "");
	}

	/**
	 * Transfer from src to dst without throwing exception if src.remaining() >
	 * dst.remaining() but copying dst.remaining() bytes from src instead.
	 * {@code transferLimit} further limits the number of bytes transferred from
	 * src to dst.
	 */
	public static ByteBuffer put(ByteBuffer dst, ByteBuffer src,
			int transferLimit) {
		if (src.remaining() <= dst.remaining()
				&& transferLimit >= dst.remaining())
			return dst.put(src);
		int oldLimit = src.limit();
		src.limit(src.position() + Math.min(dst.remaining(), transferLimit));
		dst.put(src);
		src.limit(oldLimit);
		return dst;
	}

	public static ByteBuffer put(ByteBuffer dst, ByteBuffer src) {
		return put(dst, src, dst.remaining());
	}

	private static final String CHARSET = "ISO-8859-1";

	private static String sockAddrToEncodedString(InetSocketAddress isa)
			throws UnsupportedEncodingException {
		byte[] address = isa.getAddress().getAddress();
		byte[] buf = new byte[address.length + 2];
		for (int i = 0; i < address.length; i++)
			buf[i] = address[i];
		buf[address.length] = (byte) (isa.getPort() >> 8);
		buf[address.length + 1] = (byte) (isa.getPort() & 255);
		return new String(buf, CHARSET);

	}

	private static InetSocketAddress encodedStringToInetSocketAddress(String str)
			throws UnknownHostException, UnsupportedEncodingException {
		byte[] buf = str.getBytes(CHARSET);
		int port = (int) (buf[buf.length - 2] << 8)
				+ (buf[buf.length - 1] & 255);
		return new InetSocketAddress(InetAddress.getByAddress(Arrays
				.copyOfRange(buf, 0, 4)), port);
	}

	public static byte[] longToBytes(long value) {
		int size = Long.SIZE / 8;
		byte[] buf = new byte[size];
		for (int i = 0; i < size; i++)
			buf[i] = (byte) ((value >> ((size - i - 1) * 8)) & 255);
		return buf;
	}

	public static String longToEncodedString(long value)
			throws UnsupportedEncodingException {
		return new String(longToBytes(value), CHARSET);
	}

	public static long bytesToLong(byte[] bytes) {
		long value = 0;
		for (int i = 0; i < bytes.length; i++)
			value += ((long) (bytes[i] & 255) << ((bytes.length - i - 1) * 8));
		return value;
	}

	public static long encodedStringToLong(String str)
			throws UnsupportedEncodingException {
		return Util.bytesToLong(str.getBytes(CHARSET));
	}

	public static long toLong(Object obj) {
		if (obj instanceof Long)
			return (long) obj;
		return (int) obj;
	}

	public static Set<Integer> toIntSet(int i) {
		Set<Integer> set = new HashSet<Integer>();
		set.add(i);
		return set;
	}

	// TEST CODE

	private static void testGetInetSocketAddressFromString() {
		assert (getInetSocketAddressFromString("10.0.1.50/10.0.1.50:24404")
				.equals(new InetSocketAddress("10.0.1.50", 24404)));
	}

	private static void testGetInetAddressFromString()
			throws UnknownHostException {
		assert (getInetAddressFromString("10.0.1.50/10.0.1.50:24404")
				.equals(InetAddress.getByName("10.0.1.50")));
	}

	private static void testToBytesAndBack() throws UnknownHostException,
			UnsupportedEncodingException {
		InetSocketAddress isa = new InetSocketAddress("128.119.235.43", 23451);
		assert (Util.encodedStringToInetSocketAddress(Util
				.sockAddrToEncodedString(isa)).equals(isa));
		int n = 10000;
		for (int i = 0; i < n; i++) {
			long t = (long) (Math.random() * Long.MAX_VALUE);
			byte[] buf = (Util.longToBytes(t));
			assert (t == Util.bytesToLong(buf));
		}
		for (int i = 0; i < n; i++) {
			long value = (long) (Math.random() * Long.MAX_VALUE);
			assert (value == Util.encodedStringToLong(Util
					.longToEncodedString(value)));
		}
		for (int i = 0; i < n; i++) {
			byte[] address = new byte[4];
			for (int j = 0; j < 4; j++)
				address[j] = (byte) (Math.random() * Byte.MAX_VALUE);
			InetSocketAddress sockAddr = new InetSocketAddress(
					InetAddress.getByAddress(address),
					(int) (Math.random() * Short.MAX_VALUE));
			assert (Util.encodedStringToInetSocketAddress(Util
					.sockAddrToEncodedString(sockAddr)).equals(sockAddr));
		}
	}

	public static Object getRandomOtherThan(Set<?> all, Set<?> exclude) {
		Object[] allArray = all.toArray();
		int index = -1;
		if (exclude.containsAll(all))
			return null;
		while (exclude
				.contains(allArray[index = (int) (Math.random() * allArray.length)]))
			;
		return allArray[index];
	}

	public static Object getRandomOtherThan(Set<?> all, Object exclude) {
		for (Object obj : all)
			if (!obj.equals(exclude))
				return obj;
		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Set<?> getOtherThan(Set<?> set, Object exclude) {
		Set<?> copy = new HashSet(set);
		copy.remove(exclude);
		return copy;
	}

	public static Object getOtherThanString(Set<?> set, Object exclude) {
		return new Object() {
			public String toString() {
				@SuppressWarnings({ "rawtypes", "unchecked" })
				Set<?> copy = new HashSet(set);
				copy.remove(exclude);
				return copy.toString();
			}
		};
	}

	private static final Set<Class<?>> WRAPPER_TYPES = getWrapperTypes();

	public static boolean isWrapperType(Class<?> clazz) {
		return WRAPPER_TYPES.contains(clazz);
	}

	private static Set<Class<?>> getWrapperTypes() {
		Set<Class<?>> ret = new HashSet<Class<?>>();
		ret.add(Boolean.class);
		ret.add(Character.class);
		ret.add(Byte.class);
		ret.add(Short.class);
		ret.add(Integer.class);
		ret.add(Long.class);
		ret.add(Float.class);
		ret.add(Double.class);
		ret.add(Void.class);
		return ret;
	}

	// converts to org.json JSON via json-smart
	public static JSONObject viaJSONSmart(net.minidev.json.JSONObject jsonS)
			throws JSONException {
		JSONObject json = new JSONObject();
		for (String key : jsonS.keySet()) {
			if (jsonS.get(key) != null) {
				// Object val = jsonS.get(key);
				// if(WRAPPER_TYPES.contains(val.getClass()))
				json.put(key, jsonS.get(key));
				// else
				// json.put("command",
				// viaJSONSmart((net.minidev.json.JSONObject) jsonS
				// .get("command")));
			}
		}
		return json;
	}

	public static InetSocketAddress offsetPort(InetSocketAddress isa, int offset) {
		return new InetSocketAddress(isa.getAddress(), isa.getPort() + offset);
	}

	public static Set<?> removeFromSetCopy(Set<?> set, Object element) {
		if (set != null && element != null) {
			Set<?> copy = new HashSet<>(set);
			copy.remove(element);
			return copy;
		}
		return set;
	}

	public static Set<?> removeFromSet(Set<?> set, Object element) {
		if (set != null && element != null)
			set.remove(element);
		return set;
	}

	// return s1 - s2
	public static Set<Object> diff(Set<?> s1, Set<?> s2) {
		Set<Object> diff = new HashSet<Object>();
		for (Object node : s1)
			if (!s2.contains(node))
				diff.add(node);
		return diff;
	}

	public static void main(String[] args) throws UnsupportedEncodingException,
			UnknownHostException {
		Util.assertAssertionsEnabled();
		testGetInetSocketAddressFromString();
		testGetInetAddressFromString();
		testToBytesAndBack();
		System.out.println(recursiveFind("/Users/arun/gigapaxos/src/",
				".*paxosutil"));
		System.out.println("SUCCESS!");
		// testGetPublicInetSocketAddressFromString();
		System.out.println(Util.class.getClassLoader().getResource(".")
				.toString().replace("file:", ""));
	}

	public static boolean recursiveRemove(File file) {
		boolean deleted = true;
		if (file.isDirectory())
			for (File f : file.listFiles())
				if (f.isFile())
					deleted = deleted && f.delete();
				else
					recursiveRemove(f);
		return deleted && file.delete();
	}

	/**
	 * 
	 * @param dir
	 * @param match
	 * @return Files (recursively) within {@code dir} matching any of the match
	 *         patterns in {@code match}.
	 */
	public static File[] getMatchingFiles(String dir, String... match) {
		File dirFile = new File(dir);
		Set<File> matchFiles = new HashSet<File>();
		for (String m : match)
			if (dirFile.getPath().toString().startsWith(m.replaceAll("/$", "")))
				matchFiles.add(dirFile);
		if (dirFile.isDirectory()) {
			// check constituent files in directory
			for (File f : dirFile.listFiles())
				matchFiles.addAll(Arrays.asList(getMatchingFiles(f.getPath(),
						match)));
		}
		return matchFiles.toArray(new File[0]);
	}

	public static boolean recursiveRemove(String dir, String... match) {
		boolean deleted = true;
		for (File f : getMatchingFiles(dir, match))
			deleted = Util.recursiveRemove(f) && deleted;
		return deleted;
	}

	public static ArrayList<String> recursiveFind(String dir, String regex) {
		ArrayList<String> matches = new ArrayList<String>();
		for (File f : new File(dir).listFiles()) {
			if (f.toString().matches(regex))
				matches.add(f.toString());
			if (f.isDirectory())
				matches.addAll(recursiveFind(f.toString(), regex));
		}
		return matches;
	}

	public static void wipeoutFile(String filename) throws IOException {
		FileWriter writer = null;
		try {
			(writer = new FileWriter(filename, false)).close();
		} finally {
			writer.close();
		}
	}

	public static Set<InetAddress> socketAddressesToInetAddresses(
			InetSocketAddress[] sockAddrs) {
		InetAddress[] IPs = new InetAddress[sockAddrs.length];
		for (int i = 0; i < sockAddrs.length; i++)
			IPs[i] = sockAddrs[i].getAddress();
		return new HashSet<InetAddress>(Arrays.asList(IPs));
	}

	public static Object printThisLine() {
		return printThisLine(true);

	}

	public static Object printThisLine(Boolean log) {
		return log ? new Object() {
			public String toString() {
				StackTraceElement ste = Thread.currentThread().getStackTrace()[log == null ? 5
						: 4];
				return ste.getFileName() + ":" + ste.getLineNumber();
			}
		}
				: null;
	}

	/**
	 * Sorts a map by value.
	 * 
	 * @param <K>
	 * @param <V>
	 * @param map
	 * @return the sorted map
	 */
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
			Map<K, V> map) {
		Map<K, V> result = new LinkedHashMap<>();
		Stream<Map.Entry<K, V>> st = map.entrySet().stream();

		st.sorted(Map.Entry.comparingByValue()).forEachOrdered(
				new Consumer<Entry<K, V>>() {
					@Override
					public void accept(Entry<K, V> entry) {
						result.put(entry.getKey(), entry.getValue());
					}
				});
		// Android doesn't like Lambdas - Westy 10/16
		// st.sorted(Map.Entry.comparingByValue()).forEachOrdered(
		// e -> result.put(e.getKey(), e.getValue()));

		return result;
	}

	public static Set<InetSocketAddress> getSocketAddresses(JSONArray jarray)
			throws JSONException {
		Set<InetSocketAddress> addresses = new HashSet<InetSocketAddress>();
		if (jarray != null)
			for (int i = 0; i < jarray.length(); i++)
				addresses.add(Util.getInetSocketAddressFromString(jarray
						.getString(i)));
		return addresses;
	}

	public static JSONArray getJSONArray(Set<InetSocketAddress> addresses)
			throws JSONException {
		JSONArray jarray = new JSONArray();
		for (InetSocketAddress isa : addresses)
			jarray.put(isa.getAddress().getHostAddress() + ":" + isa.getPort());
		return jarray;
	}

	/**
	 * @param entryServer
	 * @return Returns a canonical string form that can be easily converted back
	 *         to the original socket address.
	 */
	public static String toString(InetSocketAddress entryServer) {
		return entryServer.getAddress().getHostAddress() + ":"
				+ entryServer.getPort();
	}

	/* This method converts a JSONArray to an ArrayList while recursively
	 * converting the elements to JSONObject or JSONArray as needed. */
	public static List<?> JSONArrayToList(JSONArray jsonArray)
			throws JSONException {
		if (jsonArray == null)
			return null;
		ArrayList<Object> list = new ArrayList<Object>();
		Object val = null;

		if (jsonArray.length() > 0)
			for (int i = 0; i < jsonArray.length(); i++)
				if ((val = jsonArray.get(i)) instanceof JSONObject)
					list.add(JSONObjectToMap((JSONObject) val));

				else if (val instanceof JSONArray)
					list.add(JSONArrayToList((JSONArray) val));

				else
					// primitive type object
					list.add(val);
		return list;
	}

	/**
	 * Get an array of field names from a JSONObject.
	 * COPIED from org.json - Android's moving this method to util to preserve
	 * Android and iOS compatibility.
	 * @return An array of field names, or null if there are no names.
	 */
	public static String[] getNames(JSONObject jo) {
		int length = jo.length();
		if (length == 0) {
			return null;
		}
		Iterator<?> iterator = jo.keys();
		String[] names = new String[length];
		int i = 0;
		while (iterator.hasNext()) {
			names[i] = (String) iterator.next();
			i += 1;
		}
		return names;
	}

	/* This method converts a JSONObject to a Map<String,?> while recursively
	 * converting the values to JSONObject or JSONArray as needed. */
	public static Map<String, ?> JSONObjectToMap(JSONObject json)
			throws JSONException {
		if (json == null)
			return null;
		String[] keys = getNames(json);
		if (keys != null) {
			Map<String, Object> map = new HashMap<String, Object>();
			for (String key : keys)
				if (json.get(key) instanceof JSONObject)
					map.put(key, JSONObjectToMap(json.getJSONObject(key)));
				else if (json.get(key) instanceof JSONArray)
					map.put(key, JSONArrayToList(json.getJSONArray(key)));

				else
					// primitive type object
					map.put(key, json.get(key));
			return map;
		}
		return null;
	}

	public static InetSocketAddress getOffsettedAddress(InetSocketAddress isa, int offset) {
		return offset==0 ? isa : new InetSocketAddress(isa.getAddress(), isa
				.getPort()+offset);
	}

}
