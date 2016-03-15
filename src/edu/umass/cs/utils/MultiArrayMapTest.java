package edu.umass.cs.utils;

import java.util.Iterator;

import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.utils.MultiArrayMap.StringValue;

/**
*
*/
public class MultiArrayMapTest extends DefaultTest {

	@SuppressWarnings("unchecked")
	private static void createSimpleArray(int size) {
		StringValue<Integer>[] svarray = null;
		svarray = new StringValue[size]; // SuppressWarnings
		for (int i = 0; i < size; i++) {
			svarray[i] = new StringValue<Integer>("someRandomString" + i,
					i + 23);
		}
	}

	private static int testSum = 0;

	private static MultiArrayMap<String, StringValue<Integer>> createRandomMAM(
			int size) {
		System.out.print("Inserting " + size / (1000 * 1000)
				+ " million values");
		MultiArrayMap<String, StringValue<Integer>> map = new MultiArrayMap<String, StringValue<Integer>>(
				(int) (size), 6);
		assert (map.get("hello") == null);
		map.put("someRandomString0", new StringValue<Integer>(
				"someRandomString0", Integer.MAX_VALUE));
		assert (((StringValue<Integer>) map.get("someRandomString0")).value == Integer.MAX_VALUE);
		for (int i = 0; i < size; i++) {
			String key = "someRandomString" + i;
			int intValue = ((int) (Math.random() * Integer.MAX_VALUE));
			map.put(key, new StringValue<Integer>(key, intValue));
			testSum += intValue;
			assert (((StringValue<Integer>) map.get(key)).value == intValue);
			printProgressBar(i);
		}

		return map;
	}

	private static void printProgressBar(int i) {
		if (i % 200000 == 0)
			System.out.print(".");
	}

	/**
	 * 
	 */
	@Test
	public void testMain() {
		Util.assertAssertionsEnabled();
		int million = 1000000;
		int size = (int) (10 * million);

		boolean simpleArray = false;
		MultiArrayMap<String, StringValue<Integer>> map = null;

		long t1 = System.currentTimeMillis();
		System.out.println("Initiating test...");
		if (simpleArray)
			createSimpleArray(size);
		map = createRandomMAM(size);
		System.out.println("succeeded (cop-out hashmap size = "
				+ map.hashmapSize() + "); time = "
				+ (System.currentTimeMillis() - t1) + "ms");

		System.out.print("Iterating(1)");
		int count = 0;
		int sum = 0;
		t1 = System.currentTimeMillis();
		count = sum = 0;
		for (Iterator<StringValue<Integer>> iter = map.concurrentIterator(); iter
				.hasNext();) {
			printProgressBar(count);
			sum += iter.next().value;
			count++;
		}
		assert (count == map.size() && sum == testSum) : count + " != "
				+ map.size();
		System.out.println("succeeded; time = "
				+ (System.currentTimeMillis() - t1) + "ms");

		System.out.print("Iterating(2)");
		t1 = System.currentTimeMillis();
		count = sum = 0;
		for (Iterator<StringValue<Integer>> iter = map.iterator(); iter
				.hasNext();) {
			printProgressBar(count);
			sum += iter.next().value;
			count++;
		}
		assert (count == map.size() && sum == testSum) : count + " != "
				+ map.size();
		System.out.println("succeeded; time = "
				+ (System.currentTimeMillis() - t1) + "ms");

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		Result result = JUnitCore.runClasses(MultiArrayMapTest.class);
		for (Failure failure : result.getFailures())
			System.out.println(failure.toString());

	}
}
