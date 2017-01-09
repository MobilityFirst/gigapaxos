package edu.umass.cs.utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.paxosutil.LogIndex;

/**
 * Test class for {@link DiskMap}.
 */
public class DiskMapTest extends DefaultTest {
	private static class KeyableString extends LogIndex implements
			Keyable<String> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		final String key;

		// final String value;

		KeyableString(String k) {
			super(k, 0);
			this.key = k;
			// this.value = v;
		}

		@Override
		public String getKey() {
			return key;
		}
	}

	private static DiskMap<String, KeyableString> makeTestMap() {
		int dbSize = 4000 * 1000;
		ConcurrentHashMap<String, KeyableString> db = new ConcurrentHashMap<String, KeyableString>(
				dbSize);
		boolean sleep = false;
		int capacity = 1000000;
		DiskMap<String, KeyableString> dmap = new DiskMap<String, KeyableString>(
				capacity) {

			@Override
			public Set<String> commit(Map<String, KeyableString> toCommit)
					throws IOException {
				try {
					if (sleep)
						Thread.sleep(toCommit.size());
					for (String key : toCommit.keySet()) {
						if (toCommit.get(key) == null) {
							db.remove(key);
							toCommit.remove(key);
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				db.putAll(toCommit);
				return new HashSet<String>(toCommit.keySet());
			}

			@Override
			public KeyableString restore(String key) throws IOException {
				try {
					if (sleep)
						Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return db.get(key);
			}
		};
		return dmap;
	}

	/**
	 * 
	 */
	@Test
	public void test01_Remove() {
		DiskMap<String, KeyableString> dmap = makeTestMap();
		String key1 = "key1";
		KeyableString val1 = new KeyableString("value1");
		dmap.put(key1, val1);
		Assert.assertEquals(dmap.get("key1"), val1);
		dmap.remove(key1);
		Assert.assertTrue(!dmap.containsKey(key1));
	}

	/**
	 * 
	 */
	@Test
	public void test99_Main() {
		int dbSize = 4000 * 1000;
		ConcurrentHashMap<String, KeyableString> db = new ConcurrentHashMap<String, KeyableString>(
				dbSize);
		MultiArrayMap<String, KeyableString> gt = new MultiArrayMap<String, KeyableString>(
				dbSize);
		boolean sleep = false;
		int capacity = 1000000;
		DiskMap<String, KeyableString> dmap = new DiskMap<String, KeyableString>(
				capacity) {

			@Override
			public Set<String> commit(Map<String, KeyableString> toCommit)
					throws IOException {
				try {
					if (sleep)
						Thread.sleep(toCommit.size());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				db.putAll(toCommit);
				return new HashSet<String>(toCommit.keySet());
			}

			@Override
			public KeyableString restore(String key) throws IOException {
				try {
					if (sleep)
						Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return db.get(key);
			}
		};
		long t = System.currentTimeMillis(), t1 = t;
		System.out.println("Starting test with dbSize=" + dbSize);
		int count = 0, resetCount = 0;
		int freq = 500000;
		int numRequests = 10000000;
		while (count < numRequests) {
			int i = count % dbSize;// (int) (Math.random() * (dbSize));
			count++;
			resetCount++;
			if (count % freq == 0) {
				System.out.println("after "
						+ count
						+ ": put/get rate = "
						+ Util.df(resetCount * 1000.0
								/ (System.currentTimeMillis() - t1))
						+ DelayProfiler.getStats() + "; in-memory-size = "
						+ dmap.size() + ": dbSize = " + db.size()
						+ "; gtSize = " + gt.size());
				t1 = System.currentTimeMillis();
				resetCount = 0;
			}

			if (Math.random() < 0.5) {
				KeyableString ks = new KeyableString(i + "");
				dmap.put(i + "", ks);
				ks.add(i, 2, 102, 3, "file" + i, 123, 4567);
				gt.put(i + "", ks);
			} else {
				KeyableString retrieved = dmap.get(i + "");
				KeyableString real = gt.get(i + "");
				assert ((retrieved == null && real == null) || (retrieved != null
						&& real != null && retrieved.equals(real))) : retrieved
						+ " != " + real + " at count=" + count;
			}
			assert (dmap.size() <= capacity) : dmap.size();
		}
		dmap.close();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		Result result = JUnitCore.runClasses(DiskMapTest.class);
		for (Failure failure : result.getFailures())
			System.out.println(failure.toString());
	}
}
