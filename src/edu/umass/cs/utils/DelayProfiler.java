package edu.umass.cs.utils;

import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author V. Arun
 */
public class DelayProfiler {
	private static ConcurrentHashMap<String, Double> averageMillis = new ConcurrentHashMap<String, Double>();
	private static ConcurrentHashMap<String, Double> averageNanos = new ConcurrentHashMap<String, Double>();
	private static ConcurrentHashMap<String, Double> averages = new ConcurrentHashMap<String, Double>();
	private static ConcurrentHashMap<String, Double> stdDevs = new ConcurrentHashMap<String, Double>();
	private static ConcurrentHashMap<String, Double> counters = new ConcurrentHashMap<String, Double>();
	private static ConcurrentHashMap<String, Double> instarates = new ConcurrentHashMap<String, Double>();

	private static ConcurrentHashMap<String, Double> lastArrivalNanos = new ConcurrentHashMap<String, Double>();

	private static ConcurrentHashMap<String, Double> lastRecordedNanos = new ConcurrentHashMap<String, Double>();
	private static ConcurrentHashMap<String, Double> lastCount = new ConcurrentHashMap<String, Double>();

	private static boolean enabled = true;
	
	/**
	 * Will disable instrumentation (true by default).
	 */
	public static void disable() {
		enabled = false;
	}

	/**
	 * @param field
	 * @param map
	 * @return True if inserted.
	 */
	public static boolean register(String field,
			ConcurrentMap<String, Double> map) {
		if(!enabled) return false;
		synchronized (map) {
			if (map.containsKey(field))
				return true;
			if (map == lastRecordedNanos)
				map.put(field, (double) System.nanoTime());
			else
				//map.put(field, 0.0)
				;
		}
		synchronized (stdDevs) {
			stdDevs.put(field, 0.0);
		}
		return false;
	}

	/**
	 * @param field
	 * @param time
	 * @param alpha
	 */
	public static void updateDelay(String field, double time, double alpha) {
		if(!enabled) return;
		double delay;
		long endTime = System.currentTimeMillis();
		synchronized (averageMillis) {
			register(field, averageMillis); // register if not registered
			averageMillis.putIfAbsent(field, endTime - time);
			delay = averageMillis.get(field);
			delay = Util.movingAverage(endTime - time, delay, alpha);
			averageMillis.put(field, delay);
		}
		synchronized (stdDevs) {
			// update deviation
			double dev = stdDevs.get(field);
			dev = Util.movingAverage(endTime - time - delay, dev, alpha);
			stdDevs.put(field, dev);
		}
	}

	/**
	 * @param field
	 * @param time
	 */
	public static void updateDelay(String field, double time) {
		updateDelay(field, time, Util.ALPHA);
	}

	/**
	 * @param field
	 * @param time
	 */
	public static void updateDelayNano(String field, double time) {
		if(!enabled) return;
		double delay;
		long endTime = System.nanoTime();
		synchronized (averageNanos) {
			register(field, averageNanos); // register if not registered
			averageNanos.putIfAbsent(field, endTime - time);
			delay = averageNanos.get(field);
			delay = Util.movingAverage(endTime - time, delay);
			averageNanos.put(field, delay);
		}
		synchronized (stdDevs) {
			// update deviation
			Double dev = stdDevs.get(field);
			dev = (dev!=null ? Util.movingAverage(endTime - time - delay, dev) : 0.0);
			stdDevs.put(field, dev!=null ? dev : 0.0);
		}
	}

	/**
	 * @param field
	 * @param time
	 * @param n
	 */
	public static void updateDelayNano(String field, long time, int n) {
		if(!enabled) return;
		for (int i = 0; i < n; i++)
			updateDelayNano(field, System.nanoTime()
					- (System.nanoTime() - time) * 1.0 / n);
	}

	/**
	 * @param field
	 * @param time
	 * @param n
	 */
	public static void updateDelay(String field, long time, int n) {
		if(!enabled) return;
		for (int i = 0; i < n; i++)
			updateDelay(field,
					System.currentTimeMillis()
							- (System.currentTimeMillis() - time) * 1.0 / n);
	}

	/**
	 * @param field
	 * @return The delay.
	 */
	public static double get(String field) {
		synchronized (averageMillis) {
			return averageMillis.containsKey(field) ? averageMillis.get(field)
					: averageNanos.containsKey(field) ? averageNanos.get(field)
							: averages.containsKey(field) ? averages.get(field)
									: counters.containsKey(field) ? counters
											.get(field) : instarates
											.containsKey(field) ? instarates
											.get(field) : 0.0;
		}
	}

	/**
	 * @param field
	 * @param sample
	 */
	public static void updateMovAvg(String field, double sample) {
		updateMovAvg(field, sample, Util.ALPHA);
	}

	/**
	 * @param field
	 * @param sample
	 * @param alpha
	 */
	public static void updateMovAvg(String field, double sample, double alpha) {
		if(!enabled) return;

		double value;
		synchronized (averages) {
			register(field, averages); // register if not registered
			averages.putIfAbsent(field, sample);
			// update value
			value = averages.get(field);
			value = Util.movingAverage(sample, value);
			averages.put(field, value);
		}
		synchronized (stdDevs) {
			// update deviation
			double dev = stdDevs.get(field);
			dev = Util.movingAverage(sample - value, dev);
			stdDevs.put(field, dev);
		}
	}

	/**
	 * @param field
	 * @param incr
	 */
	public static void updateCount(String field, int incr) {
		if(!enabled) return;

		synchronized (counters) {
			register(field, counters);
			counters.putIfAbsent(field, 0D);
			double value = counters.get(field);
			value += incr;
			counters.put(field, value);
		}
	}

	/**
	 * @param field
	 * @param value
	 */
	public static void updateValue(String field, double value) {
		if(!enabled) return;

		synchronized (counters) {
			register(field, counters);
			counters.put(field, value);
		}
	}

	/**
	 * @param field
	 * @param numArrivals
	 * @param samplingFactor
	 */
	public static void updateInterArrivalTime(String field, int numArrivals,
			int samplingFactor) {
		updateInterArrivalTime(field, numArrivals, samplingFactor, Util.ALPHA);
	}

	/**
	 * @param field
	 * @param numArrivals
	 * @param samplingFactor
	 * @param alpha
	 */
	public static void updateInterArrivalTime(String field, int numArrivals,
			int samplingFactor, double alpha) {
		if(!enabled) return;

		if (!Util.oneIn(samplingFactor))
			return;
		synchronized (lastArrivalNanos) {
			register(field, lastArrivalNanos);
			long curTime = System.nanoTime();
			double value = lastArrivalNanos.containsKey(field) ? lastArrivalNanos
					.get(field) : curTime;
			if (value == 0)
				value = curTime;
			DelayProfiler.updateMovAvg(field, (curTime - (long) value)
					/ (numArrivals * samplingFactor));
			lastArrivalNanos.put(field, System.nanoTime() * 1.0);
		}
	}

	/**
	 * @param field
	 * @param numArrivals
	 */
	public static void updateInterArrivalTime(String field, int numArrivals) {
		updateInterArrivalTime(field, numArrivals, 1);
	}

	/**
	 * @param field
	 * @param numArrivals
	 * @param samplingFactor
	 */
	public static void updateRate(String field, int numArrivals,
			int samplingFactor) {
		if(!enabled) return;

		if (!Util.oneIn(samplingFactor))
			return;
		register(field, lastCount);
		register(field, lastRecordedNanos);
		register(field, instarates);
		synchronized (lastCount) {
			double count = lastCount.get(field) + samplingFactor;
			if (count == numArrivals) {
				instarates.put(field, numArrivals * 1000 * 1000 * 1000.0
						/ (System.nanoTime() - lastRecordedNanos.get(field)));
				lastCount.put(field, (double) 0);
				lastRecordedNanos.put(field, (double) System.nanoTime());
			} else
				lastCount.put(field, count);
		}
	}

	/**
	 * @param field
	 * @param numArrivals
	 */
	public static void updateRate(String field, int numArrivals) {
		updateRate(field, numArrivals, 1);
	}

	/**
	 * @param field
	 * @return Throughput calculated from interarrival time.
	 */
	public static double getThroughput(String field) {
		return averages.containsKey(field) && averages.get(field) > 0 ? 1000 * 1000 * 1000.0 / (averages
				.get(field)) : 0;
	}

	/**
	 * @param field
	 * @return Moving average of instantaneous rate.
	 */
	public static double getRate(String field) {
		return instarates.containsKey(field) ? (instarates.get(field)) : 0;
	}

	/**
	 * @return Statistics as a string.
	 */
	public static String getStats() {
		return getStats(null);
	}
	
	/**
	 * @return Statistics as object for logging.
	 */
	public static Object getStatsLog() {
		return new Object() {
			public String toString() {
				return getStats(null);
			}
		};
	}

	/**
	 * @param fields
	 * @return Statistics as a string for fields in {@code fields}.
	 */
	public static String getStats(Set<String> fields) {
		String s = "[ ";
		s += statsHelper(averageMillis, "ms", fields);
		s += statsHelper(averageNanos, "ns", fields);
		s += statsHelper(averages, "", fields);
		s += statsHelper(counters, "", fields);
		s += statsHelper(instarates, "/s", fields);

		return (s + "]").replace(" | ]", " ]");
	}

	private static String statsHelper(ConcurrentMap<String, Double> map,
			String units, Set<String> fields) {
		String s = "";
		TreeMap<String, Double> treeMap = new TreeMap<String, Double>(map);
		synchronized (treeMap) {
			for (String field : treeMap.keySet()) {
				if (fields != null && !fields.contains(field))
					continue;
				boolean rateParam = lastArrivalNanos.containsKey(field);
				s += (field
						+ ":"
						+ (!rateParam ? Util.df(units.equals("ns") ? treeMap
								.get(field) / 1000.0 : treeMap.get(field))
								: Util.df(getThroughput(field)))
						+ "/"
						+ (stdDevs.get(field) >= 0 ? "+" : "")
						+ (!rateParam ? Util.df(units.equals("ns") ? stdDevs
								.get(field) / 1000.0 : treeMap.get(field))
								: Util.df(1000 * 1000 * 1000.0 / stdDevs
										.get(field)))
						+ (!rateParam ? units.equals("ns") ? "us" : units
								: "/s") + " | ");
			}
		}
		return s;
	}

	/**
	 * 
	 */
	public static void clear() {
		averageMillis.clear();
		averageNanos.clear();
		averages.clear();
		stdDevs.clear();
		counters.clear();
		instarates.clear();
		lastArrivalNanos.clear();
		lastRecordedNanos.clear();
		lastCount.clear();
	}
}