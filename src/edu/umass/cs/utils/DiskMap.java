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
 * the License.
 * 
 * Initial developer(s): V. Arun */

package edu.umass.cs.utils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * @author arun
 * 
 * @param <K>
 * @param <V>
 *
 *            DiskMap implements the ConcurrentMap<K,V> interface and allows
 *            applications to maintain very large maps limited only by the
 *            available disk space by automatically stowing away infrequently
 *            used map entries to disk. DiskMap is not a database and does not
 *            try to guarantee durability under crashes. However, if the map is
 *            closed gracefully, it will ensure durability by committing entries
 *            to disk before closing.
 * 
 *            DiskMap itself does not implement any disk I/O but relies on the
 *            application that must implement {@link #commit(Map)} and
 *            {@link #restore(Object)} methods that respectively write to and
 *            read from the disk.
 * 
 *            DiskMap is nearly indistinguishable in performance from
 *            ConcurrentHashMap when all map entries fit in memory. If not, its
 *            performance depends on how frequently entries are reused. If
 *            entries are rarely reused, e.g., a sweep over a very large number
 *            of map entries, then every access will on average force a disk
 *            read (to unpause the entry being accessed) and a write (to pause
 *            long idle entries). Normally, only entries that have been idle for
 *            at least {@code idleThreshold} paused to disk, so the caller must
 *            use {@link #setIdleThreshold(long)} so as to ensure that either no
 *            more than {@code sizeThreshold} entries are accessed in that
 *            interval or there is sufficient memory to hold all entries
 *            accessed in the last {@code idleThreshold} time. If not, a random
 *            fraction of entries, including possibly recently used entries,
 *            will be paused to disk in order to make room in memory.
 * 
 *            DiskMap can use MultiArrayMap as its underlying in-memory map if
 *            that is supplied in the constructor. Although not necessary, the
 *            use of MultiArrayMap makes the map more compact because of its
 *            cuckoo hashing design, but it requires values to implement the
 *            Keyable<K> interface. If values additionally also implement
 *            {@link Pausable}, DiskMap incurs just ~6B overhead per value as it
 *            does not have to maintain its own stats for last active times for
 *            map entries.
 * 
 *            Note: There is no way to ensure that the value pointed to in the
 *            map is not modified after it has been paused to disk. It does not
 *            help to verify that the value pointed to is the same as the
 *            serialized value paused to disk as the value pointed to in the map
 *            could be modified by a caller even after its serialized form has
 *            been paused to disk. As a result, we can not support the
 *            "expectation" that modifications to the value object by the caller
 *            will be reflected in the map. So, the caller is forced to reckon
 *            with the possibility that any modifications to the value object
 *            may be lost unless the caller explicitly invokes a put
 *            subsequently.
 */

public abstract class DiskMap<K, V> implements ConcurrentMap<K, V>,
		Diskable<K, V> {

	/**
	 * 
	 */
	public static final int DEFAULT_CAPACITY = 1024 * 64;

	private final Map<K, V> map;

	private final ConcurrentMap<K, V> pauseQ = new ConcurrentHashMap<K, V>();

	/**
	 * This map could either be a MultiArrayMap or LinkedHashMap depending on
	 * whether {@link #enablePriorityQueue()} is enabled.
	 */
	private Map<Object, LastActive> stats;

	private final ScheduledExecutorService GC = Executors
			.newScheduledThreadPool(1, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread thread = Executors.defaultThreadFactory().newThread(
							r);
					thread.setName(DiskMap.class.getSimpleName() + "-GC");
					return thread;
				}
			});

	private final long capacityEstimate;
	private long idleThreshold = 30000;
	private long pauseThreadPeriod = 30000;
	private long lastGCAttempt = 0;
	private boolean ongoingGC = false;
	
	/**
	 * True means that the in-memory map will always be consistent with the
	 * underlying database, which is achieved by always committing an entry
	 * upon a put.
	 */
	private boolean cleanCache = false;

	/**
	 * Minimum idle time in order to be pausable.
	 * 
	 * @param idleTime
	 */
	public void setIdleThreshold(long idleTime) {
		this.idleThreshold = idleTime;
	}
	
	/**
	 * Set the cleanCache option to true, which means that puts will be
	 * immediately committed, i.e., the in-memory cache is always consistent
	 * with the disk.
	 * 
	 * @param set
	 */
	public synchronized void setCleanCache(boolean set) {
		this.cleanCache = set;
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public synchronized void enablePriorityQueue() {
		if (isSingleLinkedHashMap())
			return;
		if (this.stats instanceof LinkedHashMap)
			return;
		LinkedHashMap<Object, LastActive> tmpStats = new LinkedHashMap<Object, LastActive>(
				16, (float) 0.75, true);
		if (this.stats != null)
			for (Iterator<LastActive> iterLA = ((MultiArrayMap) this.stats)
					.concurrentIterator(); iterLA.hasNext();) {
				LastActive la = iterLA.next();
				tmpStats.put(la.key, la);
			}
		this.stats = tmpStats;
	}

	/**
	 * Period after which a pausing attempt is made by the GC thread.
	 * 
	 * @param period
	 */
	public void setPauseThreadPeriod(long period) {
		this.pauseThreadPeriod = period;
	}

	/**
	 *
	 */
	public class LastActive implements Keyable<Object> {

		final Object key;
		long lastActive = System.currentTimeMillis();

		LastActive(Object key) {
			this.key = key;
		}

		@Override
		public Object getKey() {
			return this.key;
		}

		LastActive justActive() {
			this.lastActive = System.currentTimeMillis();
			return this;
		}
	}

	private void initPeriodicGC() {
		this.GC.scheduleWithFixedDelay(new TimerTask() {
			@Override
			public void run() {
				DiskMap.this.GC(false);
			}
		}, 0, pauseThreadPeriod, TimeUnit.MILLISECONDS);
	}

	private void initOnetimeGC() {
		DiskMap.this.GC(true);
	}

	private static final boolean USE_LINKED_HASH_MAP = true;

	/**
	 * @param inMemoryCapacity
	 *            Capacity for in-memory map. If the in-memory map size reaches
	 *            this value, GC forcibly kicks in.
	 */
	public DiskMap(long inMemoryCapacity) {
		this.map = USE_LINKED_HASH_MAP ? Collections
				.synchronizedMap(new LinkedHashMap<K, V>(16, (float) 0.75, true))
				: new ConcurrentHashMap<K, V>();
		this.externalMap = false;
		this.capacityEstimate = inMemoryCapacity;
		this.initPeriodicGC();
	}

	private final boolean externalMap;

	/**
	 * The supplied {@code map} will be used as the underlying in-memory map.
	 * From this point onwards, making direct modifications to this underlying
	 * map is unsafe.
	 * 
	 * @param map
	 */
	public DiskMap(ConcurrentMap<K, V> map) {
		this.map = map;
		this.externalMap = true;
		this.capacityEstimate = map instanceof MultiArrayMap ? ((MultiArrayMap<?, ?>) map)
				.capacity() : DEFAULT_CAPACITY;
		this.initPeriodicGC();
	}

	private void initStats() {
		if (isSingleLinkedHashMap())
			return;
		this.enablePriorityQueue();
	}

	/**
	 * This method should return only after successfully persisting the
	 * key,value pair, otherwise it should throw an exception.
	 * 
	 * @throws IOException
	 */
	abstract public Set<K> commit(Map<K, V> toCommit) throws IOException;

	/**
	 * @param key
	 * @return Value for key restored from persistent store.
	 * @throws IOException
	 */
	abstract public V restore(K key) throws IOException;

	@Override
	public int size() {
		return this.map.size();
	}

	@Override
	public boolean isEmpty() {
		return this.map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		// need to check disk here
		return this.map.containsKey(key) || this.get(key) != null;
	}

	@Override
	public boolean containsValue(Object value) {
		if (!(value instanceof Keyable<?>))
			throw new RuntimeException(
					"containsValue(value) can only be invoked on a value of type Keyable<?>");
		return (this.map.containsValue(value) || this.get(
				((Keyable<?>) value).getKey()).equals(value));
	}

	@Override
	public V get(Object key) {
		V value = this.map.get(key);
		if (value == null)
			value = this.getOrRestore(key);
		if (!(value instanceof Pausable))
			this.markActive(key);
		if (this.shouldGC(true))
			this.initOnetimeGC();
		return value;
	}

	private boolean isSingleLinkedHashMap() {
		return USE_LINKED_HASH_MAP && !this.externalMap;
	}

	/* No synchronization needed here because concurrent insertions are not
	 * fatal as stats only affects GC-related performance. */
	private void markActive(Object key) {
		if (isSingleLinkedHashMap())
			return;
		if (this.stats == null)
			this.initStats();
		LastActive la = this.stats.get(key);
		if (la == null) {
			la = new LastActive(key);
		} else {
			la.justActive();
		}
		this.stats.put(key, la);
	}

	@Override
	public V put(K key, V value) {
		if (!(value instanceof Pausable))
			this.markActive(key);
		if (this.shouldGC(true))
			this.initOnetimeGC();
		assert (value != null) : key;
		V prev = null;
		// all mods need to be synchronized
		synchronized (this) {
			prev = this.getOrRestore(key);
			this.map.put(key, value);
			recordPut(key);
			if (this.cleanCache)
				this.putCommit(key, value);
		}
		return prev;
	}
	
	private void putCommit(K key, V value) {
		Set<K> committed = null;
		try {
			committed = this.commit(key);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (committed == null || !committed.contains(key)
					|| committed.size() != 1)
				throw new RuntimeException("Failed to commit <" + key + ", "
						+ value + ">" + "to disk");
		}
	}

	boolean recordPuts = false, recordRemoves = false;
	private Set<K> concurrentPuts = new HashSet<K>();
	private Set<Object> concurrentRemoves = new HashSet<Object>();

	private synchronized void recordPut(K key) {
		if (recordingPuts()) {
			concurrentPuts.add(key);
		}
	}

	private synchronized void recordRemove(Object key) {
		if (recordingRemoves()) {
			concurrentRemoves.add(key);
		}
	}

	private synchronized void startRecordingPuts() {
		recordPuts = true;
	}

	private synchronized void startRecordingRemoves() {
		recordRemoves = true;
	}

	private synchronized void stopRecordingPuts() {
		recordPuts = false;
		this.concurrentPuts.clear();
	}

	private synchronized void stopRecordingRemoves() {
		recordRemoves = false;
		this.concurrentRemoves.clear();
	}

	private synchronized boolean recordingPuts() {
		return recordPuts;
	}

	private synchronized boolean recordingRemoves() {
		return recordRemoves;
	}

	@Override
	public synchronized V remove(Object key) {
		// will try to get from disk if needed
		if (!this.containsKey(key))
			return null;

		if (this.stats != null)
			this.stats.remove(key);
		Map<K, V> entryAsMap = this.removeEntry(key);
		V value = null;
		if (entryAsMap != null && !entryAsMap.isEmpty()) {
			Map.Entry<K, V> entry = entryAsMap.entrySet().iterator().next();
			if (!this.isGCEnabled())
				return entry.getValue();
			value = entry.getValue();
			entryAsMap.put(entry.getKey(), null);
			try {
				this.commit(entryAsMap);
			} catch (IOException e) {
				// any better option here?
				throw new RuntimeException(e.getMessage());
			}
		}
		// need to remove soft copies *after* commit
		this.map.remove(key);
		this.pauseQ.remove(key);
		this.recordRemove(key);
		return value;
	}

	/* There is seemingly no easy way to get the key of type K from the supplied
	 * key of type Object. So this method has to iterate over all entries to get
	 * the key in the map. If it is a MultiArrayMap, we can just get the key
	 * from the value. */
	@SuppressWarnings("unchecked")
	private Map<K, V> removeEntry(Object key) {
		if (this.map.containsKey(key)) {
			HashMap<K, V> entryAsMap = new HashMap<K, V>();
			if (this.map instanceof MultiArrayMap) {
				V value = this.map.remove(key);
				if (value != null)
					entryAsMap.put(((Keyable<K>) value).getKey(), value);
				return entryAsMap;
			} else {
				K removedKey = null;
				V value = null;
				try {
					removedKey = (K) key;
					value = this.map.remove(key);

					// also remove from pauseQ
					V enqueuePausedVal = this.pauseQ.remove(key);
					if (value == null)
						value = enqueuePausedVal;

				} catch (ClassCastException cce) {
					for (Iterator<Map.Entry<K, V>> entryIter = this.map
							.entrySet().iterator(); entryIter.hasNext();) {
						Map.Entry<K, V> entry = entryIter.next();
						if (entry.getKey().equals(key)) {
							removedKey = entry.getKey();
							value = entry.getValue();
							entryIter.remove();
							break;
						}
					}
				}
				if (removedKey != null)
					entryAsMap.put(removedKey, value);
				return entryAsMap;
			}
		}
		return null;
	}

	@Override
	public synchronized void putAll(Map<? extends K, ? extends V> m) {
		for (K key : m.keySet())
			this.map.put(key, m.get(key));
	}

	@Override
	public void clear() {
		if (this.stats != null)
			this.stats.clear();
		this.map.clear();
	}

	/**
	 * This method only returns the in-memory key set. There is currently no
	 * method to get the on-disk keys.
	 */
	@Override
	public Set<K> keySet() {
		return this.map.keySet();
	}

	/**
	 * This method only returns the in-memory value set. There is currently no
	 * method to get the on-disk values.
	 */
	@Override
	public Collection<V> values() {
		return this.map.values();
	}

	/**
	 * This method only returns the in-memory entry set. There is currently no
	 * method to get the on-disk entries.
	 */
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return this.map.entrySet();
	}

	protected synchronized void hintRestore(K key, V value) {
		if (!this.map.containsKey(key) && !this.pauseQ.containsKey(key))
			this.map.put(key, value);
	}
	
	private boolean isGCEnabled() {
		return this.capacityEstimate < Long.MAX_VALUE;
	}

	// get from map or restore from pauseQ or disk
	@SuppressWarnings("unchecked")
	private V getOrRestore(Object key) {
		V value = null;
		if ((value = this.map.get(key)) != null)
			return value;
		else if(!this.isGCEnabled()) return null;
		else if ((value = this.pauseQ.get(key)) != null) {
			// try restore from pauseQ
			try {
				synchronized (this) {
					this.map.put((K) key, value);
					this.pauseQ.remove(key);
				}
			} catch (ClassCastException e) {
				// do nothing
			}
		} else {
			// try restore from disk
			try {
				synchronized (this) {
					if ((value = this.restore((K) key)) != null)
						this.map.put((K) key, value);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassCastException e) {
				// do nothing
			}
		}
		return value;
	}

	private boolean longIdle(LastActive la) {
		return System.currentTimeMillis() - la.lastActive > idleThreshold;
	}

	private static final int FORCE_PAUSE_FACTOR = 20;

	private boolean enqueuePause(K key, V value, LastActive la,
			Iterator<?> iterator) {
		assert (value == null || iterator != null);
		synchronized (this) {
			// came here via stats
			if (value == null)
				value = this.map.get(key);
			if (((value instanceof Pausable) && ((Pausable) value).isPausable())
					// if value is not Pausable, stats must be non-null
					|| ((la != null || (this.stats != null && (la = this.stats
							.get(key)) != null)) && longIdle(la))
					// pause at least one entry
					|| this.pauseQ.size() < Math.max(this.capacityEstimate
							/ FORCE_PAUSE_FACTOR, 1)) {
				if (iterator != null)
					iterator.remove();
				else
					value = this.map.remove(key);
				this.pauseQ.put(key, value);
				return true;
			}
		}
		return false;
	}

	private boolean shouldGC(boolean strict) {
		// avoid unnecessary synchronization
		if(this.ongoingGC) return false;
		
		synchronized (this) {
			if (strict)
				return this.map.size() >= this.capacityEstimate;
			if (this.map.size() > this.capacityEstimate
					&& System.currentTimeMillis() - lastGCAttempt < this.pauseThreadPeriod)
				return true;
			return false;
		}
	}

	/* Pauses long idle entries. This method can be called both by the client or
	 * by the GC thread but allows only one GC to proceed at any time. */
	@SuppressWarnings("unchecked")
	private void GC(boolean strict) {
		synchronized (this) {
			if (!shouldGC(strict))
				return;
			// else
			while (this.ongoingGC)
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			this.lastGCAttempt = System.currentTimeMillis();
			this.ongoingGC = true;
		}
		startRecordingRemoves();
		startRecordingPuts();
		long t = System.currentTimeMillis();
		/* stats is directly traversed only if it is a LinkedHashMap, otherwise
		 * we iterate over the underlying in-memory map and use stats to get
		 * last active times instead. */
		if (this.stats != null && this.stats instanceof LinkedHashMap) {
			while (this.map.size() >= this.capacityEstimate
					&& this.pauseQ.isEmpty()) {
				// need to check for concurrent modification
				try {
					for (Iterator<Map.Entry<Object, LastActive>> iterLA = this.stats
							.entrySet().iterator(); iterLA.hasNext();) {
						Map.Entry<Object, LastActive> entry = iterLA.next();
						if (!this.enqueuePause((K) (entry.getValue().key),
								null, entry.getValue(), null))
							break;
					}
				} catch (ConcurrentModificationException cme) {
					// do nothing, will continue to retry
				}
			}
		}
		/* One and only underlying, in-memory LinkedHashMap, the default and the
		 * cleanest, good enough option. */
		else if (isSingleLinkedHashMap()) {
			while (this.map.size() >= this.capacityEstimate
					&& this.pauseQ.isEmpty()) {
				// need to check for concurrent modification
				try {
					/* Synchronization is needed for correctness here as we can
					 * not rely on a concurrent modification exception to be
					 * thrown. Without synchronization, enqueuePause's
					 * iterator.remove() could remove a newer value from the
					 * in-memory map while pausing an older value to disk. Even
					 * though enqueuePause is synchronized the key-value pairs
					 * are coming from the iterator here, so we either need
					 * guaranteed fail-fast behavior (or at least need the
					 * property that an iterator.remove() will not succeed if
					 * corresponding entry has been modified in any way since
					 * the entry's value was retrieved) or need to explicitly
					 * synchronize the iteration itself like below. */
					synchronized (this) {
						for (Iterator<Map.Entry<K, V>> iterE = this.map
								.entrySet().iterator(); iterE.hasNext();) {
							Map.Entry<K, V> entry = iterE.next();
							if (!this.enqueuePause(entry.getKey(),
									entry.getValue(), null, iterE))
								break;
						}
						// startRecordingPuts();
					}
				} catch (ConcurrentModificationException cme) {
					// do nothing, will continue to retry
				}
			}
		}

		else {
			if (this.map instanceof MultiArrayMap) {
				for (Iterator<Keyable<K>> iterV = (Iterator<Keyable<K>>) (((MultiArrayMap<?, ?>) this.map)
						.concurrentIterator()); iterV.hasNext();) {
					Keyable<K> value = iterV.next();
					this.enqueuePause(value.getKey(), null, null, null);
				}
			} else {
				for (Iterator<Map.Entry<K, V>> iterE = this.map.entrySet()
						.iterator(); iterE.hasNext();) {
					Map.Entry<K, V> entry = iterE.next();
					this.enqueuePause(entry.getKey(), null, null, null);
				}
			}
		}

		if (!this.pauseQ.isEmpty()) {
			Set<K> committed = null;
			try {
				/* This commit can take a long time. While it is happening, it
				 * is possible for puts or removes to happen concurrently. So we
				 * have to explicitly check for such events by recording
				 * concurrent puts or removes. For concurrent puts, we don't
				 * remove the corresponding key from this.map. For concurrent
				 * removes not superceded by a subsequent put, we should
				 * re-remove the key just in case the pause-commit unremoved the
				 * remove from disk. 
				 * 
				 * 
				 * With the cleanCache option, everything in pause can be assumed
				 * to have alredy been committed.
				 * */
				committed = this.cleanCache ? this.pauseQ.keySet() : this
						.commit(this.pauseQ);
			} catch (IOException e) {
				e.printStackTrace();
			}

			// remove paused key,value pairs from the map
			if (committed != null)
				for (K key : committed) {
					synchronized (this) {
						V pausedValue = this.pauseQ.remove(key);
						// concurrent puts should not be removed
						if (!this.concurrentPuts.contains(key))
							// remove in case intervening gets restored key
							this.map.remove(key, pausedValue);

						// concurrent removes should be (re-)removed
						if (this.concurrentRemoves.contains(key)
						// if no subsequent puts happened
								&& !this.map.containsKey(key))
							this.remove(key); // remove again for good measure

						if (this.stats != null)
							this.stats.remove(key);
					}
				}
			stopRecordingPuts();
			stopRecordingRemoves();
			DelayProfiler.updateDelay("GC", t);
		}
		synchronized (this) {
			this.ongoingGC = false;
		}
	}

	@Override
	public synchronized V putIfAbsent(K key, V value) {
		if (!this.containsKey(key))
			return this.put(key, value);
		return null;
	}

	@Override
	public synchronized boolean remove(Object key, Object value) {
		V existing = null;
		if ((existing = this.get(key)) == null || !existing.equals(value))
			return false;
		this.remove(key);
		return true;
	}

	@Override
	public synchronized boolean replace(K key, V oldValue, V newValue) {
		if (!this.get(key).equals(oldValue))
			return false;
		this.put(key, newValue);
		return true;
	}

	@Override
	public synchronized V replace(K key, V value) {
		if (this.containsKey(key))
			return this.put(key, value);
		return null;
	}

	@SuppressWarnings("unchecked")
	private Map<K, V> extractIterableMap(ConcurrentMap<K, V> m, int size) {
		if (m instanceof MultiArrayMap) {
			ConcurrentHashMap<K, V> chmap = new ConcurrentHashMap<K, V>();
			MultiArrayMap<K, ?> mamap = (MultiArrayMap<K, ?>) m;
			int count = 0;
			for (Iterator<?> iter = mamap.concurrentIterator(); iter.hasNext(); count++) {
				Keyable<K> value = (Keyable<K>) iter.next();
				chmap.put(value.getKey(), (V) value);
				if (count == size - 1)
					break;
			}
			return chmap;
		} else
			return m;
	}

	// unused
	protected synchronized Set<K> commit(K key) throws IOException {
		Map<K, V> singleton = new HashMap<K, V>();
		if (this.map.containsKey(key))
			singleton.put(key, this.map.get(key));
		return this.commit(singleton);
	}

	/**
	 * Commits all in-memory entries in the map.
	 */
	public synchronized void commit() {
		this.commitAll(this.map, true);
	}

	public String toString() {
		return this.map.toString();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private synchronized void commitAll(Map<K, V> m, boolean clone) {
		ConcurrentMap<K, V> copy = new ConcurrentHashMap<K, V>();
		if (m instanceof MultiArrayMap) {
			for (Iterator<V> iter = (Iterator<V>) ((MultiArrayMap) m)
					.concurrentIterator(); iter.hasNext();) {
				V value = iter.next();
				if (value != null)
					copy.put(((Keyable<K>) value).getKey(), value);
			}
		} else
			for (Object key : m.keySet().toArray()) {
				V value = m.get(key);
				if (value != null)
					copy.put((K) key, m.get(key));
			}

		while (!copy.isEmpty()) {
			Set<K> committed = null;
			try {
				committed = this.commit(this.extractIterableMap(copy,
						BATCH_SIZE));
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (committed != null)
				for (K key : committed)
					copy.remove(key);
		}
	}

	private static final int BATCH_SIZE = 100;

	/**
	 * Will stop the GC thread but will have no other effect. The map can
	 * continue to be used with the same semantics.
	 * 
	 * @param commitAll
	 */
	public void close(boolean commitAll) {
		this.GC.shutdown();
		if (commitAll)
			this.commitAll(this.map, false);
	}

	/**
	 * 
	 */
	public void close() {
		close(false);
	}

}