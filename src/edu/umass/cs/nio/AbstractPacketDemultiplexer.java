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
package edu.umass.cs.nio;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.utils.Stringer;
import edu.umass.cs.utils.Summarizable;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * @param <MessageType>
 *            Indicates the generic type of messages processed by this
 *            demultiplexer.
 */
public abstract class AbstractPacketDemultiplexer<MessageType> implements
		PacketDemultiplexer<MessageType> {

	/**
	 * The default thread pool size.
	 * 
	 * FIXME: Unclear what a good value is.
	 */
	public static final int DEFAULT_THREAD_POOL_SIZE = 5;
	private static int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

	private final int myThreadPoolSize;

	private static boolean emulateDelays = false;

	/**
	 * @param threadPoolSize
	 *            The threadPoolSize parameter determines the level of
	 *            parallelism in NIO packet processing. Setting it to 0 means no
	 *            parallelism, i.e., each received packet will be fully
	 *            processed by NIO before it does anything else. So if the
	 *            {@link #handleMessage(Object,NIOHeader)} implementation
	 *            blocks, NIO may deadlock.
	 * 
	 *            Setting the threadPoolSize higher allows
	 *            {@link #handleMessage(Object,NIOHeader)} to include a limited
	 *            number of blocking operations, but NIO can still deadlock if
	 *            the number of pending {@link #handleMessage(Object,NIOHeader)}
	 *            invocations at a node exceeds the thread pool size in this
	 *            class. Thus, it is best for
	 *            {@link #handleMessage(Object,NIOHeader)} methods to only
	 *            perform operations that return quickly; if longer packet
	 *            processing is needed, {@link #handleMessage(Object,NIOHeader)}
	 *            must accordingly spawn its own helper threads. It is a bad
	 *            idea, for example, for
	 *            {@link #handleMessage(Object,NIOHeader)} to itself send a
	 *            request over the network and wait until it gets back a
	 *            response.
	 */
	public static synchronized void setThreadPoolSize(int threadPoolSize) {
		AbstractPacketDemultiplexer.threadPoolSize = threadPoolSize;
	}

	protected static synchronized int getThreadPoolSize() {
		return threadPoolSize;
	}

	private final ScheduledThreadPoolExecutor executor;
	private final HashMap<Integer, PacketDemultiplexer<MessageType>> demuxMap = new HashMap<Integer, PacketDemultiplexer<MessageType>>();
	private final Set<Integer> orderPreservingTypes = new HashSet<Integer>();
	protected static final Logger log = NIOTransport.getLogger();

	abstract protected Integer getPacketType(MessageType message);

	abstract protected MessageType processHeader(byte[] message,
			NIOHeader header);

	abstract protected boolean matchesType(Object message);

	private static final String DEFAULT_THREAD_NAME = AbstractPacketDemultiplexer.class
			.getSimpleName();
	private String threadName = DEFAULT_THREAD_NAME;

	/**
	 * 
	 * @param threadPoolSize
	 *            Refer documentation for {@link #setThreadPoolSize(int)
	 *            setThreadPoolsize(int)}.
	 */
	public AbstractPacketDemultiplexer(int threadPoolSize) {
		this.executor = (ScheduledThreadPoolExecutor) Executors
				.newScheduledThreadPool(threadPoolSize, new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread thread = Executors.defaultThreadFactory()
								.newThread(r);
						thread.setName(threadName);
						return thread;
					}
				});
		this.myThreadPoolSize = threadPoolSize;
	}

	/**
	 * Sets the number of packet demultiplexing threads.
	 * 
	 * @param corePoolSize
	 * @return {@code this}
	 */
	public AbstractPacketDemultiplexer<MessageType> setNumDemultiplexerThreads(
			int corePoolSize) {
		this.executor.setCorePoolSize(corePoolSize);
		return this;
	}

	/**
	 * 
	 */
	public AbstractPacketDemultiplexer() {
		this(getThreadPoolSize());
	}

	protected AbstractPacketDemultiplexer<MessageType> setThreadName(String name) {
		this.threadName = DEFAULT_THREAD_NAME + "[" + myThreadPoolSize + "]"
				+ (name != null ? ":" + name : "");
		return this;
	}

	public String toString() {
		return this.threadName;
	}

	// This method will be invoked by NIO
	protected boolean handleMessageSuper(byte[] msg, NIOHeader header)
			throws JSONException {
		NIOInstrumenter.incrRcvd();
		MessageType message = null;
		Level level = Level.FINEST;
		try {
			message = processHeader(msg, header);
		} catch (Exception | Error e) {
			e.printStackTrace();
			return false;
		}
		Integer type = message != null ? getPacketType(message) : null;
		log.log(level,
				"{0} handling type {1} message {2}:{3}",
				new Object[] {
						this,
						type,
						header,
						log.isLoggable(level) ? (message instanceof Summarizable ? ((Summarizable) message)
								.getSummary(log.isLoggable(level)) : Util
								.truncate(new Stringer(msg), 32, 32))
								: msg });

		if (type == null || !this.demuxMap.containsKey(type)) {
			/* It is natural for some demultiplexers to not handle some packet
			 * types, so it is not a "bad" thing that requires a warning log. */
			log.log(level,
					"{0} ignoring unknown packet type: {1}: {2}",
					new Object[] {
							this,
							type,
							(message instanceof Summarizable ? ((Summarizable) message)
									.getSummary(log.isLoggable(level))
									: (message instanceof Summarizable ? ((Summarizable) message)
											.getSummary(log.isLoggable(level))
											: Util.truncate(new Stringer(msg),
													32, 32))) });
			return false;
		}
		Tasker tasker = new Tasker(message, this.demuxMap.get(type), header);
		if (this.myThreadPoolSize == 0 || isOrderPreserving(message)) {
			log.log(Level.FINER,
					"{0} handling message type {1} in selector thread; this can cause "
							+ "deadlocks if the handler involves blocking operations",
					new Object[] { this, type });
			// task better be lightning quick
			tasker.run();
		} else
			try {
				log.log(level = Level.FINER,
						"{0} invoking {1}.handleMessage({2})",
						new Object[] {
								this,
								tasker.pd,
								(message instanceof Summarizable ? ((Summarizable) message)
										.getSummary(log.isLoggable(level))
										: Util.truncate(message, 32, 32)) });
				// task should still be non-blocking
				executor.schedule(tasker,
						emulateDelays ? JSONDelayEmulator.getEmulatedDelay()
								: 0, TimeUnit.MILLISECONDS);
			} catch (RejectedExecutionException ree) {
				if (!executor.isShutdown())
					ree.printStackTrace();
				return false;
			}
		/* Note: executor.submit() consistently yields poorer performance than
		 * scheduling at 0 as above even though they are equivalent. Probably
		 * garbage collection or heap optimization issues. */
		return true;
	}

	/**
	 * Turns on delay emulation. There is no way to disable, so use with care.
	 */
	public static final void emulateDelays() {
		emulateDelays = true;
	}

	protected boolean loopback(Object obj) {
		if (!this.matchesType(obj))
			return false;
		@SuppressWarnings("unchecked")
		// checked above
		MessageType message = (MessageType) obj;
		Integer type = getPacketType(message);
		if (type == null || !this.demuxMap.containsKey(type))
			this.demuxMap.get(type).handleMessage(message, null);
		return true;
	}

	/**
	 * @param msg
	 * @return True if message order is preserved.
	 */
	public boolean isOrderPreserving(MessageType msg) {
		return false;
	}

	/**
	 * Registers {@code type} with {@code this}.
	 * 
	 * @param type
	 */
	public void register(IntegerPacketType type) {
		register(type, this);
	}

	/**
	 * Registers {@code type} with {@code pd}.
	 * 
	 * @param type
	 * @param pd
	 */
	public void register(IntegerPacketType type,
			PacketDemultiplexer<MessageType> pd) {
		if (pd == null)
			return;
		if (this.demuxMap.containsKey(type))
			throw new RuntimeException("re-registering type " + type);
		log.log(Level.FINE, "{0} registering type {1}:{2} {3}", new Object[] {
				this, type, type.getInt(), (this != pd ? "with " + pd : "") });
		this.demuxMap.put(type.getInt(), pd);
	}

	/**
	 * @return True if congested
	 */
	protected boolean isCongested(NIOHeader header) {
		return false;
	}

	/**
	 * Registers {@code types} with {@code pd};
	 * 
	 * @param types
	 * @param pd
	 */
	public void register(Set<IntegerPacketType> types,
			PacketDemultiplexer<MessageType> pd) {
		log.log(Level.INFO, "{0} registering types {1} for {2}", new Object[] {
				this, types, pd });
		for (IntegerPacketType type : types)
			register(type, pd);
	}

	/**
	 * Registers {@code types} with {@code this}.
	 * 
	 * @param types
	 */
	public void register(Set<IntegerPacketType> types) {
		this.register(types, this);
	}

	/**
	 * Registers {@code types} with {@code this}.
	 * 
	 * @param types
	 * @param pd
	 */
	public void register(IntegerPacketType[] types,
			PacketDemultiplexer<MessageType> pd) {
		log.info(pd + " registering types "
				+ (new HashSet<Object>(Arrays.asList(types))));
		for (Object type : types)
			register((IntegerPacketType) type, pd);
	}

	/**
	 * Registers {@code types} with {@code this}.
	 * 
	 * @param types
	 */
	public void register(IntegerPacketType[] types) {
		log.log(Level.INFO, "{0} registering types "
				+ (new HashSet<Object>(Arrays.asList(types))) + " for " + this);
		for (Object type : types)
			register((IntegerPacketType) type, this);
	}

	/**
	 * @param type
	 */
	public void registerOrderPreserving(IntegerPacketType type) {
		register(type);
		this.orderPreservingTypes.add(type.getInt());
	}

	/**
	 * Any created instance of AbstractPacketDemultiplexer or its inheritors
	 * must be cleanly closed by invoking this stop method.
	 */
	public void stop() {
		this.executor.shutdown();
	}

	// helper task for handleMessageSuper
	protected class Tasker implements Runnable {

		private final MessageType json;
		private final PacketDemultiplexer<MessageType> pd;
		private final edu.umass.cs.nio.nioutils.NIOHeader header;

		Tasker(MessageType json, PacketDemultiplexer<MessageType> pd,
				edu.umass.cs.nio.nioutils.NIOHeader header) {
			this.json = json;
			this.pd = pd;
			this.header = header;
		}

		public void run() {
			long t = 0;
			try {
				if (NIOInstrumenter.monitorHandleMessageEnabled())
					t = insert(this.json);
				pd.handleMessage(this.json, header);
			} catch (RejectedExecutionException ree) {
				if (!executor.isShutdown())
					ree.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace(); // unless printed task will die silently
			} catch (Error e) {
				e.printStackTrace();
			} finally {
				if (NIOInstrumenter.monitorHandleMessageEnabled())
					release(t);
			}
		}
	}

	private static TreeMap<Long, Object> handleMessageStats = new TreeMap<Long, Object>();

	/**
	 * @param threshold
	 * @return Report of message(s) taking over {@code threshold} to be handled.
	 */
	public static String getHandleMessageReport(long threshold) {
		Map.Entry<Long, Object> entry = handleMessageStats.firstEntry();
		if (entry != null
				&& (System.nanoTime() - entry.getKey()) / 1000 / 1000 > threshold)
			return "Message ["
					+ Util.truncate(entry.getValue().toString(), 64, 64)
					+ " has not been handled within " + threshold / 1000
					+ " seconds; total=" + handleMessageStats.size();
		return null;
	}

	private long insert(Object msg) {
		synchronized (handleMessageStats) {
			long t = System.nanoTime();
			handleMessageStats.put(t, msg);
			return t;
		}
	}

	private synchronized void release(long t) {
		synchronized (handleMessageStats) {
			handleMessageStats.remove(t);
		}
	}

}
