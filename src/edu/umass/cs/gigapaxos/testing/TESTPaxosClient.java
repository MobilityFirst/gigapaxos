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
package edu.umass.cs.gigapaxos.testing;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.minidev.json.JSONValue;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.MessageExtractor;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 */
public class TESTPaxosClient {
	static {
		//TESTPaxosConfig.load();
	}

	// because the single-threaded sender is a bottleneck on multicore
	private static ScheduledThreadPoolExecutor executor = null;

	private static int totalNoopCount = 0;

	private static int numResponses = 0; // used only for latency
	private static long totalLatency = 0;
	private static double movingAvgLatency = 0;
	private static double movingAvgDeviation = 0;
	private static int numRtxReqs = 0;
	private static int rtxCount = 0;
	private static long lastResponseReceivedTime = System.currentTimeMillis();

	private static synchronized void incrTotalLatency(long ms) {
		totalLatency += ms;
		numResponses++;
	}

	private static synchronized void updateMovingAvgLatency(long ms) {
		movingAvgLatency = Util.movingAverage(ms, movingAvgLatency);
		movingAvgDeviation = Util.movingAverage(ms, movingAvgDeviation);
	}

	private static synchronized void updateLatency(long ms) {
		lastResponseReceivedTime = System.currentTimeMillis();
		incrTotalLatency(ms);
		updateMovingAvgLatency(ms);
	}

	private static synchronized double getTimeout() {
		return Math.max(movingAvgLatency + 4 * movingAvgDeviation,
				TESTPaxosConfig.CLIENT_REQ_RTX_TIMEOUT);
	}

	private static synchronized double getAvgLatency() {
		return totalLatency * 1.0 / numResponses;
	}

	private static synchronized void incrRtxCount() {
		rtxCount++;
	}

	private static synchronized void incrNumRtxReqs() {
		numRtxReqs++;
	}

	private static synchronized int getRtxCount() {
		return rtxCount;
	}

	private static synchronized int getNumRtxReqs() {
		return numRtxReqs;
	}

	protected synchronized static void resetLatencyComputation(
			TESTPaxosClient[] clients) {
		runDone = false;
		totalLatency = 0;
		numResponses = 0;
		for (TESTPaxosClient client : clients)
			client.runReplyCount = 0;
	}

	private MessageNIOTransport<Integer, Object> niot;
	private final NodeConfig<Integer> nc;
	private final int myID;
	private int totReqCount = 0;
	private int totReplyCount = 0;
	private int runReplyCount = 0;
	private int noopCount = 0;

	private final ConcurrentHashMap<Long, RequestAndCreateTime> requests = new ConcurrentHashMap<Long, RequestAndCreateTime>();
	// private final ConcurrentHashMap<Long, Long> requestCreateTimes = new
	// ConcurrentHashMap<Long, Long>();
	private final Timer timer; // for retransmission

	private static Logger log = Logger.getLogger(TESTPaxosClient.class
			.getName());

	// PaxosManager.getLogger();

	private synchronized int incrReplyCount() {
		this.runReplyCount++;
		return this.totReplyCount++;
	}

	private synchronized int incrReqCount() {
		return ++this.totReqCount;
	}

	private synchronized int incrNoopCount() {
		incrTotalNoopCount();
		return this.noopCount++;
	}

	private synchronized static int incrTotalNoopCount() {
		return totalNoopCount++;
	}

	protected synchronized static int getTotalNoopCount() {
		return totalNoopCount;
	}

	private synchronized int getTotalReplyCount() {
		return this.totReplyCount;
	}

	private synchronized int getRunReplyCount() {
		return this.runReplyCount;
	}

	private synchronized int getTotalRequestCount() {
		return this.totReqCount;
	}

	private synchronized int getNoopCount() {
		return this.noopCount;
	}

	synchronized void close() {
		executor.shutdownNow();
		this.timer.cancel();
		this.niot.stop();
	}

	synchronized boolean noOutstanding() {
		return this.requests.isEmpty();
	}

	/******** Start of ClientPacketDemultiplexer ******************/

	private class ClientPacketDemultiplexer extends
			AbstractPacketDemultiplexer<Object> {
		private final TESTPaxosClient client;

		private ClientPacketDemultiplexer(TESTPaxosClient tpc) {
			super(1);
			this.client = tpc;
			this.register(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
			this.setThreadName("" + tpc.myID);
			Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
		}

		public boolean handleMessage(Object msg, edu.umass.cs.nio.nioutils.NIOHeader header) {
			assert (msg instanceof RequestPacket);
			return this.handleMessage((RequestPacket) msg);
		}

		public boolean handleMessage(RequestPacket request) {
			// long t = System.nanoTime();
			try {
				// RequestPacket request = new RequestPacket(msg);
				RequestAndCreateTime sentRequest = requests.remove(request
						.getRequestID());
				if (sentRequest != null) {
					long latency = System.currentTimeMillis()
							- sentRequest.createTime;
					client.incrReplyCount();
					Level level = Level.FINE;
					if (log.isLoggable(level))
						TESTPaxosClient.log
								.log(level,
										"Client {0} received response #{1} with latency {2} [{3}] : {4} {5}",
										new Object[] {
												client.myID,
												client.getTotalReplyCount(),
												latency,
												request.getDebugInfo(log
														.isLoggable(level)),
												request.getSummary(log
														.isLoggable(level)),
												request });

					if (!PROBE_CAPACITY)
						DelayProfiler.updateInterArrivalTime("response_rate",
								1, 100);
					// DelayProfiler.updateRate("response_rate2", 1000, 10);

					updateLatency(latency);
					synchronized (client) {
						client.notify();
					}
				} else {
					Level level = Level.FINE;
					TESTPaxosClient.log
							.log(level,
									"Client {0} received PHANTOM response #{1} [{2}] for request {3} : {4}",
									new Object[] {
											client.myID,
											client.getTotalReplyCount(),
											request.getDebugInfo(log
													.isLoggable(level)),
											request.requestID,
											request.getSummary(log
													.isLoggable(level)) });
				}
				if (request.isNoop())
					client.incrNoopCount();
				// requests.remove(request.requestID);
			} catch (Exception je) {
				log.severe(this + " incurred Exception while processing "
						+ request.getSummary());
				je.printStackTrace();
			}
			// if (Util.oneIn(100))
			// DelayProfiler.updateDelayNano("handleMessage", t);
			return true;
		}

		@Override
		protected Integer getPacketType(Object message) {
			assert (message instanceof RequestPacket);
			return message != null ? PaxosPacket.PaxosPacketType.PAXOS_PACKET
					.getInt() : null;
		}

		@Override
		protected Object processHeader(byte[] message, NIOHeader header) {
			PaxosPacketType type = PaxosPacket.getType(message);
			if (type != null) {
				assert (type == PaxosPacketType.REQUEST);
				try {
					return new RequestPacket(message);
				} catch (UnsupportedEncodingException | UnknownHostException e) {
					e.printStackTrace();
					return null;
				}
			}

			try {
				return new RequestPacket(
						(net.minidev.json.JSONObject) JSONValue
								.parse(MessageExtractor.decode(message)));
			} catch (UnsupportedEncodingException | JSONException e) {
				e.printStackTrace();
			}
			return null;
		}

		@Override
		protected boolean matchesType(Object message) {
			// TODO Auto-generated method stub
			return false;
		}
	}

	/******** End of ClientPacketDemultiplexer ******************/

	private class Retransmitter extends TimerTask {
		final int id;
		final RequestPacket req;
		final double timeout;
		boolean first;

		Retransmitter(int id, RequestPacket req) {
			this(id, req, getTimeout());
			first = true;
		}

		Retransmitter(int id, RequestPacket req, double timeout) {
			this.id = id;
			this.req = req;
			this.timeout = timeout;
			first = false;
		}

		public void run() {
			try {
				// checks parent queue
				if (requests.containsKey(req.requestID)) {
					incrRtxCount();
					if (first)
						incrNumRtxReqs();
					log.log(Level.INFO, "{0}{1}{2}{3}{4}{5}", new Object[] {
							"Retransmitting request ", "" + req.requestID,
							" to node ", id, ": ", req });
					sendToID(id, req.toJSONObject());
					timer.schedule(new Retransmitter(id, req, timeout * 2),
							(long) (timeout * 2));
				}
			} catch (IOException | JSONException e) {
				e.printStackTrace();
			}
		}
	}

	protected TESTPaxosClient(int id, NodeConfig<Integer> nc)
			throws IOException {
		this.myID = id;
		this.nc = (nc == null ? TESTPaxosConfig.getNodeConfig() : nc);
		niot = (new MessageNIOTransport<Integer, Object>(id, this.nc,
				(new ClientPacketDemultiplexer(this)), true,
				SSLDataProcessingWorker.SSL_MODES.valueOf(Config
						.getGlobalString(PC.CLIENT_SSL_MODE))));
		this.timer = new Timer(TESTPaxosClient.class.getSimpleName() + myID);

		synchronized (TESTPaxosClient.class) {
			if (executor == null || executor.isShutdown())
				// one extra thread for response tracker
				executor = (ScheduledThreadPoolExecutor) Executors
						.newScheduledThreadPool(Config
								.getGlobalInt(TC.NUM_CLIENTS) + 1);
		}
	}

	private boolean sendRequest(RequestPacket req) throws IOException,
			JSONException {
		int[] group = TESTPaxosConfig.getGroup(req.getPaxosID());
		int index = !PIN_CLIENT ? (int) (req.requestID % group.length)
				: (int) ((myID + 0) % group.length);
		assert (!(index < 0 || index >= group.length || TESTPaxosConfig
				.isCrashed(group[index])));
		return this.sendRequest(group[index], req);
	}

	private static final boolean ENABLE_REQUEST_COUNTS = false;
	static ConcurrentHashMap<Integer, Integer> reqCounts = new ConcurrentHashMap<Integer, Integer>();

	synchronized static void urc(int id) {
		if (ENABLE_REQUEST_COUNTS) {
			reqCounts.putIfAbsent(id, 0);
			reqCounts.put(id, reqCounts.get(id) + 1);
		}
	}

	class RequestAndCreateTime {
		final long createTime = System.currentTimeMillis();
		final RequestPacket request;

		RequestAndCreateTime(RequestPacket request) {
			// super(request);
			this.request = request;
		}
	}

	private static long lastWarningTime = 0;

	synchronized static boolean testAndSetLastWarningTime() {
		if (System.currentTimeMillis() - lastWarningTime > 1000) {
			lastWarningTime = System.currentTimeMillis();
			return true;
		}
		return false;
	}

	protected boolean sendRequest(int id, RequestPacket req)
			throws IOException, JSONException {
		InetAddress address = nc.getNodeAddress(id);
		assert (address != null) : id;
		Level level = Level.FINE;
		if (log.isLoggable(level))
			log.log(level,
					this.myID + " sending request to node {0}:{1}:{2} {2}",
					new Object[] { id, address, nc.getNodePort(id),
							req.getSummary(log.isLoggable(level)) });
		if (this.requests.put(req.requestID, new RequestAndCreateTime(req)) != null)
			return false; // collision in integer space
		this.incrReqCount();

		// no retransmission send
		while (this.niot.sendToID(id, req) <= 0) {
			try {
				Thread.sleep(req.lengthEstimate() / RequestPacket.SIZE_ESTIMATE
						+ 1);
				if (testAndSetLastWarningTime())
					log.log(Level.WARNING,
							"{0} retrying send to node {1} probably because of congestion",
							new Object[] { this, id });

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		urc(id);
		// retransmit if enabled
		if (TESTPaxosConfig.ENABLE_CLIENT_REQ_RTX)
			this.timer
					.schedule(new Retransmitter(id, req), (long) getTimeout());
		return true;
	}

	private int sendToID(int id, JSONObject json) throws IOException {
		return this.niot.sendToID(id, json);
	}

	private static final String GIBBERISH = "89432hoicnbsd89233u2eoiwdj-329hbousfnc";
	static String gibberish = Config.getGlobalBoolean(TC.COMPRESSIBLE_REQUEST) ? createGibberishCompressible()
			: createGibberish();

	private static final String createGibberishCompressible() {
		gibberish = GIBBERISH;
		int baggageSize = Config.getGlobalInt(TC.REQUEST_BAGGAGE_SIZE);
		if (gibberish.length() > baggageSize)
			gibberish = gibberish.substring(0, baggageSize);
		else
			while (gibberish.length() < baggageSize)
				gibberish += (baggageSize > 2 * gibberish.length() ? gibberish
						: gibberish.substring(0,
								baggageSize - gibberish.length()));
		Util.assertAssertionsEnabled();
		assert (gibberish.length() == baggageSize);
		return gibberish;
	}

	private static final String createGibberish() {
		int baggageSize = Config.getGlobalInt(TC.REQUEST_BAGGAGE_SIZE);
		byte[] buf = new byte[baggageSize];
		byte[] chars = Util.getAlphanumericAsBytes();
		for (int i = 0; i < baggageSize; i++)
			buf[i] = (chars[(int) (Math.random() * chars.length)]);
		gibberish = new String(buf);
		if (gibberish.length() > baggageSize)
			gibberish = gibberish.substring(0, baggageSize);
		else
			gibberish += gibberish.substring(0,
					baggageSize - gibberish.length());
		Util.assertAssertionsEnabled();
		assert (gibberish.length() == baggageSize);
		return gibberish;
	}

	/**
	 * @return Literally gibberish.
	 */
	public static String getGibberish() {
		return gibberish;
	}

	private RequestPacket makeRequest() {
		long reqID = ((long) (Math.random() * Long.MAX_VALUE));
		RequestPacket req = new RequestPacket(reqID,
		// createGibberish(), // randomly create each string
				gibberish, false);
		return req;
	}

	private RequestPacket makeRequest(String paxosID) {
		RequestPacket req = this.makeRequest();
		req.putPaxosID(paxosID != null ? paxosID : TEST_GUID, 0);
		return req;
	}

	protected boolean makeAndSendRequest(String paxosID) throws JSONException,
			IOException, InterruptedException, ExecutionException {
		return TESTPaxosClient.this.sendRequest(TESTPaxosClient.this
				.makeRequest(paxosID));
	}

	protected boolean makeAndSendRequestCallable(String paxosID)
			throws JSONException, IOException, InterruptedException,
			ExecutionException {
		executor.submit(new Callable<Boolean>() {
			public Boolean call() {
				// long t = System.nanoTime();
				RequestPacket req = TESTPaxosClient.this.makeRequest(paxosID);
				try {
					return TESTPaxosClient.this.sendRequest(req);
				} catch (IOException | JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
				// if (Util.oneIn(100))
				// DelayProfiler.updateDelayNano("makeAndSendRequest", t);
			}
		});
		// while loop to retry inside sendRequest
		return true;
	}

	protected static TESTPaxosClient[] setupClients(NodeConfig<Integer> nc) {
		System.out.println("\n\nInitiating paxos clients setup");
		TESTPaxosClient[] clients = new TESTPaxosClient[Config
				.getGlobalInt(TC.NUM_CLIENTS)];
		for (int i = 0; i < Config.getGlobalInt(TC.NUM_CLIENTS); i++) {
			try {
				clients[i] = new TESTPaxosClient(
						Config.getGlobalInt(TC.TEST_CLIENT_ID) + i, nc);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		System.out.println("Completed initiating "
				+ Config.getGlobalInt(TC.NUM_CLIENTS) + " clients");
		return clients;
	}

	private static void initResponseTracker(TESTPaxosClient[] clients,
			int numRequests) {
		executor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					waitForResponses(clients, System.currentTimeMillis(),
							numRequests);
				} catch (Exception | Error e) {
					e.printStackTrace();
				}
			}
		});
	}

	private final boolean PROBE_CAPACITY = Config
			.getGlobalBoolean(TC.PROBE_CAPACITY);

	protected static void sendTestRequests(int numReqs,
			TESTPaxosClient[] clients, double load) throws JSONException,
			IOException, InterruptedException, ExecutionException {
		System.out.print("\nTesting " + "[#requests=" + numReqs
				+ ", request_size=" + gibberish.length() + "B, #clients="
				+ clients.length + ", #groups=" + NUM_GROUPS_CLIENT
				+ ", #total_groups=" + Config.getGlobalInt(TC.NUM_GROUPS)
				+ ", group_size=" + Config.getGlobalInt(TC.GROUP_SIZE)
				+ ", load=" + Util.df(load) + "/s" + "]"
				+ (Config.getGlobalBoolean(TC.PROBE_CAPACITY) ? "" : "\n"));

		Future<?>[] futures = new Future<?>[Config.getGlobalInt(TC.NUM_CLIENTS)];
		// assert (executor.getCorePoolSize() == SEND_POOL_SIZE);
		if (!Config.getGlobalBoolean(TC.PROBE_CAPACITY))
			initResponseTracker(clients, numReqs);
		long initTime = System.currentTimeMillis();
		// if (TOTAL_LOAD > LOAD_THRESHOLD)
		{
			int SEND_POOL_SIZE = Config.getGlobalInt(TC.NUM_CLIENTS);
			if (SEND_POOL_SIZE > 0) {
				for (int i = 0; i < SEND_POOL_SIZE; i++) {
					final int j = i;
					assert (!executor.isShutdown());
					futures[j] = executor.submit(new Runnable() {
						public void run() {
							try {
								sendTestRequests(
										// to account for integer division
										j < SEND_POOL_SIZE - 1 ? numReqs
												/ SEND_POOL_SIZE : numReqs
												- numReqs / SEND_POOL_SIZE
												* (SEND_POOL_SIZE - 1),
										clients, false, load / SEND_POOL_SIZE);
							} catch (JSONException | IOException
									| InterruptedException | ExecutionException e) {
								e.printStackTrace();
							}
						}
					});
				}
				for (Future<?> future : futures)
					future.get();
			} else
				sendTestRequests(numReqs, clients, false, load);
		}
		// all done sending if here
		mostRecentSentRate = numReqs * 1000.0
				/ (System.currentTimeMillis() - initTime);
		System.out.println((Config.getGlobalBoolean(TC.PROBE_CAPACITY) ? "..."
				: "")
				+ "done sending "
				+ numReqs
				+ " requests in "
				+ Util.df((System.currentTimeMillis() - initTime) / 1000.0)
				+ " secs; estimated average_sent_rate = "
				+ Util.df(mostRecentSentRate) + "/s" + " \n " + reqCounts);

	}

	private static double mostRecentSentRate = 0;

	private static void sendTestRequests(int numReqs,
			TESTPaxosClient[] clients, boolean warmup, double rate)
			throws JSONException, IOException, InterruptedException,
			ExecutionException {
		if (warmup)
			System.out.print((warmup ? "\nWarming up " : "\nTesting ")
					+ "[#requests=" + numReqs + ", request_size="
					+ gibberish.length() + "B, #clients=" + clients.length
					+ ", #groups=" + NUM_GROUPS_CLIENT + ", load="
					+ Config.getGlobalDouble(TC.TOTAL_LOAD) + "/s" + "]...");
		RateLimiter rateLimiter = new RateLimiter(rate);
		// long initTime = System.currentTimeMillis();
		for (int i = 0; i < numReqs; i++) {
			while (!clients[i % NUM_CLIENTS]
					.makeAndSendRequest(TEST_GUID_PREFIX
							+ (Util.oneIn(WORKLOAD_SKEW) ? ((RANDOM_REPLAY + i) % (NUM_GROUPS_CLIENT))
									: 0)))
				;
			rateLimiter.record();
		}

		if (warmup)
			System.out.println("done");

	}

	protected static void waitForResponses(TESTPaxosClient[] clients,
			long startTime, int numRequests) {
		for (int i = 0; i < Config.getGlobalInt(TC.NUM_CLIENTS); i++) {
			while ((numResponses < numRequests)
					|| (clients[i].requests.size() > 0)) {
				synchronized (clients[i]) {
					if (clients[i].requests.size() > 0)
						try {
							clients[i].wait(4000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
				}
				if (System.currentTimeMillis() - startTime > 1000)
					System.out
							.println("["
									+ clients[i].myID
									+ "] "
									+ getWaiting(clients)
									+ (getRtxCount() > 0 ? "; #num_total_retransmissions = "
											+ getRtxCount()
											: "")
									+ (getRtxCount() > 0 ? "; num_retransmitted_requests = "
											+ getNumRtxReqs()
											: "")
									+ ("; aggregate response rate = "
											+ Util.df(getTotalThroughput(
													clients, startTime)) + " reqs/sec")
									+ "; time = "
									+ (System.currentTimeMillis() - startTime)
									+ " ms");
				if (System.currentTimeMillis() - startTime > MAX_WAIT_TIME) {
					String err = ("Timing out after having received "
							+ numResponses + " responses for " + numRequests + " requests");
					System.out.println(err);
					log.warning(err);
					break;
				}
				// if (clients[i].requests.size() > 0)
				try {
					Thread.sleep(1000);
					// System.out.println(DelayProfiler.getStats());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		runDone = true;
		assert (numResponses >= numRequests && noOutstanding(clients)) : numResponses
				+ " responses received for " + numRequests + " requests";
		synchronized (TESTPaxosClient.class) {
			TESTPaxosClient.class.notify();
		}
		assert (numResponses >= numRequests && noOutstanding(clients));
		clearOutstanding(clients);
	}

	private static void clearOutstanding(TESTPaxosClient[] clients) {
		for (TESTPaxosClient client : clients)
			client.requests.clear();
	}

	private static boolean runDone = false;

	protected static boolean noOutstanding(TESTPaxosClient[] clients) {
		boolean noOutstanding = true;
		for (TESTPaxosClient client : clients)
			if (!(noOutstanding = noOutstanding && client.noOutstanding()))
				System.out.println(client + " has " + client.requests.size()
						+ " outstanding requests");
		return noOutstanding;
	}

	protected static Set<RequestPacket> getMissingRequests(
			TESTPaxosClient[] clients) {
		Set<RequestPacket> missing = new HashSet<RequestPacket>();
		for (int i = 0; i < clients.length; i++) {
			for (RequestAndCreateTime reqCT : clients[i].requests.values())
				missing.add(reqCT.request);
		}
		return missing;
	}

	private static String getWaiting(TESTPaxosClient[] clients) {
		int total = 0;
		String s = " unfinished requests: [ ";
		for (int i = 0; i < clients.length; i++) {
			s += "C" + i + ":" + clients[i].requests.size() + " ";
			total += clients[i].requests.size();
		}
		s += "]; numResponses = " + numResponses;
		return total + s;
	}

	private static double getTotalThroughput(TESTPaxosClient[] clients,
			long startTime) {
		int totalExecd = 0;
		for (int i = 0; i < clients.length; i++) {
			totalExecd += clients[i].getRunReplyCount();
		}

		return totalExecd * 1000.0 / (System.currentTimeMillis() - startTime);
	}

	protected static void printOutput(TESTPaxosClient[] clients) {
		for (int i = 0; i < Config.getGlobalInt(TC.NUM_CLIENTS); i++) {
			if (clients[i].requests.isEmpty()) {
				System.out.println("\n\n[SUCCESS] requests issued = "
						+ clients[i].getTotalRequestCount()
						+ "; requests turned to no-ops = "
						+ clients[i].getNoopCount() + "; responses received = "
						+ clients[i].getTotalReplyCount() + "\n");
			} else
				System.out.println("\n[FAILURE]: Requests issued = "
						+ clients[i].getTotalRequestCount()
						+ "; requests turned to no-ops = "
						+ clients[i].getNoopCount() + "; responses received = "
						+ clients[i].getTotalReplyCount() + "\n");
		}
	}

	protected static String getAggregateOutput(long delay) {
		return "\n  average_sent_rate = "
				+ Util.df(mostRecentSentRate)
				+ "/s"
				+ "\n  average_response_time = "
				+ Util.df(TESTPaxosClient.getAvgLatency())
				+ "ms"
				+ "\n  average_response_rate = "
				+ Util.df(Config.getGlobalInt(TC.NUM_REQUESTS)
						* 1000.0
						/ (delay - (System.currentTimeMillis() - lastResponseReceivedTime)))
				+ "/s"

				+ "\n  noop_count = " + TESTPaxosClient.getTotalNoopCount()
				+ "\n  " 
				//+ DelayProfiler.getStats() + "\n  "
				;
	}

	/**
	 * This method probes for the capacity by multiplicatively increasing the
	 * load until the response rate is at least a threshold fraction of the
	 * injected load and the average response time is within a threshold. These
	 * thresholds are picked up from {@link TESTPaxosConfig}.
	 * 
	 * @param load
	 * @param clients
	 * @return
	 * @throws JSONException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static double probeCapacity(double load, TESTPaxosClient[] clients)
			throws JSONException, IOException, InterruptedException,
			ExecutionException {
		long runDuration = Config.getGlobalLong(TC.PROBE_RUN_DURATION); // seconds
		double responseRate = 0, capacity = 0, latency = Double.MAX_VALUE;
		double threshold = Config.getGlobalDouble(TC.PROBE_RESPONSE_THRESHOLD), loadIncreaseFactor = Config
				.getGlobalDouble(TC.PROBE_LOAD_INCREASE_FACTOR), minLoadIncreaseFactor = 1.01;
		int runs = 0, consecutiveFailures = 0;

		/**************** Start of capacity probes *******************/
		do {
			if (runs++ > 0)
				// increase probe load only if successful
				if (consecutiveFailures == 0)
					load *= loadIncreaseFactor;
				else
					// scale back if failed
					load *= (1 - (loadIncreaseFactor - 1) / 2);

			/* Two failures => increase more cautiously. Sometimes a failure
			 * happens in the very first run if the JVM is too cold, so we wait
			 * for at least two consecutive failures. */
			if (consecutiveFailures == 2)
				loadIncreaseFactor = (1 + (loadIncreaseFactor - 1) / 2);

			// we are within roughly 0.1% of capacity
			if (loadIncreaseFactor < minLoadIncreaseFactor)
				break;

			/* Need to clear requests from previous run, otherwise the response
			 * rate can be higher than the sent rate, which doesn't make sense. */
			for (TESTPaxosClient client : clients)
				client.requests.clear();
			TESTPaxosClient.resetLatencyComputation(clients);

			int numRunRequests = (int) (load * runDuration);
			long t1 = System.currentTimeMillis();
			sendTestRequests(numRunRequests, clients, load);

			// no need to wait for all responses
			// waitForResponses(clients, t1);
			while (numResponses < threshold * numRunRequests)
				Thread.sleep(500);

			responseRate = // numRunRequests
			numResponses * 1000.0 / (lastResponseReceivedTime - t1);
			latency = TESTPaxosClient.getAvgLatency();
			if (latency < Config.getGlobalLong(TC.PROBE_LATENCY_THRESHOLD))
				capacity = Math.max(capacity, responseRate);
			boolean success = (responseRate > threshold * load && latency <= Config
					.getGlobalLong(TC.PROBE_LATENCY_THRESHOLD));
			System.out.println("capacity >= " + Util.df(capacity)
					+ "/s; (response_rate=" + Util.df(responseRate)
					+ "/s, average_response_time=" + Util.df(latency) + "ms)"
					+ (!success ? "    !!!!!!!!FAILED!!!!!!!!" : ""));
			Thread.sleep(2000);
			if (success)
				consecutiveFailures = 0;
			else
				consecutiveFailures++;
		} while (consecutiveFailures < Config
				.getGlobalInt(TC.PROBE_MAX_CONSECUTIVE_FAILURES)
				&& runs < Config.getGlobalInt(TC.PROBE_MAX_RUNS));
		/**************** End of capacity probes *******************/
		System.out
				.println("capacity <= "
						+ Util.df(Math.max(capacity, load))
						+ ", stopping probes because"
						+ (capacity < threshold * load ? " response_rate was less than 95% of injected load"
								+ Util.df(load) + "/s; "
								: "")
						+ (latency > Config
								.getGlobalLong(TC.PROBE_LATENCY_THRESHOLD) ? " average_response_time="
								+ Util.df(latency)
								+ "ms"
								+ " >= "
								+ Config.getGlobalLong(TC.PROBE_LATENCY_THRESHOLD)
								+ "ms;"
								: "")
						+ (loadIncreaseFactor < minLoadIncreaseFactor ? " capacity is within "
								+ Util.df((minLoadIncreaseFactor - 1) * 100)
								+ "% of next probe load level;"
								: "")
						+ (consecutiveFailures > Config
								.getGlobalInt(TC.PROBE_MAX_CONSECUTIVE_FAILURES) ? " too many consecutive failures;"
								: "")
						+ (runs >= Config.getGlobalInt(TC.PROBE_MAX_RUNS) ? " reached limit of "
								+ Config.getGlobalInt(TC.PROBE_MAX_RUNS)
								+ " runs;"
								: ""));
		return responseRate;
	}

	protected static void waitUntilRunDone() throws InterruptedException {
		synchronized (TESTPaxosClient.class) {
			while (!runDone)
				TESTPaxosClient.class.wait();
		}
	}

	protected static void twoPhaseTest(int numReqs, TESTPaxosClient[] clients)
			throws InterruptedException, JSONException, IOException,
			ExecutionException {
		// begin first run
		long t1 = System.currentTimeMillis();
		sendTestRequests(numReqs, clients,
				Config.getGlobalDouble(TC.TOTAL_LOAD));
		// waitForResponses(clients, t1);
		waitUntilRunDone();
		long t2 = System.currentTimeMillis();
		System.out.println("\n[run1]" + getAggregateOutput(t2 - t1));
		// end first run

		resetLatencyComputation(clients);
		Thread.sleep(1000);

		// begin second run
		t1 = System.currentTimeMillis();
		sendTestRequests(numReqs, clients,
				Config.getGlobalDouble(TC.TOTAL_LOAD));
		// waitForResponses(clients, t1);
		waitUntilRunDone();
		t2 = System.currentTimeMillis();
		printOutput(clients); // printed only after second
		System.out.println("\n[run2] " + getAggregateOutput(t2 - t1));
		// end second run
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Config.register(args);
			initStaticParams();
			TESTPaxosConfig.setConsoleHandler(Level.WARNING);
			TESTPaxosConfig.setDistribtedTest();

			TESTPaxosClient[] clients = TESTPaxosClient
					.setupClients(TESTPaxosConfig.getFromPaxosConfig(true));
			System.out.println(TESTPaxosConfig.getFromPaxosConfig(true));
			int numReqs = Config.getGlobalInt(TC.NUM_REQUESTS);

			// begin warmup run
			if (Config.getGlobalBoolean(TC.WARMUP)) {
				long t1 = System.currentTimeMillis();
				int numWarmupRequests = Math.min(numReqs, 10 * NUM_CLIENTS);
				sendTestRequests(numWarmupRequests, clients, true,
						10 * NUM_CLIENTS);
				waitForResponses(clients, t1, numWarmupRequests);
				System.out.println("[success]");
			}
			// end warmup run

			resetLatencyComputation(clients);

			if (Config.getGlobalBoolean(TC.PROBE_CAPACITY))
				TESTPaxosClient.probeCapacity(
						Config.getGlobalDouble(TC.PROBE_INIT_LOAD), clients);
			else
				TESTPaxosClient.twoPhaseTest(numReqs, clients);

			for (TESTPaxosClient client : clients) {
				client.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static int NUM_CLIENTS;
	private static String TEST_GUID_PREFIX;
	private static int WORKLOAD_SKEW;
	private static int RANDOM_REPLAY;
	private static int NUM_GROUPS_CLIENT;
	private static long MAX_WAIT_TIME;
	private static boolean PIN_CLIENT;
	private static String TEST_GUID;

	// need a method like this to support command-line initialization
	private static void initStaticParams() {
		NUM_CLIENTS = Config.getGlobalInt(TC.NUM_CLIENTS);
		TEST_GUID_PREFIX = Config.getGlobalString(TC.TEST_GUID_PREFIX);
		WORKLOAD_SKEW = Config.getGlobalInt(TC.WORKLOAD_SKEW);
		RANDOM_REPLAY = (int) (Math.random() * Config
				.getGlobalInt(TC.NUM_GROUPS_CLIENT));
		NUM_GROUPS_CLIENT = Config.getGlobalInt(TC.NUM_GROUPS_CLIENT);
		PIN_CLIENT = Config.getGlobalBoolean(TC.PIN_CLIENT);
		TEST_GUID = Config.getGlobalString(TC.TEST_GUID);
		MAX_WAIT_TIME = (long) (Config.getGlobalInt(TC.NUM_REQUESTS) / Config
				.getGlobalDouble(TC.TOTAL_LOAD))
				+ Config.getGlobalInt(TC.MAX_RESPONSE_WAIT_TIME);
		createGibberish();
	}
}
