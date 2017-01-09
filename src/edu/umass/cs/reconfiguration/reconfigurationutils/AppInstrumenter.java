package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;

/**
 * @author arun
 * 
 *         A utility class to instrument app requests that could help diagnose
 *         bugs or performance issues.
 *
 */
public class AppInstrumenter {

	private static int numRecvdLocal = 0;
	private static int numRecvdCoordinated = 0;
	private static int numSentRespLocal = 0;
	private static int numSentRespCoordinated = 0;
	private static int numActiveReplicaErrors = 0;

	private static int numSentLocal = 0;
	private static int numRcvdRespLocal = 0;
	private static int numSentCoordinated = 0;
	private static int numRcvdRespCoordinated = 0;

	private static int numAppPackets = 0;

	private static Timer timer = new Timer(true);
	private static long lastUpdated = System.currentTimeMillis();
	private static int numOutstanding = 0;
	private static int numRcvdSSLPackets = 0;

	@SuppressWarnings("serial")
	private static final HashMap<Long, ClientRequest> outstanding = new HashMap<Long, ClientRequest>() {
		private static final int MAX_ENTRIES = 100;

		@SuppressWarnings("unused")
		protected boolean removeEldestEntry(
				@SuppressWarnings("rawtypes") Map.Entry eldest) {
			return size() > MAX_ENTRIES;
		}
	};

	private static final long PERIOD = 5000;
	static {
		if (Config.getGlobalBoolean(RC.ENABLE_INSTRUMENTATION))
			timer.scheduleAtFixedRate(new TimerTask() {
				public void run() {
					if (System.currentTimeMillis() - lastUpdated < PERIOD)
						System.out.println(AppInstrumenter.getStats()
								+ "; "
								+ DelayProfiler.getStats(new HashSet<String>(
										Arrays.asList("wrapWrite"))));
				}
			}, 0, PERIOD);
	}

	/**
	 * @param request
	 */
	public synchronized static void rcvdRequest(Request request) {
		lastUpdated = System.currentTimeMillis();
		if (request instanceof ReplicableRequest
				&& ((ReplicableRequest) request).needsCoordination())
			numRecvdCoordinated++;
		else
			numRecvdLocal++;
	}

	/**
	 * @param request
	 */
	public synchronized static void sentResponseLocal(Request request) {
		lastUpdated = System.currentTimeMillis();
		numSentRespLocal++;
	}

	/**
	 * @param request
	 */
	public synchronized static void sentResponseCoordinated(Request request) {
		lastUpdated = System.currentTimeMillis();
		numSentRespCoordinated++;
	}

	/**
	 * 
	 */
	public synchronized static void sentActiveReplicaError() {
		lastUpdated = System.currentTimeMillis();
		numActiveReplicaErrors++;
	}

	/**
	 * Packets received by processHeader, the first step after NIO.
	 */
	public synchronized static void recvdAppPacket() {
		lastUpdated = System.currentTimeMillis();
		numAppPackets++;
	}

	/**
	 * @return Statistics as a string.
	 */
	public synchronized static String getStats() {
		return (AppInstrumenter.class.getSimpleName()
				+ ":["
				// server local
				+ (numRecvdLocal != 0 ? "numRecvdLocal=" + numRecvdLocal : "")
				+ (numSentRespLocal != 0 ? ", numSentRespLocal="
						+ numSentRespLocal : "")
				// server coordinated
				+ (numRecvdCoordinated != 0 ? ", numRecvdCoordinated="
						+ numRecvdCoordinated : "")
				+ (numSentRespCoordinated != 0 ? ", numSentRespCoordinated="
						+ numSentRespCoordinated : "")
				// active replica error
				+ (numActiveReplicaErrors != 0 ? ", numActiveReplicaErrors="
						+ numActiveReplicaErrors : "")

				// client local
				+ (numSentLocal != 0 ? "numSentLocal=" + numSentLocal : "")
				+ (numRcvdRespLocal != 0 ? ", numRecvdRespLocal="
						+ numRcvdRespLocal : "")
				// client coordinated
				+ (numSentCoordinated != 0 ? ", numSentCoordinated="
						+ numSentCoordinated : "")
				+ (numRcvdRespCoordinated != 0 ? ", numRecvdRespCoordinated="
						+ numRcvdRespCoordinated : "")
				// active replica error
				+ (numActiveReplicaErrors != 0 ? ", numActiveReplicaErrors="
						+ numActiveReplicaErrors : "")

				+ (numAppPackets != 0 ? ", numAppPackets=" + numAppPackets : "")
				+ (numOutstanding != 0 ? ", numOutstanding=" + outstanding.size()
						: "")
//				 + (!outstanding.isEmpty() ? ", outstanding="
//				 + outstanding.keySet() : "")
				+ (numRcvdSSLPackets != 0 ? ", numRcvdSSLPackets="
						+ numRcvdSSLPackets : "")

		+ "]");
	}

	/**
	 * @param request
	 */
	public synchronized static void sentRequest(ClientRequest request) {
		lastUpdated = System.currentTimeMillis();
		if (request instanceof ReplicableRequest
				&& ((ReplicableRequest) request).needsCoordination())
			numSentCoordinated++;
		else
			numSentLocal++;
		outstanding.put(request.getRequestID(), request);
	}

	/**
	 * @param response
	 */
	public synchronized static void recvdResponse(ClientRequest response) {
		lastUpdated = System.currentTimeMillis();
		if (response instanceof ReplicableRequest
				&& ((ReplicableRequest) response).needsCoordination())
			numRcvdRespCoordinated++;
		else
			numRcvdRespLocal++;
		outstanding.remove(response.getRequestID());
	}

	/**
	 * @param numOutstandingAppRequests
	 * @param request
	 */
	public synchronized static void outstandingAppRequest(
			int numOutstandingAppRequests, ClientRequest request) {
		lastUpdated = System.currentTimeMillis();
		numOutstanding = numOutstandingAppRequests;
	}

	/**
	 * 
	 */
	public synchronized static void rcvdSSLPacket() {
		lastUpdated = System.currentTimeMillis();
		numRcvdSSLPackets++;
	}

}
