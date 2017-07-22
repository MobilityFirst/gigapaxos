package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.async.RequestCallbackFuture;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.NearestServerSelector;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.gigapaxos.interfaces.TimeoutRequestCallback;
import edu.umass.cs.gigapaxos.paxosutil.E2ELatencyAwareRedirector;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageExtractor;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.GigaPaxosClient;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket.ResponseCodes;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.EchoRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ServerReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.AppInstrumenter;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;
import edu.umass.cs.utils.Stringer;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * @param <V>
 *
 */
public abstract class ReconfigurableAppClientAsync<V> implements
		GigaPaxosClient<V>, AppRequestParser {
	static {
		ReconfigurationConfig.load();
	}
	/* Could be any high value coz we clear cached entry upon error or deletion.
	 * Having it too small means more query overhead (and potentially marginally
	 * improved responsiveness to replica failures). */
	private static final long MIN_REQUEST_ACTIVES_INTERVAL = 60000;

	/**
	 * The default timeout for {@link ClientRequest} as well as
	 * {@link ClientReconfigurationPacket} requests.
	 */
	public static final long DEFAULT_GC_TIMEOUT = 8000;
	private static final long DEFAULT_GC_LONG_TIMEOUT = 5 * 60 * 1000;
	// can by design sometimes take a long time
	private static final long SRP_GC_TIMEOUT = 60 * 60 * 000; // an hour

	private static final long MAX_COORDINATION_LATENCY = 2000;
	
	private final MessageNIOTransport<String, Object> niot, niotMA;
	final Set<InetSocketAddress> clientFacingReconfigurators, reconfigurators;
	final SSL_MODES sslMode;
	
	private final E2ELatencyAwareRedirector e2eRedirector;

	final GCConcurrentHashMapCallback defaultGCCallback = new GCConcurrentHashMapCallback() {
		@Override
		public void callbackGC(Object key, Object value) {
			log.log(Level.INFO, "{0} timing out {1}:{2}", new Object[] { this,
					key, value });
		}
	};

	/**
	 * Used to slowly forget replicas that cause a timeout. Using a moving
	 * average learner in {@link #e2eRedirector} means that a single timeout
	 * will not necessarily immediately exclude that replica. Furthermore,
	 * because of probing, some requests may still go to such "long latency"
	 * replicas that have actually failed. Note that excluding such replicas for
	 * a limited time doesn't prevent them from being probed again after that
	 * time. Unless we do active probing, we have to either direct some requests
	 * to failed replicas or be reconciled to not ever using them again even
	 * after they recover.
	 * 
	 */
	final GCConcurrentHashMapCallback appGCCallback = new GCConcurrentHashMapCallback() {
		@SuppressWarnings("unchecked")
		@Override
		public void callbackGC(Object key, Object value) {
			assert (value instanceof ReconfigurableAppClientAsync.RequestAndCallback);
			ReconfigurableAppClientAsync.this.e2eRedirector.learnSample(
					((RequestAndCallback) value).serverSentTo,
					System.currentTimeMillis()
							- ((RequestAndCallback) value).sentTime);
			ReconfigurableAppClientAsync.this.mostRecentlyWrittenMap
					.remove(((RequestAndCallback) value).request
							.getServiceName());
			log.log(Level.INFO, "{0} timing out {1}:{2}",
					new Object[] { this, key + "",
							((RequestAndCallback) value).request.getSummary() });
		}
	};

	final GCConcurrentHashMapCallback crpGCCallback = new GCConcurrentHashMapCallback() {
		@Override
		public void callbackGC(Object key, Object value) {
			assert (value instanceof ReconfigurableAppClientAsync.CRPRequestCallback);
			@SuppressWarnings("unchecked")
			CRPRequestCallback callback = (CRPRequestCallback) value;
			// clear creation memory if a timeout happens
			ReconfigurableAppClientAsync.this.mostRecentlyCreatedMap
					.remove(callback.request.getServiceName());
			// forget failed reconfigurators
			ReconfigurableAppClientAsync.this.e2eRedirector.learnSample(
					callback.serverSentTo, System.currentTimeMillis()
							- callback.sentTime);
			log.log(Level.INFO, "{0} timing out {1}:{2}", new Object[] { this,
					key, value });
		}
	};

	/**
[	 *
	 */
	public final class BlockingRequestCallback implements Callback<Request, V> {
		Request response = null;
		final Long timeout;
		
		BlockingRequestCallback(long timeout) {
			this.timeout = timeout;
		}

		@Override
		public V processResponse(Request response) {
			this.response = response;
			synchronized (this) {
				this.notify();
			}
			// return value is not used by anyone
			return null;
		}

		Request getResponse() {
			synchronized (this) {
				if (this.response == null)
					try {
						this.wait(this.timeout);
					} catch (InterruptedException e) {
						e.printStackTrace();
						// continue waiting
					}
			}
			return this.response;
		}
	}

	// all app client request callbacks
	private final GCConcurrentHashMap<Long, Callback<Request, V>> callbacks = new GCConcurrentHashMap<Long, Callback<Request, V>>(
			appGCCallback, getAppRequestTimeout(DEFAULT_GC_TIMEOUT));
	private final GCConcurrentHashMap<Long, Callback<Request, V>> callbacksLongTimeout = new GCConcurrentHashMap<Long, Callback<Request, V>>(
			defaultGCCallback, getAppRequestLongTimeout(DEFAULT_GC_LONG_TIMEOUT));

	// client reconfiguration packet callbacks
	private final GCConcurrentHashMap<String, Callback<Request,Request>> callbacksCRP = new GCConcurrentHashMap<String, Callback<Request,Request>>(
			crpGCCallback, getCRPTimeout(DEFAULT_GC_TIMEOUT));
	private final GCConcurrentHashMap<String, Callback<Request,Request>> callbacksCRPLongTimeout = new GCConcurrentHashMap<String, Callback<Request,Request>>(
			crpGCCallback, getCRPTimeout(DEFAULT_GC_TIMEOUT));

	// server reconfiguration packet callbacks,
	private final GCConcurrentHashMap<String, RequestCallback> callbacksSRP = new GCConcurrentHashMap<String, RequestCallback>(
			crpGCCallback, SRP_GC_TIMEOUT);

	// name->actives map
	private final GCConcurrentHashMap<String, ActivesInfo> activeReplicas = new GCConcurrentHashMap<String, ActivesInfo>(
			defaultGCCallback, MIN_REQUEST_ACTIVES_INTERVAL);
	// name->unsent app requests for which active replicas are not yet known
	private final GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>> requestsPendingActives = new GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>>(
			defaultGCCallback, getCRPTimeout(DEFAULT_GC_TIMEOUT)); // FIXME: long timeout version

	/**
	 * name->last replica from which coordinated response.
	 * 
	 * This map is used to implement {@link PaxosConfig.PC#READ_YOUR_WRITES} by
	 * sending a request to the active replica from which a response to the most
	 * recent coordinated request was received.
	 * 
	 * It is important to forget this entry when an app request times out,
	 * otherwise we may lose liveness even if a single active replica crashes.
	 */
	private final GCConcurrentHashMap<String, InetSocketAddress> mostRecentlyWrittenMap = new GCConcurrentHashMap<String, InetSocketAddress>(
			defaultGCCallback, MAX_COORDINATION_LATENCY);

	/**
	 * name->last reconfigurator from which a create confirmation was received.
	 * 
	 * Sending ClientReconfigurationPackets to a reconfigurator that confirmed a
	 * creation implies that a request for active replicas immediately after
	 * will necessarily succeed under graceful conditions; sending to a random
	 * replica however may not satisfy this property.
	 * 
	 * Sending ClientReconfigurationPackets to the same reconfigurator
	 * repeatedly has the downside of being less agile to reconfigurator
	 * failures, so it is important that this entry be removed when a
	 * ClientReconfigurationPacket times out in callbacksCRP, otherwise we may
	 * lose liveness even if a single reconfigurator crashes.
	 */
	@SuppressWarnings("serial")
	// never serialized
	private final LinkedHashMap<String, InetSocketAddress> mostRecentlyCreatedMap = new LinkedHashMap<String, InetSocketAddress>() {
		protected boolean removeEldestEntry(
				Map.Entry<String, InetSocketAddress> eldest) {
			return size() > 1024;
		}
	};

	Timer timer = null;

	private static final Logger log = Logger
			.getLogger(ReconfigurableAppClientAsync.class.getName()); // Reconfigurator.getLogger();

	private static int maxOutstandingAppRequests = 4096;

	private static int maxOutstandingCRPRequests = 4096;

	class ActivesInfo {
		final Set<InetSocketAddress> actives;
		// last queried time used to rate limit RequestActiveReplicas queries
		final long createTime;

		ActivesInfo(Set<InetSocketAddress> actives, long createTime) {
			this.actives = actives;
			this.createTime = createTime;
		}
	}

	/**
	 * The constructor specifies the default set of reconfigurators. This set
	 * may change over time, so it is the caller's responsibility to ensure that
	 * this set remains up-to-date. Some staleness however can be tolerated as
	 * reconfigurators will by design forward a request to the responsible
	 * reconfigurator if they are not responsible.
	 * 
	 * 
	 * @param reconfigurators
	 * @param sslMode
	 * @param clientPortOffset
	 * @param checkConnectivity
	 *            If true, a connectivity check with at least one reconfigurator
	 *            will be enforced and an exception will be thrown if
	 *            unsuccessful.
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync(Set<InetSocketAddress> reconfigurators,
			SSLDataProcessingWorker.SSL_MODES sslMode, int clientPortOffset,
			boolean checkConnectivity) throws IOException {
		this.sslMode = sslMode;
           
		// NEW CODE
                Set<IntegerPacketType> allTypes = new HashSet<>();
                allTypes.addAll(getRequestTypes());
                allTypes.addAll(getMutualAuthTypes());
                 
		// set up transport
		(this.niot = (new MessageNIOTransport<String, Object>(null, null,
				(new ClientPacketDemultiplexer(getRequestTypes())), true,
				sslMode))).setName(this.toString());
		if (this.sslMode != ReconfigurationConfig.getServerSSLMode()
				&& (!this.getMutualAuthTypes().isEmpty() || !Config
						.getGlobalBoolean(RC.ALLOW_CLIENT_TO_CREATE_DELETE)))
			(this.niotMA = (new MessageNIOTransport<String, Object>(null, null,
					(new ClientPacketDemultiplexer(allTypes)),
					true, ReconfigurationConfig.getServerSSLMode())))
					.setName(this.toString());
		else
			this.niotMA = null;
		
		log.log(Level.INFO,
				"{0} listening on {1}; ssl mode = {2}; client port offset = {3}",
				new Object[] { this, niot.getListeningSocketAddress(),
						this.sslMode, clientPortOffset });
		this.clientFacingReconfigurators = (PaxosConfig.offsetSocketAddresses(
				this.reconfigurators = reconfigurators, clientPortOffset));

		log.log(Level.FINE, "{0} reconfigurators={1}", new Object[] { this,
				(this.clientFacingReconfigurators) });
		this.e2eRedirector = new E2ELatencyAwareRedirector(
				this.niot.getListeningSocketAddress());

		if (checkConnectivity)
			this.checkConnectivity();
	}
	
	protected String getLabel() {
		return "";
	}


	private static long getCRPTimeout(long t) {
		return t;
	}

	private static long getAppRequestLongTimeout(long t) {
		return t;
	}

	private static long getAppRequestTimeout(long t) {
		return t;
	}

	private static int getMaxOutstandingAppRequests() {
		return maxOutstandingAppRequests;
	}
	private static int getMaxOutstandingCRPRequests() {
		return maxOutstandingCRPRequests;
	}
	protected static void setMaxOutstandingAppRequests(int n) {
		 maxOutstandingAppRequests=n;
	}
	protected static void getMaxOutstandingCRPRequests(int n) {
		 maxOutstandingCRPRequests=n;
	}

	/**
	 * @param timeout
	 */
	public final void setGCTimeout(long timeout) {
		this.activeReplicas.setGCTimeout(timeout);
		this.callbacks.setGCTimeout(timeout);
		this.callbacksCRP.setGCTimeout(timeout);
		this.requestsPendingActives.setGCTimeout(timeout);
	}

	/**
	 * @param reconfigurators
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync(Set<InetSocketAddress> reconfigurators)
			throws IOException {
		this(reconfigurators, ReconfigurationConfig.getClientSSLMode(),
				(ReconfigurationConfig.getClientPortOffset()), true);
	}

	/**
	 * @param reconfigurators
	 * @param checkConnectivity
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync(Set<InetSocketAddress> reconfigurators,
			boolean checkConnectivity) throws IOException {
		this(reconfigurators, ReconfigurationConfig.getClientSSLMode(),
				(ReconfigurationConfig.getClientPortOffset()),
				checkConnectivity);
	}

	/**
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync() throws IOException {
		this(ReconfigurationConfig.getReconfiguratorAddresses());
	}

	private static enum Keys {
		INCOMING_STRING
	};

	private static final Stringifiable<?> unstringer = new StringifiableDefault<String>(
			"");

	/* ***************** Start of ClientPacketDemultiplexer **************** */
	class ClientPacketDemultiplexer extends
			AbstractPacketDemultiplexer<Request> {

		private Logger log = ReconfigurableAppClientAsync.log;

		ClientPacketDemultiplexer(Set<IntegerPacketType> types) {
			super(1);
			this.register(ReconfigurationPacket.clientPacketTypes);
			this.register(ReconfigurationPacket.serverPacketTypes);
			this.register(types);
		}

		// TODO: unused, remove
		final BasicReconfigurationPacket<?> parseAsReconfigurationPacket(
				Object strMsg) {
			if (strMsg instanceof BasicReconfigurationPacket<?>)
				return (BasicReconfigurationPacket<?>) strMsg;

			if (strMsg instanceof ReplicableClientRequest)
				return null;

			if (!(strMsg instanceof JSONObject))
				return null;

			BasicReconfigurationPacket<?> rcPacket = ReconfigurationPacket
					.getReconfigurationPacketSuppressExceptions(
							(JSONObject) strMsg, unstringer);

			if (rcPacket != null
					&& (rcPacket = (rcPacket instanceof ClientReconfigurationPacket
							|| rcPacket instanceof ServerReconfigurationPacket
							|| rcPacket instanceof EchoRequest ? rcPacket
							: null)) == null)
				ReconfigurableAppClientAsync.log.log(Level.WARNING,
						"{0} dropping an undecodeable reconfiguration packet {1}",
						new Object[] { ReconfigurableAppClientAsync.this,
								strMsg });

			return rcPacket;
		}

		// TODO: unused, remove
		final Request parseAsAppRequest(Object msg) {
			if (msg instanceof Request)
				return (Request) msg;
			Request request = null;
			try {
				request = ReconfigurableAppClientAsync.this.jsonPackets
						&& msg instanceof JSONObject ? getRequestFromJSON((JSONObject) msg)
						: getRequest(msg instanceof String ? (String) msg
						/* This case below may happen if the app hasn't enabled
						 * JSON messages but the incoming message happened to be
						 * JSON formatted anyway. In this case, we will do the
						 * right thing by giving the app the incoming string. */
						: ((JSONObject) msg).getString(Keys.INCOMING_STRING
								.toString()));
			} catch (RequestParseException | JSONException e) {
				// should never happen unless app has bugs
				ReconfigurableAppClientAsync.log.log(Level.WARNING,
						"{0} received an unrecognizable message that can not be decoded as an application packet: {1}",
						new Object[] { this, msg });
				e.printStackTrace();
			}
			assert (request == null || request instanceof ClientRequest);
			return request;
		}

		private void updateBestReplica(RequestAndCallback callback) {
			Request request = callback.request;
			if (request instanceof ReplicableRequest
					&& ((ReplicableRequest) request).needsCoordination()) {
				// we could just always rely on serverSentTo here
				InetSocketAddress mostRecentReplica = callback.serverSentTo;

				assert (!ReconfigurableAppClientAsync.this.jsonPackets || mostRecentReplica != null);
				assert (request.getServiceName() != null) : callback.request;
				assert (mostRecentReplica != null) : callback.request;
				ReconfigurableAppClientAsync.this.mostRecentlyWrittenMap.put(
						request.getServiceName(), mostRecentReplica);
				ReconfigurableAppClientAsync.log.log(Level.FINE,
						"{0} most recently received a commit for a coordinated request [{1}] from replica {2} [{3}ms]",
						new Object[] {
								this,
								request.getSummary(),
								mostRecentReplica,
								(System.currentTimeMillis() - callback.sentTime) });
			}
			ReconfigurableAppClientAsync.log.log(Level.FINEST,
					"{0} updated latency map with sample {1}:{2} to {3}",
					new Object[] { this, callback.serverSentTo,
							System.currentTimeMillis() - callback.sentTime,
							ReconfigurableAppClientAsync.this.e2eRedirector });
			ReconfigurableAppClientAsync.this.e2eRedirector.learnSample(
					callback.serverSentTo, System.currentTimeMillis()
							- callback.sentTime);
		}

		private static final boolean USE_FORWARDEE_INFO = true;

		private Level debug = Level.FINEST;

		/**
		 * The main demultiplexing method for responses. It invokes the app
		 * request callback upon getting app responses or internally processes
		 * ReconfigurationPacket packets. It includes support for retransmitting
		 * active replicas requests upon a failure or retransmitting app
		 * requests upon an active replica error. Such retransmission should
		 * make it unlikely but not impossible for errors because of laggard
		 * replicas.
		 * 
		 * @param request
		 * @param header
		 * @return True if handled.
		 */

		@Override
		public boolean handleMessage(Request request, NIOHeader header) {
			assert (request != null); // otherwise would never come here
			ReconfigurableAppClientAsync.log.log(debug, "{0} handleMessage received {1}", new Object[] {
					this, request.getSummary(ReconfigurableAppClientAsync.log.isLoggable(debug)) });
			Request response = request;
			Callback<Request, V> callback = null;
			Callback<Request,Request> callbackCRP = null;
			if (response != null) {
				// execute registered callback
				if ((response instanceof ClientRequest)
						&& ((callback = ReconfigurableAppClientAsync.this.callbacks
								.remove(((ClientRequest) response)
										.getRequestID())) != null || (callback = ReconfigurableAppClientAsync.this.callbacksLongTimeout
								.remove(((ClientRequest) response)
										.getRequestID())) != null)) {

					if (!(response instanceof EchoRequest))
						AppInstrumenter
								.recvdResponse(((RequestAndCallback) callback).request);

					callback.processResponse(response instanceof ReplicableClientRequest ? ((ReplicableClientRequest) response)
							.getRequest() : (ClientRequest) response);

					updateBestReplica((RequestAndCallback) callback);
				} else if (response instanceof ClientRequest) {
					ReconfigurableAppClientAsync.log.log(Level.WARNING,
							"{0} received an app response with no matching callback{1}",
							new Object[] { ReconfigurableAppClientAsync.this,
									response.getSummary() });
				}
				// ActiveReplicaError has to be dealt with separately
				else if ((response instanceof ActiveReplicaError)
						&& (callback = callbacks
								.get(((ActiveReplicaError) response)
										.getRequestID())) != null
						&& callback instanceof ReconfigurableAppClientAsync.RequestAndCallback) {
					ActivesInfo activesInfo = ReconfigurableAppClientAsync.this.activeReplicas
							.get(response.getServiceName());
					if (activesInfo != null
							&& activesInfo.actives != null
							&& ((RequestAndCallback) callback)
									.incrActiveReplicaErrors() < activesInfo.actives
									.size()/2 + 1)
						try {
							InetSocketAddress untried = ((RequestAndCallback) callback)
									.getUntried(activesInfo.actives);
							ReconfigurableAppClientAsync.log.log(Level.INFO,
									"{0} received {1}; retrying with  replica {2}; attempts={3}",
									new Object[] { this, response.getSummary(),
											untried, ((RequestAndCallback) callback).activeReplicaErrors });
							// retry with a random other active replica
							ReconfigurableAppClientAsync.this.sendRequest(
									((RequestAndCallback) callback).request,
									untried, ((RequestAndCallback) callback).callback);
						} catch (IOException e) {
							e.printStackTrace();
							ReconfigurableAppClientAsync.this
									.cleanupActiveReplicasInfo((RequestAndCallback) callback);
						}
					else {
						callbacks
						.remove(((ActiveReplicaError) response)
								.getRequestID());
						ReconfigurableAppClientAsync.this
								.cleanupActiveReplicasInfo((RequestAndCallback) callback);
						callback.processResponse(response);
					}
				} else if (response instanceof ClientReconfigurationPacket) {
					ReconfigurableAppClientAsync.log.log(Level.FINE,
							"{0} received response {1} from reconfigurator {2}",
							new Object[] {
									this,
									response.getSummary(),
									((ClientReconfigurationPacket) response)
											.getSender() });
					// call create/delete app callback
					if ((callbackCRP = ReconfigurableAppClientAsync.this.callbacksCRP
							.remove(getKey((ClientReconfigurationPacket) response))) != null
							|| (callbackCRP = ReconfigurableAppClientAsync.this.callbacksCRPLongTimeout
									.remove(getKey((ClientReconfigurationPacket) response))) != null)
						callbackCRP.processResponse(response);
					else
						// we usually don't expect to find a callback for
						// ClientReconfigurationPacket
						ReconfigurableAppClientAsync.log.log(Level.FINEST,
								"{0} found no callback for {1}",
								new Object[] {
										this,
										response.getSummary(log
												.isLoggable(Level.FINEST)) });

					// if name deleted, clear cached actives
					if (response instanceof DeleteServiceName
							&& !((DeleteServiceName) response).isFailed())
						ReconfigurableAppClientAsync.this.activeReplicas
								.remove(response.getServiceName());

					// if RequestActiveReplicas, send or unpend pending requests
					if (response instanceof RequestActiveReplicas)
						ReconfigurableAppClientAsync.this
								.sendRequestsPendingActives((RequestActiveReplicas) response);

					// remember reconfigurator that initiated confirmed creation
					if (response instanceof CreateServiceName
							&& !((CreateServiceName) response).isFailed()) {
						InetSocketAddress isa = null;
						ReconfigurableAppClientAsync.this.mostRecentlyCreatedMap
								.put(response.getServiceName(),
										isa = ((ClientReconfigurationPacket) response)
												.getForwader() == null
												|| !USE_FORWARDEE_INFO ?
										// initiating reconfigurator
										((ClientReconfigurationPacket) response)
												.getSender() :
										// forwarder uses default port
												Util.getOffsettedAddress(
														((CreateServiceName) response)
																.getForwardee(),
														-getServerPortOffset()));
						ReconfigurableAppClientAsync.log
								.log(Level.FINER,
										"{0} inserted {1}:{2} into mostRecentlyCreatedMap",
										new Object[] { this,
												response.getServiceName(), isa });
					}
				} else if (response instanceof ServerReconfigurationPacket<?>) {
					if ((callbackCRP = ReconfigurableAppClientAsync.this.callbacksSRP
							.remove(getKey((ServerReconfigurationPacket<?>) response))) != null) {
						callbackCRP.processResponse(response);
					}
				} else if (response instanceof EchoRequest) {
					if ((callback = ReconfigurableAppClientAsync.this.callbacks
							.remove((EchoRequest) response)) != null) {
						callback.processResponse(response);
					}
				}
			}
			return true;
		}

		/**
		 * We simply return a constant integer corresponding to either a
		 * ClientReconfigurationPacket or app request here as this method is
		 * only used to confirm the demultiplexer, which is needed only in the
		 * case of multiple chained demultiplexers, but we only have one
		 * demultiplexer in this simple client, so it suffices to return any
		 * registered packet type.
		 * 
		 * @param strMsg
		 * @return
		 */
		@Override
		protected Integer getPacketType(Request strMsg) {
			return strMsg.getRequestType().getInt();
		}

		@Override
		protected Request processHeader(byte[] bytes, NIOHeader header) {
			ReconfigurableAppClientAsync.log.log(Level.FINEST, "{0} received message from {1}",
					new Object[] { this, header.sndr });
			String message = null;
			try {
				Integer type = null;
				JSONObject json = null;

				// reconfiguration packet
				if (BYTEIFICATION
						&& bytes.length >= Integer.BYTES
						&& ReconfigurationPacket.PacketType.intToType
								.containsKey(type = ByteBuffer
										.wrap(bytes, 0, 4).getInt())) {
					// typical reconfiguration protocol packet
					if (type != ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST
							.getInt())
						if (JSONPacket.couldBeJSON(message = MessageExtractor
								.decode(bytes, 4, bytes.length - 4)))
							return ReconfigurationPacket
									.getReconfigurationPacketSuppressExceptions(
											MessageExtractor
													.stampAddressIntoJSONObject(
															header.sndr,
															header.rcvr,
															new JSONObject(
																	message)),
											unstringer);
						// wrapped app packet, type == REPLICABLE_CLIENT_REQUEST
						else
							return ReconfigurableAppClientAsync.this instanceof AppRequestParserBytes ?
							// from bytes
							new ReplicableClientRequest(
									bytes,
									header,
									(AppRequestParserBytes) ReconfigurableAppClientAsync.this)
									// from string
									: new ReplicableClientRequest(
											bytes,
											(AppRequestParser) ReconfigurableAppClientAsync.this);

				}

				// old JSON option for reconfiguration packets
				else if (!BYTEIFICATION && JSONPacket.couldBeJSON(bytes)
						&& (message = MessageExtractor.decode(bytes)) != null
						&& (json = new JSONObject(message)) != null
						&& ReconfigurationPacket.isReconfigurationPacket(json))
					return ReconfigurationPacket
							.getReconfigurationPacketSuppressExceptions(
									MessageExtractor
											.stampAddressIntoJSONObject(
													header.sndr, header.rcvr,
													json), unstringer);

				// byte-parseable app packet
				else if (ReconfigurableAppClientAsync.this instanceof AppRequestParserBytes) {
					try {
						return ((AppRequestParserBytes) ReconfigurableAppClientAsync.this)
								.getRequest(bytes, header);
					} catch (Exception | Error e) {
						log.log(Level.SEVERE, "{0} received unparseable packet {1}:{2}", new Object[]{ReconfigurableAppClientAsync.this,
								header, new Stringer(bytes)});
						throw e;
					}
				}

				// JSON-stampable app packet
				else if (JSONPacket.couldBeJSON(bytes)
						&& ReconfigurableAppClientAsync.this.jsonPackets
						&& (message = MessageExtractor.decode(bytes)) != null)
					return getRequestFromJSON(MessageExtractor
							.stampAddressIntoJSONObject(header.sndr,
									header.rcvr, new JSONObject(message)));

				// default stringified request
				else
					return getRequest(new String(bytes,
							MessageNIOTransport.NIO_CHARSET_ENCODING));
			} catch (Exception je) {
				/* Do nothing but log. It is possible to receive rogue or
				 * otherwise undecodeable response packets; if so, we just
				 * return null. */
				je.printStackTrace();
				ReconfigurableAppClientAsync.log.log(Level.INFO, this + ":" + je.getMessage(), je);
			}
			return null;
		}

		@Override
		protected boolean matchesType(Object message) {
			return message instanceof Object;// String ;
		}

		public String toString() {
			return ReconfigurableAppClientAsync.this.toString();
		}
	}

	/* ***************** End of ClientPacketDemultiplexer **************** */

	private static final boolean BYTEIFICATION = Config
			.getGlobalBoolean(PC.BYTEIFICATION);

	private void cleanupActiveReplicasInfo(RequestAndCallback callback) {
		ReconfigurableAppClientAsync.this.activeReplicas
				.remove(callback.request.getServiceName());
		ReconfigurableAppClientAsync.this.mostRecentlyWrittenMap
				.remove(callback.request.getServiceName());
	}

	private static final boolean READ_YOUR_WRITES = Config
			.getGlobalBoolean(PC.READ_YOUR_WRITES);

	/**
	 * @param request
	 * @return The response obtained by executing this request.
	 * @throws IOException
	 */
	public Request sendRequest(Request request) throws IOException {
		BlockingRequestCallback callback;
		this.sendRequest(request, callback = new BlockingRequestCallback(0));
		return callback.getResponse(); // blocking
	}
	
	/**
	 * @param request
	 * @param timeout 
	 * @return The response obtained by executing this request.
	 * @throws IOException
	 */
	public Request sendRequest(Request request, long timeout) throws IOException {
		BlockingRequestCallback callback;
		this.sendRequest(request, callback = new BlockingRequestCallback(timeout));
		return callback.getResponse(); // blocking
	}

	/**
	 * This method will convert an app request to a
	 * {@link ReplicableClientRequest} and then send the request.
	 * 
	 * @param request
	 * @param callback
	 * @return True if sent successfully.
	 * @throws IOException
	 */
	public RequestFuture<V> sendRequest(Request request, Callback<Request, V> callback)
			throws IOException {
		return (request instanceof ClientRequest ? this.sendRequest(
				(ClientRequest) request, callback) : this.sendRequest(
				ReplicableClientRequest.wrap(request), callback));
	}

	// TODO: true hasn't been tested rigorously
	private static final boolean ENABLE_ID_TRANSFORM = Config
			.getGlobalBoolean(RC.ENABLE_ID_TRANSFORM);

	/**
	 * @param request
	 * @param server
	 * @return The response obtained by executing {@code request}.
	 * @throws IOException
	 */
	public Request sendRequest(ClientRequest request, InetSocketAddress server) throws IOException {
		BlockingRequestCallback callback;
		this.sendRequestAnycast(request, callback = new BlockingRequestCallback(0));
		return callback.getResponse(); // blocking
		
	}
	/**
	 * @param request
	 * @param server
	 * @param timeout 
	 * @return The response obtained by executing {@code request}.
	 * @throws IOException
	 */
	public Request sendRequest(ClientRequest request, InetSocketAddress server, long timeout) throws IOException {
		BlockingRequestCallback callback;
		this.sendRequestAnycast(request, callback = new BlockingRequestCallback(timeout));
		return callback.getResponse(); // blocking
		
	}
	/**
	 * @param request
	 * @param server
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 */
	public RequestFuture<V> sendRequest(ClientRequest request, InetSocketAddress server,
			Callback<Request, V> callback) throws IOException {
		boolean sendFailed = false;
		assert (request.getServiceName() != null);
		Callback<Request, V> prev = null;

		// send mutual auth requests to main active replica port instead
		if (this.sendToServerPort(request))
			server = Util.offsetPort(server, getServerPortOffset());

		// use replica recently written to if any
		InetSocketAddress mostRecentlyWritten = READ_YOUR_WRITES ? this.mostRecentlyWrittenMap
				.get(request.getServiceName()) : null;
		if (mostRecentlyWritten != null && !mostRecentlyWritten.equals(server)) {
			log.log(Level.FINE,
					"{0} using replica {1} most recently written to instead of server {2}",
					new Object[] { this, mostRecentlyWritten, server });
			server = mostRecentlyWritten;
		}

		if (this.numOutstandingAppRequests() > getMaxOutstandingAppRequests())
			throw new IOException("Too many outstanding requests");

		if (!(request instanceof EchoRequest))
			AppInstrumenter.outstandingAppRequest(
					this.numOutstandingAppRequests(), request);

		RequestCallbackFuture<V> future = callback instanceof RequestCallbackFuture ? (RequestCallbackFuture<V>) callback
				: new RequestCallbackFuture<V>(request, callback);
			
		try {
			Map<Long, Callback<Request,V>> correctMap = (!hasLongTimeout(callback) ? this.callbacks
					: this.callbacksLongTimeout);
			RequestAndCallback requestAndCallback = new RequestAndCallback(
					request, future).setServerSentTo(server);
			// transforms all requests to a unique ID unless retransmission
			while ((prev = (correctMap)
					.putIfAbsent(
							request.getRequestID(),
							requestAndCallback)) != null
					&& !((RequestAndCallback) prev).request.equals(request))
				if (ENABLE_ID_TRANSFORM)
					request = ReplicableClientRequest.wrap(request,
							(long) (Math.random() * Long.MAX_VALUE));
				else
					throw new IOException(this
							+ " received unequal requests with the same ID "
							+ request.getRequestID() + ":\n"
							+ request.getClass() + ":" + request.getSummary()
							+ "\n  !=\n"
							+ ((RequestAndCallback) prev).request.getClass()
							+ ":"
							+ ((RequestAndCallback) prev).request.getSummary());

			// if callback has changed, replace old with new
			if (prev != null
			// not internal retransmission
					&& callback != ((RequestAndCallback) prev).callback)
				(correctMap).put(
						request.getRequestID(), requestAndCallback);

			// special case for long timeout tasks
			if (hasLongTimeout(callback) && prev==null)
				this.spawnGCClientRequest(
						((TimeoutRequestCallback) callback).getTimeout(),
						request);

			sendFailed = this.getNIO(request).sendToAddress(server, request) <= 0;
			Level level = request instanceof EchoRequest ? Level.FINE
					: Level.FINE;
			log.log(level,
					"{0} {1} request {2} to server {3}",
					new Object[] {
							this,
							!sendFailed ? "sent" : "failed to send",
							ReconfigurationConfig.getSummary(request,
									log.isLoggable(level)), server });
			if (!sendFailed && !(request instanceof EchoRequest))
				AppInstrumenter.sentRequest(request);
		} finally {
			if (sendFailed && prev == null) {
				this.callbacks.remove(request.getRequestID(), callback);
				this.callbacksLongTimeout.remove(request.getRequestID(),
						callback);
				// FIXME: return an error to the caller
				return null;
			}
		}
		return future;//request.getRequestID();
	}

	/**
	 * @param type
	 * @param name
	 * @param initialState
	 *            Used only if type is
	 *            {@link edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType#CREATE_SERVICE_NAME}
	 *            .
	 * @param callback
	 * @throws IOException
	 */
	public void sendReconfigurationRequest(
			ReconfigurationPacket.PacketType type, String name,
			String initialState, RequestCallback callback) throws IOException {
		ClientReconfigurationPacket request = null;
		switch (type) {
		case CREATE_SERVICE_NAME:
			request = new CreateServiceName(name, initialState);
			break;
		case DELETE_SERVICE_NAME:
			request = new DeleteServiceName(name);
			break;
		case REQUEST_ACTIVE_REPLICAS:
			request = new RequestActiveReplicas(name);
			break;
		default:
			break;
		}
		this.sendRequest(request, callback);
	}

	/**
	 * Server reconfiguration packets have to be privileged, so we authenticate
	 * them implicitly by listening for them only on the server-server
	 * MUTUAL_AUTH port. So we subtract the offset below accordingly.
	 * 
	 * @param rcPacket
	 * @param callback
	 * @throws IOException
	 */
	public void sendRequest(ServerReconfigurationPacket<?> rcPacket,
			RequestCallback callback) throws IOException {
		if (this.numOutStandingCRPs() > getMaxOutstandingCRPRequests())
			throw new IOException("Too many outstanding requests");

		InetSocketAddress isa = getRandom(this.clientFacingReconfigurators
				.toArray(new InetSocketAddress[0]));
		// overwrite the most recent callback
		this.callbacksSRP.put(getKey(rcPacket), callback);
		this.niot.sendToAddress(Util.offsetPort(
				isa,
				// subtract offset to go to server-server port
				getServerPortOffset()), rcPacket);
	}
	
	private int getServerPortOffset() {
		return this.sslMode == SSL_MODES.CLEAR ? -ReconfigurationConfig
				.getClientPortClearOffset() : -ReconfigurationConfig
				.getClientPortSSLOffset();
	}
	
	private static RequestCallback defaultCRPCallback = new RequestCallback() {
		@Override
		public void handleResponse(Request response) {
		}
	};

	
	/**
	 * @param request
	 * @return ClientReconfigurationPacket response for {@code request}.
	 * @throws IOException
	 */
	public RequestFuture<ClientReconfigurationPacket> sendRequest(
			ClientReconfigurationPacket request) throws IOException,
			ReconfigurationException {
		return this.sendRequest(request,
				defaultCRPCallback);
	}

	private static RequestFuture<ClientReconfigurationPacket> toRequestFutureCRP(
			RequestFuture<Request> future) {
		return new RequestFuture<ClientReconfigurationPacket>() {

			@Override
			public boolean isDone() {
				return future.isDone();
			}

			@Override
			public ClientReconfigurationPacket get()
					throws InterruptedException, ExecutionException {
				return processCRPIfException((ClientReconfigurationPacket) future.get());
			}

			@Override
			public ClientReconfigurationPacket get(long timeout, TimeUnit unit)
					throws InterruptedException, ExecutionException,
					TimeoutException {
				return processCRPIfException((ClientReconfigurationPacket) future.get(timeout, unit));
			}
		};
	}

	private static ClientReconfigurationPacket processCRPIfException(
			ClientReconfigurationPacket crp) throws ExecutionException {
		try {
			return processCRPResponse(crp);
		} catch (ReconfigurationException e) {
			throw new ExecutionException(e);
		}
	}

	private static ClientReconfigurationPacket processCRPResponse(
			ClientReconfigurationPacket crp) throws ReconfigurationException {
		if (crp.isFailed())
			throw new ReconfigurationException(crp.getResponseCode(),
					crp.getResponseMessage());
		return crp;
	}

	/**
	 * @param request
	 * @param timeout Timeout period in milliseconds; 0 means infinity.
	 * @return ClientReconfigurationPacket response for {@code request}.
	 * @throws IOException
	 */
	public ClientReconfigurationPacket sendRequest(
			ClientReconfigurationPacket request, long timeout)
			throws IOException, ReconfigurationException {
		RequestFuture<ClientReconfigurationPacket> future = this.sendRequest(request,
				defaultCRPCallback);
		try {
			return future.get(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			if(e.getCause() instanceof ReconfigurationException)
				throw (ReconfigurationException)(e.getCause());
			throw new ReconfigurationException(e);
		}
	}

	/**
	 * This method exists for backwards compatibility and is slated for deprecation.
	 * 
	 * @param request
	 * @param callback
	 * @return Future for asynchronous task
	 * @throws IOException
	 */
	public RequestFuture<ClientReconfigurationPacket> sendRequest(
			ClientReconfigurationPacket request, RequestCallback callback)
			throws IOException {
		InetSocketAddress server = null;
		return this
				.sendRequest(
						request,
						// to avoid lagging reconfigurators
						(server = this.mostRecentlyCreatedMap.get(request
								.getServiceName())) != null ? server
								// could also select random reconfigurator here
								: this.e2eRedirector.getNearest(this
										.sendToServerPort(request) ? this.reconfigurators
										: this.clientFacingReconfigurators),
						callback);
	}

	private void sendRequesNullCallback(ClientReconfigurationPacket request)
			throws IOException {
		this.sendRequest(request, (RequestCallback) null);
	}

	private boolean hasLongTimeout(Callback<Request,?> callback) {
		return callback instanceof TimeoutRequestCallback
				&& ((TimeoutRequestCallback) callback).getTimeout() > getCRPTimeout(DEFAULT_GC_TIMEOUT);
	}


	// we don't initialize a timer unless really needed at least once
	private void initTimerIfNeeded() {
		synchronized (this) {
			if (this.timer == null)
				this.timer = new Timer(this.toString(), true);
		}
	}

	private void spawnGCClientReconfigurationPacket(long timeout,
			ClientReconfigurationPacket request) {
		initTimerIfNeeded();
		this.timer.schedule(new TimerTask() {

			@Override
			public void run() {
				ReconfigurableAppClientAsync.this.callbacksCRPLongTimeout
						.remove(request.getKey());
			}

		}, timeout);
	}

	private void spawnGCClientRequest(long timeout, ClientRequest request) {
		initTimerIfNeeded();
		this.timer.schedule(new TimerTask() {

			@Override
			public void run() {
				ReconfigurableAppClientAsync.this.callbacksLongTimeout
						.remove(request.getRequestID());
			}

		}, timeout);
	}

	private RequestFuture<ClientReconfigurationPacket> sendRequest(
			ClientReconfigurationPacket request,
			InetSocketAddress reconfigurator,
			Callback<Request, Request> callback) throws IOException {
		if (callback == null)
			callback = defaultCRPCallback;
		RequestCallbackFuture<Request> future = new RequestCallbackFuture<Request>(
				request, callback);
		if ((callback = new CRPRequestCallback(request, future, reconfigurator)) != null)
			if (!hasLongTimeout(callback))
				this.callbacksCRP.put(getKey(request), callback);
			else if (this.callbacksCRPLongTimeout
					.put(getKey(request), callback) == null)
				spawnGCClientReconfigurationPacket(
						((TimeoutRequestCallback) callback).getTimeout(),
						request);

		log.log(Level.FINER, "{0} sending QQ request {1} to {2}", new Object[] {
				this, request.getSummary(), reconfigurator });
		this.sendRequest(request, reconfigurator != null ? reconfigurator
				: this.e2eRedirector.getNearest(clientFacingReconfigurators));
		return toRequestFutureCRP(future);
	}

	private boolean sendRequest(ClientReconfigurationPacket request,
			InetSocketAddress reconfigurator) throws IOException {
		log.log(Level.FINER, "{0} sending request {1} to {2}", new Object[] {
				this, request.getSummary(), reconfigurator });
		return this.getNIO(request).sendToAddress(reconfigurator, request) > 0;
	}
	
	private MessageNIOTransport<String, Object> getNIO(Request request) {
		return this.sendToServerPort(request) && this.niotMA!=null ? this.niotMA : this.niot;
	}
	private boolean sendToServerPort(Request request) {
		return isMutualAuth(request)
				|| (request instanceof ClientReconfigurationPacket && !Config
						.getGlobalBoolean(RC.ALLOW_CLIENT_TO_CREATE_DELETE)) ? true
				: false;
	}
	private boolean isMutualAuth(Request request) {
		return (request instanceof ClientRequest && this.getMutualAuthTypes()
				.contains(request.getRequestType()));
	}

	private InetSocketAddress getRandom(InetSocketAddress[] isas) {
		return isas != null && isas.length > 0 ? isas[(int) (Math.random() * isas.length)]
				: null;
	}

	private static final String getKey(ClientReconfigurationPacket crp) {
		return crp.getRequestType() + ":" + crp.getServiceName();
	}

	private String getKey(ServerReconfigurationPacket<?> changeRCs) {
		return changeRCs.getRequestType()
				+ ":"
				+ (changeRCs.hasAddedNodes() ? changeRCs.newlyAddedNodes
						.keySet() : "") + ":"
				+ (changeRCs.hasDeletedNodes() ? changeRCs.deletedNodes : "");
	}

	class CRPRequestCallback implements Callback<Request,Request> {
		long sentTime = System.currentTimeMillis();
		final ClientReconfigurationPacket request;
		final Callback<Request,Request> callback;
		final InetSocketAddress serverSentTo;

		CRPRequestCallback(ClientReconfigurationPacket request,
				Callback<Request,Request> callback, InetSocketAddress serverSentTo) {
			this.request = request;
			this.callback = callback;
			this.serverSentTo = serverSentTo;
		}

		@Override
		public Request processResponse(Request response) {
			return this.callback.processResponse(response);
		}
	}

	class RequestAndCallback extends RequestCallbackFuture<V> implements Callback<Request, V> {
		long sentTime = System.currentTimeMillis();
		final ClientRequest request;
		final Callback<Request, V> callback;
		final NearestServerSelector redirector;
		InetSocketAddress serverSentTo;
		int heardFromRCs = 0; // for request actives
		int activeReplicaErrors = 0; // for app requests
		LinkedHashSet<InetSocketAddress> tried;

		RequestAndCallback(ClientRequest request, Callback<Request, V> callback) {
			this(request, callback, null);
		}

		RequestAndCallback(ClientRequest request,
				Callback<Request, V> callback, NearestServerSelector redirector) {
			super(request, callback);
			assert(!(callback instanceof ReconfigurableAppClientAsync.RequestAndCallback));
			this.request = request;
			this.callback = callback;
			this.redirector = redirector;
		}

		@Override
		public V processResponse(Request response) {
			return this.callback.processResponse(response);
		}

		public String toString() {
			return this.request.getSummary() + "->" + this.serverSentTo + " "
					+ (System.currentTimeMillis() - this.sentTime) + "ms back||" + this.callback;
		}

		RequestAndCallback setServerSentTo(InetSocketAddress isa) {
			this.sentTime = System.currentTimeMillis();
			this.serverSentTo = isa;
			return this;
		}

		 boolean isExpired() {
			return System.currentTimeMillis() - this.sentTime > getAppRequestTimeout(DEFAULT_GC_TIMEOUT);
		}

		private int incrHeardFromRCs() {
			return this.heardFromRCs++;
		}

		private int incrActiveReplicaErrors() {
			return ++this.activeReplicaErrors;
		}
		
		private InetSocketAddress getUntried(Set<InetSocketAddress> actives) {
			if(this.tried==null) this.tried = new LinkedHashSet<InetSocketAddress>();
			this.tried.add(this.serverSentTo);
			for(InetSocketAddress isa : actives) 
				if(!this.tried.contains(isa)) return isa;
			return null;
		}

	}

	/**
	 * @param request
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 */
	public RequestFuture<V> sendRequest(ClientRequest request, Callback<Request, V> callback)
			throws IOException {
		return this.sendRequest(request, callback, this.e2eRedirector);
	}

	private static final String ALL_ACTIVES = Config
			.getGlobalString(ReconfigurationConfig.RC.BROADCAST_NAME);

	/**
	 * @param request
	 * @return The response obtained by executing {@code request}.
	 * @throws IOException
	 */
	public Request sendRequestAnycast(ClientRequest request) throws IOException {	
	BlockingRequestCallback callback;
	this.sendRequestAnycast(request, callback = new BlockingRequestCallback(0));
	return callback.getResponse(); // blocking
	}

	/**
	 * @param request
	 * @param timeout
	 * @return The response obtained by executing {@code request}.
	 * @throws IOException
	 */
	public Request sendRequestAnycast(ClientRequest request, long timeout)
			throws IOException {
		BlockingRequestCallback callback;
		this.sendRequestAnycast(request,
				callback = new BlockingRequestCallback(timeout));
		return callback.getResponse(); // blocking
	}

	/**
	 * @param request
	 * @param callback
	 * @return Request ID of sent or enqueued request
	 * @throws IOException
	 */
	public RequestFuture<V> sendRequestAnycast(ClientRequest request,
			Callback<Request, V> callback) throws IOException {
		Set<InetSocketAddress> actives = null;
		ActivesInfo activesInfo = null;
		synchronized (this.activeReplicas) {
			if ((activesInfo = this.activeReplicas.get(ALL_ACTIVES)) != null
					&& (actives = activesInfo.actives) != null
					&& this.queriedActivesRecently(ALL_ACTIVES))
				return this.sendRequest(request, (InetSocketAddress)Util.selectRandom(actives),
						callback);
			// else
			RequestCallbackFuture<V> future;
			this.enqueueAndQueryForActives(new RequestAndCallback(request,
					future = (callback instanceof RequestCallbackFuture ? (RequestCallbackFuture<V>) callback
							: new RequestCallbackFuture<V>(request, callback))), true, true);
			return future; //request.getRequestID();
		}
	}

	private boolean queriedActivesRecently(String name) {
		Long lastQueriedTime = null;
		ActivesInfo activesInfo = null;
		if ((activesInfo = this.activeReplicas.get(name)) != null
				&& (lastQueriedTime = activesInfo.createTime) != null
				// shorter timeout until we have some actives
				&& ((activesInfo.actives == null && System.currentTimeMillis()
						- lastQueriedTime < getCRPTimeout(DEFAULT_GC_TIMEOUT) / 2)
				// longer timeout once we have some actives
				|| (activesInfo.actives != null && System.currentTimeMillis()
						- lastQueriedTime < MIN_REQUEST_ACTIVES_INTERVAL))) {

			return true;
		}
		if (lastQueriedTime != null)
			ReconfigurationConfig.getLogger().log(Level.FINEST,
					"{0} last queried time for {1} is {2}",
					new Object[] { this, name, lastQueriedTime });
		return false;
	}

	/**
	 * @param request
	 * @param callback
	 * @param redirector
	 * @return Request ID.
	 * @throws IOException
	 */
	public RequestFuture<V> sendRequest(ClientRequest request,
			Callback<Request, V> callback, NearestServerSelector redirector)
			throws IOException {

		Set<InetSocketAddress> actives = null;
		ActivesInfo activesInfo = null;
		synchronized (this.activeReplicas) {
			// lookup actives in the cache first
			if ((activesInfo = this.activeReplicas
					.get(request.getServiceName())) != null
					&& (actives = activesInfo.actives) != null
					&& this.queriedActivesRecently(request.getServiceName()))
				return this.sendRequest(
						request,
						redirector != null ? redirector.getNearest(actives)
								: (InetSocketAddress) (Util
										.selectRandom(actives)), callback);

			RequestCallbackFuture<V> future;
			// else enqueue them
			this.enqueueAndQueryForActives(new RequestAndCallback(request,
					future = (callback instanceof RequestCallbackFuture ? (RequestCallbackFuture<V>) callback
							: new RequestCallbackFuture<V>(request, callback))
					, redirector), false, false);
			return (RequestFuture<V>)future;//request.getRequestID();
		}
	}

	private boolean enqueueAndQueryForActives(RequestAndCallback rc,
			boolean force, boolean anycast) throws IOException {
		boolean queued = this.enqueue(rc, anycast);
		this.queryForActives(rc.request, force, anycast);
		return queued;
	}

	private boolean enqueue(RequestAndCallback rc, boolean anycast) {
		String name = anycast ? ALL_ACTIVES : rc.request.getServiceName();
		// if(this.requestsPendingActives.contains(name))
		this.requestsPendingActives.putIfAbsent(name,
				new LinkedBlockingQueue<RequestAndCallback>());
		LinkedBlockingQueue<RequestAndCallback> pending = this.requestsPendingActives
				.get(name);
		assert (pending != null);
		return pending.add(rc);
	}

	private void queryForActives(Request request, boolean forceRefresh,
			boolean anycast) throws IOException {
		String name = anycast ? ALL_ACTIVES : request.getServiceName();
		if (forceRefresh || !this.queriedActivesRecently(name)) {
			if (forceRefresh) {
				this.activeReplicas.remove(name);
				this.mostRecentlyWrittenMap.remove(name);
			}
			ReconfigurationConfig.getLogger().log(Level.FINE,
					"{0} requesting active replicas for {1}",
					new Object[] { this, name });
			this.sendRequesNullCallback(new RequestActiveReplicas(name));
			// this.lastQueriedActives.put(name, System.currentTimeMillis());
			this.activeReplicas.put(name,
					new ActivesInfo(null, System.currentTimeMillis()));
		} else {
			Level level = Level.FINER;
			log.log(level,
					"{0} not querying for actives for enqueued request {1}",
					new Object[] { this,
							request.getSummary(log.isLoggable(level)),
							forceRefresh, queriedActivesRecently(name) });
		}
	}

	private void sendRequestsPendingActives(RequestActiveReplicas response) {
		// learn sample latency
		this.e2eRedirector.learnSample(response.getSender(),
				System.currentTimeMillis() - response.getCreateTime());

		/* Invariants: (1) If a request is enqueued for querying for actives, at
		 * least one request for active replicas will be subsequently sent out
		 * or was recently (< MIN_REQUEST_ACTIVES_INTERVAL) sent. (2) If a
		 * successful (nonempty) response to an active replicas request arrives,
		 * it will unpend all pending requests to the corresponding name that
		 * were issued before the activeReplicas.put below. */
		Set<InetSocketAddress> actives = response.getActives();
		if (actives != null && !actives.isEmpty()) {
			synchronized (this.activeReplicas) {
				this.activeReplicas.put(response.getServiceName(),
						new ActivesInfo(actives, response.getCreateTime()));
			}
			if (this.mostRecentlyWrittenMap.contains(response.getServiceName())
					&& !actives.contains(this.mostRecentlyWrittenMap
							.get(response.getServiceName())))
				this.mostRecentlyWrittenMap.remove(response.getServiceName());
		} else {
			this.activeReplicas.remove(response.getServiceName());
			this.mostRecentlyWrittenMap.remove(response.getServiceName());
		}

		if (!this.requestsPendingActives.containsKey(response.getServiceName())) {
			log.log(Level.FINE,
					"{0} found no requests pending actives for {1}",
					new Object[] { this, response.getSummary() });
			return;
		}
		// else
		log.log(Level.FINE,
				"{0} trying to release requests pending actives for {1} : {2}",
				new Object[] {
						this,
						response.getSummary(),
						Util.truncatedLog(this.requestsPendingActives
								.get(response.getServiceName()), 8) });
		LinkedBlockingQueue<RequestAndCallback> pendingRequests = this.requestsPendingActives
				.get(response.getServiceName());
		if (pendingRequests == null)
			return;
		/* synchronization for concurrent access by different demultiplexer
		 * threads if there is more than one. */
		synchronized (pendingRequests) {
			if (pendingRequests.isEmpty())
				this.requestsPendingActives.remove(response.getServiceName());
			else
				for (Iterator<RequestAndCallback> reqIter = this.requestsPendingActives
						.get(response.getServiceName()).iterator(); reqIter
						.hasNext();) {
					RequestAndCallback rc = reqIter.next();
					if (rc.isExpired()) {
						/* Drop silently. The app must have assumed a timeout
						 * anyway by now. This is the case when a request
						 * pending actives has spent more time waiting for
						 * actives than the app request timeout, so it makes
						 * sense to drop it rather than send it out just because
						 * we can and potentially confuse the app with
						 * potentially duplicate executions. This scenario can
						 * happen when the first request's RequestActiveReplicas
						 * fails and then a subsequent request's
						 * RequestActiveReplicas much later succeeds promptly,
						 * so we are in a position where we can send out both
						 * the old and new requests.
						 * 
						 * Note that the first request may not necessarily get
						 * garbage collected by GCConcurrentHashMap even if
						 * CRP_GC_TIMEOUT is less than APP_REQUEST_TIMEOUT as GC
						 * is done only when really needed. */

						continue;
					}
					// request not expired if here
					if (actives != null && !actives.isEmpty()) {
						// release requests pending actives
						try {
							this.sendRequest(
									rc.request,
									rc.redirector != null ? rc.redirector
											.getNearest(actives)
											: (InetSocketAddress) (Util
													.selectRandom(actives)),
									rc.callback);
						} catch (IOException e) {
							log.log(Level.WARNING,
									"{0} encountered IOException while trying "
											+ "to release requests pending {1}",
									new Object[] { this, response.getSummary() });
							e.printStackTrace();
						}
					} else {
						assert (response.getHashRCs() != null && !response
								.getHashRCs().isEmpty());
						Set<RequestAndCallback> removals = new HashSet<RequestAndCallback>();
						for (RequestAndCallback pendingRequest : pendingRequests)
							// retry once with rest of the reconfigurators
							if (response.getHashRCs() != null
									&& pendingRequest.incrHeardFromRCs() == 0
									&& response.getHashRCs().size() > 1) {
								response.getHashRCs().remove(
										response.getSender());
								log.log(Level.FINE,
										"{0} received no actives from {1} for name {2}; trying other reconfigurators {3}",
										new Object[] { this,
												response.getSender(),
												response.getServiceName(),
												response.getHashRCs() });
								try {
									for (InetSocketAddress reconfigurator : response
											.getHashRCs())
										this.sendRequest(
												new RequestActiveReplicas(
														response.getServiceName()),
												Util.offsetPort(reconfigurator, getServerPortOffset()));
								} catch (IOException e) {
									e.printStackTrace();
									// do nothing
								}
							} else if (pendingRequest.heardFromRCs/* incrHeardFromRCs(
																 * ) */< response
									.getHashRCs().size() / 2 + 1) {
								// do nothing but wait to hear from majority
								log.log(Level.INFO,
										"{0} received no actives from {1} for name {2}",
										new Object[] { this,
												response.getSender(),
												response.getServiceName() });
							} else {
								/* name does not exist, so send error to all
								 * pending requests except ones to be retried */
								log.log(Level.FINE,
										"{0} returning {1} to pending request callbacks {2}",
										new Object[] {
												this,
												ClientReconfigurationPacket.ResponseCodes.ACTIVE_REPLICA_EXCEPTION,
												pendingRequests });
								pendingRequest.callback
										.processResponse(new ActiveReplicaError(
												response.getServiceName(),
												pendingRequest.request
														.getRequestID(),
												response).setResponseMessage(ClientReconfigurationPacket.ResponseCodes.ACTIVE_REPLICA_EXCEPTION
												+ ": No active replicas found for name \""
												+ response.getServiceName()
												+ "\" at replica "
												+ response.getSender()
												+ " likely because the name doesn't exist or because this name or"
												+ " active replicas or reconfigurators are being reconfigured: "
												+ pendingRequest.request
														.getSummary()));
								removals.add(pendingRequest);
							}
						pendingRequests.removeAll(removals);
						break;
					}
					reqIter.remove();
				}
		}
	}

	// also update e2e redirector if we are actively probing anyway
	private void updateE2ERedirector(EchoRequest response) {
		if (response.isRequest())
			return;
		for (InetSocketAddress isa : this.actives)
			if (isa.getAddress().equals(response.getSender()))
				this.e2eRedirector.learnSample(isa, System.currentTimeMillis()
						- response.sentTime);
	}

	private Set<InetSocketAddress> actives = new HashSet<InetSocketAddress>();
	private Set<InetSocketAddress> heardFrom = new HashSet<InetSocketAddress>();
	private LinkedHashMap<InetAddress, Long> closest = new LinkedHashMap<InetAddress, Long>();

	private static final int CLOSEST_K = Config.getGlobalInt(RC.CLOSEST_K);

	private void updateClosest(EchoRequest response) {
		this.updateE2ERedirector(response);
		this.heardFrom.add(response.getSender());
		synchronized (this.closest) {
			long delay = System.currentTimeMillis() - response.sentTime;
			if (closest.size() < CLOSEST_K
					&& !closest.containsKey(response.getSender()))
				closest.put(response.getSender().getAddress(), delay);
			InetAddress maxDelayAddr = null;
			for (InetAddress addr : closest.keySet())
				if ((closest.get(addr)) > delay)
					maxDelayAddr = addr;
			if (maxDelayAddr != null) {
				this.closest.remove(maxDelayAddr);
				this.closest.put(response.getSender().getAddress(), delay);
				log.log(Level.INFO, "{0} updated closest server to {1}",
						new Object[] { this, this.closest });
			}
		}
		if (this.heardFrom.size() == this.actives.size() / 2 + 1) {

			Set<InetSocketAddress> servers = new HashSet<InetSocketAddress>(
					actives);
			if (SEND_CLOSEST_TO_RECONFIGURATORS)
				servers.addAll(clientFacingReconfigurators);
			log.log(Level.INFO, "{0} sending closest-{1} map {2} to {3}",
					new Object[] { this, this.closest.size(), this.closest,
							servers });
			for (InetSocketAddress address : servers) {
				try {
					this.sendRequest(new EchoRequest((InetSocketAddress) null,
							this.closest), address, new Callback<Request, V>() {
						@Override
						public V processResponse(Request response) {
							// do nothing
							return null;
						}
					});
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	/* Reconfigurators really have no need for closest actives information, but
	 * we send it anyway because people seem to get confused by
	 * RTTEstimator.getClosest(.) not working inside shouldReconfigure(.) that
	 * is called at reconfigurators, not actives. */
	private static final boolean SEND_CLOSEST_TO_RECONFIGURATORS = Config
			.getGlobalBoolean(RC.SEND_CLOSEST_TO_RECONFIGURATORS);

	private static final boolean ORIENT_CLIENT = Config
			.getGlobalBoolean(RC.ORIENT_CLIENT);

	private void orient() {
		if (!ORIENT_CLIENT)
			return;
		assert (!actives.isEmpty());
		log.log(Level.INFO, "{0} orienting itself against {1}", new Object[] {
				this, actives });
		this.heardFrom.clear();
		for (InetSocketAddress address : actives) {
			try {
				this.sendRequest(new EchoRequest((InetSocketAddress) null),
						address, new Callback<Request, V>() {
							@Override
							public V processResponse(Request response) {
								log.log(Level.INFO,
										"{0} received response {1} for echo request",
										new Object[] {
												ReconfigurableAppClientAsync.this,
												response.getSummary() });
								assert (response instanceof EchoRequest);
								updateClosest((EchoRequest) response);
								return null;
							}
						});
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Connectivity check with reconfigurator {@code address} that will block
	 * for up to {@code timeout} duration. If {@code address} is null, the check
	 * will be performed against a random reconfigurator.
	 * 
	 * @param timeout
	 * @param address
	 * @return
	 */
	protected boolean checkConnectivity(long timeout, InetSocketAddress address) {
		Object monitor = new Object();
		boolean[] success = new boolean[1];
		try {
			this.sendRequest(
					new RequestActiveReplicas(Config
							.getGlobalString(RC.BROADCAST_NAME)),
					address != null ? address : this
							.getRandom(this.clientFacingReconfigurators
									.toArray(new InetSocketAddress[0])),
					new RequestCallback() {
						@Override
						public void handleResponse(Request response) {
							// any news is good news
							success[0] = true;
							if (response instanceof RequestActiveReplicas)
								ReconfigurableAppClientAsync.this.actives = ((RequestActiveReplicas) response)
										.getActives();
							synchronized (monitor) {
								monitor.notify();
							}
						}
					});

			log.log(Level.INFO,
					"{0} sent connectivity check {1}",
					new Object[] { this,
							Config.getGlobalString(RC.BROADCAST_NAME) });

			if (!success[0])
				synchronized (monitor) {
					monitor.wait(timeout);
				}
		} catch (InterruptedException | IOException e) {
			return false;
		}
		if (success[0]) {
			log.log(Level.INFO,
					"{0} connectivity check {1} successful",
					new Object[] { this,
							Config.getGlobalString(RC.BROADCAST_NAME) });
			this.orient();
		}
		return success[0];
	}

	/**
	 * Connectivity check that will be attempted up to {@code numAttempts} times
	 * each with a timeout of {@code attemptTimeout} with the reconfigurator
	 * {@code address}. If {@code address} is null, a random reconfigurator will
	 * be chosen for each attempt.
	 * 
	 * @param attemptTimeout
	 * @param numAttempts
	 * @param address
	 * @return
	 */
	protected boolean checkConnectivity(long attemptTimeout, int numAttempts,
			InetSocketAddress address) {
		int attempts = 0;
		while (attempts++ < numAttempts)
			if (this.checkConnectivity(attemptTimeout, address)) {
				return true;
			} else {
				System.out
						.print((attempts == 1 ? "Retrying connectivity check to "
								+ address + "..."
								: "")
								+ attempts + " ");
				this.checkConnectivity(attemptTimeout, address);
			}
		return false;
	}

	/**
	 * Default connectivity check timeout.
	 */
	public static final long CONNECTIVITY_CHECK_TIMEOUT = 4000;
	private static final String CONNECTION_CHECK_ERROR = "Unable to connect to any reconfigurator";

	/**
	 * A connectivity check with every known reconfigurator each with a default
	 * timeout of {@link #CONNECTIVITY_CHECK_TIMEOUT}.
	 * 
	 * @throws IOException
	 */
	public void checkConnectivity() throws IOException {
		for (InetSocketAddress address : this.clientFacingReconfigurators)
			if (this.checkConnectivity(CONNECTIVITY_CHECK_TIMEOUT,
			/* at least 2 attempts per reconfigurator and 3 if there is just a
			 * single reconfigurator. */
			2 + 1 / this.clientFacingReconfigurators.size(), address))
				return;

		throw new IOException(CONNECTION_CHECK_ERROR + " : "
				+ this.clientFacingReconfigurators);
	}

	/**
	 * 
	 */
	public void close() {
		if (this.timer != null)
			this.timer.cancel();
		this.niot.stop();
		if(this.niotMA!=null) this.niotMA.stop();
	}

	/**
	 * @return The list of default servers.
	 */
	public Set<InetSocketAddress> getDefaultServers() {
		return new HashSet<InetSocketAddress>((this.clientFacingReconfigurators));
	}

	public String toString() {
		return this.getClass().getSimpleName() + this.getLabel()
				+ (this.niot != null ? this.niot.getListeningSocketAddress()
						.getPort() : "");
	}

	static class AsyncClient extends ReconfigurableAppClientAsync<Request> {

		AsyncClient() throws IOException {
		}

		@Override
		public Request getRequest(String stringified)
				throws RequestParseException {
			return null;
		}

		@Override
		public Set<IntegerPacketType> getRequestTypes() {
			return new HashSet<IntegerPacketType>();
		}
	}

	/**
	 * @param json
	 * @return Request created from JSON {@code json}
	 * @throws RequestParseException
	 */
	public Request getRequestFromJSON(JSONObject json)
			throws RequestParseException {
		throw new RuntimeException(
				"enableJSONPackets() is true but getJSONRequest(JSONObject) has not been overridden");
	}

	private boolean jsonPackets = false;

	/**
	 * If true, the app will only be handed JSON formatted packets via
	 * getJSONRequest(), not via {@link #getRequest(String)}. If this method is
	 * invoked, the protected method {@link #getJSONRequest(JSONObject)} must be
	 * overridden.
	 * 
	 * @return {@code this}.
	 */
	@SuppressWarnings("javadoc")
	public ReconfigurableAppClientAsync<V> enableJSONPackets() {
		this.jsonPackets = true;
		return this;
	}

	private int numOutStandingCRPs() {
		return this.callbacksCRP.size() + this.callbacksCRPLongTimeout.size();
	}

	private int numOutstandingAppRequests() {
		return this.callbacks.size() + this.callbacksLongTimeout.size();
	}
	
	private static Set<IntegerPacketType> cachedMutualAuthTypes = null;

	private Set<IntegerPacketType> getMutualAuthTypes() {
		return cachedMutualAuthTypes != null ? cachedMutualAuthTypes
				: (cachedMutualAuthTypes = getMutualAuthRequestTypes()) != null ? cachedMutualAuthTypes
						: new HashSet<IntegerPacketType>();
	}
	

	/**
	 *
	 */
	public static class ReconfigurationException extends Exception {

		final ClientReconfigurationPacket.ResponseCodes code;
		/**
	   *
	   */
		private static final long serialVersionUID = 6816831396928147083L;

		/**
		 */
		protected ReconfigurationException() {
			super();
			this.code = null;
		}

		/**
		 * @param code
		 * @param GUID
		 * @param message
		 */
		public ReconfigurationException(ResponseCodes code, String message,
				String GUID) {
			super(message);
			this.code = code;
		}

		/**
		 * @param code
		 * @param message
		 */
		public ReconfigurationException(ResponseCodes code, String message) {
			this(code, message, (String) null);
		}

		/**
		 *
		 * @param message
		 * @param cause
		 */
		public ReconfigurationException(String message, Throwable cause) {
			super(message, cause);
			this.code = null;
		}

		/**
		 *
		 * @param message
		 */
		public ReconfigurationException(String message) {
			this(null, message);
		}

		/**
		 *
		 * @param throwable
		 */
		public ReconfigurationException(Throwable throwable) {
			super(throwable);
			this.code = null;
		}

		/**
		 * @param code
		 * @param message
		 * @param cause
		 */
		public ReconfigurationException(ResponseCodes code, String message,
				Throwable cause) {
			super(message, cause);
			this.code = code;
		}

		/**
		 * @return Code
		 */
		public ResponseCodes getCode() {
			return this.code;
		}
	}


	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException {
		ReconfigurableAppClientAsync<Request> client = new AsyncClient();
		client.close();
	}
}
