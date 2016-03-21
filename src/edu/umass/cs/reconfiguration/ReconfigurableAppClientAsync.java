package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.NearestServerSelector;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.interfaces.SummarizableRequest;
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
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureActiveNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureRCNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ServerReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public abstract class ReconfigurableAppClientAsync implements AppRequestParser {
	/* Could be any high value coz we clear cached entry upon error or deletion.
	 * Having it too small means more query overhead (and potentially marginally
	 * improved responsiveness to replica failures). */
	private static final long MIN_REQUEST_ACTIVES_INTERVAL = 60000;

	private static final long DEFAULT_GC_TIMEOUT = 8000;
	private static final long DEFAULT_GC_LONG_TIMEOUT = 5 * 60 * 1000;
	/**
	 * Application clients should assume that their sent requests may after all
	 * get executed after up to {@link #CRP_GC_TIMEOUT} +
	 * {@link #APP_REQUEST_TIMEOUT} time. Timing out and retransmitting the same
	 * request before this time increases the likelihood of duplicate
	 * executions. Of course, even with this thumb rule, it is possible for a
	 * request to have actually gotten executed at servers but no execution
	 * confirmation coming back.
	 * 
	 * FIXME: Rate limit by throwing exception if there are too many outstanding
	 * requests.
	 */
	public static final long APP_REQUEST_TIMEOUT = DEFAULT_GC_TIMEOUT;
	/**
	 * 
	 */
	public static final long APP_REQUEST_LONG_TIMEOUT = DEFAULT_GC_LONG_TIMEOUT;

	/**
	 * Default timeout for a client reconfiguration packet.
	 */
	public static final long CRP_GC_TIMEOUT = DEFAULT_GC_TIMEOUT;
	/**
	 * 
	 */
	public static final long CRP_GC_LONG_TIMEOUT = DEFAULT_GC_LONG_TIMEOUT;

	// can by design sometimes take a long time
	private static final long SRP_GC_TIMEOUT = 60 * 60 * 000; // an hour

	private static final long MAX_COORDINATION_LATENCY = 2000;

	final MessageNIOTransport<String, String> niot;
	final Set<InetSocketAddress> reconfigurators;
	final SSL_MODES sslMode;
	private final E2ELatencyAwareRedirector e2eRedirector;

	final GCConcurrentHashMapCallback defaultGCCallback = new GCConcurrentHashMapCallback() {
		@Override
		public void callbackGC(Object key, Object value) {
			log.log(Level.INFO, "{0} timing out {1}:{2}", new Object[] { this,
					key, value });
		}
	};

	// used to forget replicas that cause a timeout
	final GCConcurrentHashMapCallback appGCCallback = new GCConcurrentHashMapCallback() {
		@Override
		public void callbackGC(Object key, Object value) {
			assert (value instanceof RequestAndCallback);
			if (value instanceof RequestAndCallback)
				ReconfigurableAppClientAsync.this.e2eRedirector.learnSample(
						((RequestAndCallback) value).serverSentTo,
						System.currentTimeMillis()
								- ((RequestAndCallback) value).sentTime);
			log.log(Level.INFO, "{0} timing out {1}:{2}", new Object[] { this,
					key, value });
		}
	};

	// all app client request callbacks
	final GCConcurrentHashMap<Long, RequestCallback> callbacks = new GCConcurrentHashMap<Long, RequestCallback>(
			defaultGCCallback, APP_REQUEST_TIMEOUT);
	final GCConcurrentHashMap<Long, RequestCallback> callbacksLongTimeout = new GCConcurrentHashMap<Long, RequestCallback>(
			defaultGCCallback, APP_REQUEST_LONG_TIMEOUT);

	// client reconfiguration packet callbacks
	final GCConcurrentHashMap<String, RequestCallback> callbacksCRP = new GCConcurrentHashMap<String, RequestCallback>(
			defaultGCCallback, CRP_GC_TIMEOUT);
	final GCConcurrentHashMap<String, RequestCallback> callbacksCRPLongTimeout = new GCConcurrentHashMap<String, RequestCallback>(
			defaultGCCallback, CRP_GC_TIMEOUT);

	// server reconfiguration packet callbacks,
	final GCConcurrentHashMap<String, RequestCallback> callbacksSRP = new GCConcurrentHashMap<String, RequestCallback>(
			defaultGCCallback, SRP_GC_TIMEOUT);

	// name->actives map
	final GCConcurrentHashMap<String, Set<InetSocketAddress>> activeReplicas = new GCConcurrentHashMap<String, Set<InetSocketAddress>>(
			defaultGCCallback, MIN_REQUEST_ACTIVES_INTERVAL);
	// name->unsent app requests for which active replicas are not yet known
	final GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>> requestsPendingActives = new GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>>(
			defaultGCCallback, CRP_GC_TIMEOUT); // FIXME: long timeout version
	// name->last queried time to rate limit RequestActiveReplicas queries
	final GCConcurrentHashMap<String, Long> lastQueriedActives = new GCConcurrentHashMap<String, Long>(
			defaultGCCallback, DEFAULT_GC_TIMEOUT);
	final GCConcurrentHashMap<String, InetSocketAddress> mostRecentlyWrittenMap = new GCConcurrentHashMap<String, InetSocketAddress>(
			defaultGCCallback, MAX_COORDINATION_LATENCY);

	Timer timer = null;

	private static final Logger log = Reconfigurator.getLogger();

	private static final int MAX_OUTSTANDING_APP_REQUESTS = 4096;

	private static final int MAX_OUTSTANDING_CRP_REQUESTS = 4096;

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
		(this.niot = (new MessageNIOTransport<String, String>(null, null,
				(new ClientPacketDemultiplexer(getRequestTypes())), true,
				// This will be set in the gigapaxos.properties file that we
				// invoke the client using.
				sslMode))).setName(this.toString());
		log.log(Level.INFO,
				"{0} listening on {1}; ssl mode = {2}; client port offset = {3}",
				new Object[] { this, niot.getListeningSocketAddress(),
						this.sslMode, clientPortOffset });
		this.reconfigurators = (PaxosConfig.offsetSocketAddresses(
				reconfigurators, clientPortOffset));

		log.log(Level.FINE, "{0} reconfigurators={1}", new Object[] { this,
				(this.reconfigurators) });
		this.e2eRedirector = new E2ELatencyAwareRedirector(
				this.niot.getListeningSocketAddress());

		if (checkConnectivity)
			this.checkConnectivity();
	}

	/**
	 * @param reconfigurators
	 * @param sslMode
	 * @param clientPortOffset
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync(Set<InetSocketAddress> reconfigurators,
			SSLDataProcessingWorker.SSL_MODES sslMode, int clientPortOffset)
			throws IOException {
		this(reconfigurators, sslMode, clientPortOffset, false);
	}

	/**
	 * @param reconfigurators
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync(Set<InetSocketAddress> reconfigurators)
			throws IOException {
		this(reconfigurators, ReconfigurationConfig.getClientSSLMode(),
				(ReconfigurationConfig.getClientPortOffset()), false);
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

	private static Stringifiable<?> unstringer = new StringifiableDefault<String>(
			"");

	class ClientPacketDemultiplexer extends AbstractPacketDemultiplexer<Object> {

		ClientPacketDemultiplexer(Set<IntegerPacketType> types) {
			this.register(ReconfigurationPacket.clientPacketTypes);
			this.register(ReconfigurationPacket.serverPacketTypes);
			this.register(types);
		}

		private BasicReconfigurationPacket<?> parseAsReconfigurationPacket(
				Object strMsg) {
			if (!(strMsg instanceof JSONObject))
				return null;

			BasicReconfigurationPacket<?> rcPacket = ReconfigurationPacket
					.getReconfigurationPacketSuppressExceptions(
							(JSONObject) strMsg, unstringer);
			return (rcPacket instanceof ClientReconfigurationPacket) ? (ClientReconfigurationPacket) rcPacket
					: rcPacket instanceof ServerReconfigurationPacket ? ((ServerReconfigurationPacket<?>) rcPacket)
							: null;
		}

		private Request parseAsAppRequest(Object msg) {
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
				log.log(Level.WARNING,
						"{0} received an unrecognizable message that can not be decoded as an application packet: {2}",
						new Object[] { this, msg });
				e.printStackTrace();
			}
			assert (request == null || request instanceof ClientRequest);
			return request;
		}

		private void updateBestReplica(RequestCallback callback, Object msg) {
			assert (callback instanceof RequestAndCallback);
			Request request = ((RequestAndCallback) callback).request;
			if (request instanceof ReplicableRequest
					&& ((ReplicableRequest) request).needsCoordination()
					&& msg instanceof JSONObject) {
				InetSocketAddress mostRecentReplica = MessageNIOTransport
						.getSenderAddress((JSONObject) msg);
				assert (!ReconfigurableAppClientAsync.this.jsonPackets || mostRecentReplica != null);
				ReconfigurableAppClientAsync.this.mostRecentlyWrittenMap.put(
						request.getServiceName(), mostRecentReplica);
				log.log(Level.FINE,
						"{0} most recently received a commit for a coordinated request [{1}] from replica {2}",
						new Object[] { this, request.getSummary(),
								mostRecentReplica });
			} else
				log.log(Level.FINEST,
						"{0} did not update most recently written replica for request {1}",
						new Object[] { this, request.getSummary() });
		}

		@Override
		public boolean handleMessage(Object strMsg) {
			Request response = null;
			// first try parsing as app request
			if ((response = this.parseAsReconfigurationPacket(strMsg)) == null)
				// else try parsing as ClientReconfigurationPacket
				response = parseAsAppRequest(strMsg);

			RequestCallback callback = null;
			if (response != null) {
				// execute registered callback
				if ((response instanceof ClientRequest)
						&& ((callback = ReconfigurableAppClientAsync.this.callbacks
								.remove(((ClientRequest) response)
										.getRequestID())) != null || (callback = ReconfigurableAppClientAsync.this.callbacksLongTimeout
								.remove(((ClientRequest) response)
										.getRequestID())) != null)) {
					callback.handleResponse(((ClientRequest) response));
					updateBestReplica(callback, strMsg);
				}
				// ActiveReplicaError has to be dealt with separately
				else if ((response instanceof ActiveReplicaError)
						&& (callback = callbacks
								.remove(((ActiveReplicaError) response)
										.getRequestID())) != null
						&& callback instanceof RequestAndCallback) {

					ReconfigurableAppClientAsync.this.activeReplicas
							.remove(((RequestAndCallback) callback).request
									.getServiceName());
					/* auto-retransmitting can cause an infinite loop if the
					 * name does not exist at all, so we just throw the ball
					 * back to the app.
					 * 
					 * TODO: We could also retransmit once or a small number of
					 * times here before throwing back to the app. */
					callback.handleResponse(response);
				} else if (response instanceof ClientReconfigurationPacket) {
					// call create/delete app callback
					if ((callback = ReconfigurableAppClientAsync.this.callbacksCRP
							.remove(getKey((ClientReconfigurationPacket) response))) != null
							|| (callback = ReconfigurableAppClientAsync.this.callbacksCRPLongTimeout
									.remove(getKey((ClientReconfigurationPacket) response))) != null)
						callback.handleResponse(response);

					// if name deleted, clear cached actives
					if (response instanceof DeleteServiceName
							&& !((DeleteServiceName) response).isFailed())
						ReconfigurableAppClientAsync.this.activeReplicas
								.remove(response.getServiceName());

					// if RequestActiveReplicas, send or unpend pending requests
					if (response instanceof RequestActiveReplicas)
						ReconfigurableAppClientAsync.this
								.sendRequestsPendingActives((RequestActiveReplicas) response);

				} else if (response instanceof ServerReconfigurationPacket<?>) {
					if ((callback = ReconfigurableAppClientAsync.this.callbacksSRP
							.remove(getKey((ServerReconfigurationPacket<?>) response))) != null) {
						callback.handleResponse(response);
					}
				}
			}
			return true;
		}

		private static final boolean SHORT_CUT_TYPE_CHECK = true;

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
		protected Integer getPacketType(Object strMsg) {
			if (SHORT_CUT_TYPE_CHECK)
				return ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME
						.getInt();
			Request request = strMsg instanceof String ? this
					.parseAsAppRequest((String) strMsg) : null;
			if (request == null)
				request = this.parseAsReconfigurationPacket(strMsg);
			return request != null ? request.getRequestType().getInt() : null;
		}

		@Override
		protected Object getMessage(String message) {
			return message;
		}

		@Override
		protected Object processHeader(String message, NIOHeader header) {
			log.log(Level.FINEST, "{0} received message from {1}",
					new Object[] { this, header.sndr });
			try {
				long t = System.nanoTime();
				/* FIXME: This is inefficient and inelegant, but it is unclear
				 * what else we can do to avoid at least one unnecessary
				 * JSON<->string back and forth with the current NIO API and the
				 * fact that apps in general may not use JSON for their packets
				 * but we do. The alternative is to not provide the
				 * sender-address feature to async clients; if son, we don't
				 * need the JSON'ization below. */
				if (JSONPacket.couldBeJSON(message)) {
					JSONObject json = new JSONObject(message);
					MessageExtractor.stampAddressIntoJSONObject(header.sndr,
							header.rcvr, json);
					DelayProfiler.updateDelayNano("headerInsertion", t);
					// some inelegance to avoid a needless stringification
					json.put(Keys.INCOMING_STRING.toString(), message);
					return json;// inserted;
				}
			} catch (Exception je) {
				// do nothing
				je.printStackTrace();
			}
			return message;
		}

		@Override
		protected boolean matchesType(Object message) {
			return message instanceof Object;// String ;
		}

		public String toString() {
			return ReconfigurableAppClientAsync.this.toString();
		}
	}

	/**
	 * @param request
	 * @param server
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 */
	public Long sendRequest(ClientRequest request, InetSocketAddress server,
			RequestCallback callback) throws IOException {
		boolean sendFailed = false;
		assert (request.getServiceName() != null);
		RequestCallback prev = null;

		// use replica recently written to if any
		InetSocketAddress mostRecentlyWritten = this.mostRecentlyWrittenMap
				.get(request.getServiceName());
		if (mostRecentlyWritten != null && !mostRecentlyWritten.equals(server)) {
			log.log(Level.INFO,
					"{0} using replica {1} most recently written to instead of server {2}",
					new Object[] { this, mostRecentlyWritten, server });
			server = mostRecentlyWritten;
		}

		if (this.numOutStandingAppRequests() > MAX_OUTSTANDING_APP_REQUESTS)
			throw new IOException("Too many outstanding requests");

		try {
			RequestCallback original = callback;
			prev = (!hasLongTimeout(callback) ? this.callbacks
					: this.callbacksLongTimeout).put(
					request.getRequestID(),
					callback = new RequestAndCallback(request, callback)
							.setServerSentTo(server));
			if (hasLongTimeout(original))
				this.spawnGCClientRequest(
						((TimeoutRequestCallback) original).getTimeout(),
						request);

			sendFailed = this.niot.sendToAddress(server, request.toString()) <= 0;
			Level level = Level.INFO;
			log.log(level,
					"{0} sent request {1} to server {2}; [{3}]",
					new Object[] {
							this,
							ReconfigurationConfig.getSummary(request,
									log.isLoggable(level)), server,
							!sendFailed ? "success" : "failure" });
		} finally {
			if (sendFailed && prev == null) {
				this.callbacks.remove(request.getRequestID(), callback);
				this.callbacksLongTimeout.remove(request.getRequestID(),
						callback);
				// FIXME: return an error to the caller
				return null;
			}
		}
		return request.getRequestID();
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
	public void sendServerReconfigurationRequest(
			ServerReconfigurationPacket<?> rcPacket, RequestCallback callback)
			throws IOException {
		if (this.numOutStandingCRPs() > MAX_OUTSTANDING_CRP_REQUESTS)
			throw new IOException("Too many outstanding requests");

		InetSocketAddress isa = getRandom(this.reconfigurators
				.toArray(new InetSocketAddress[0]));
		// overwrite the most recent callback
		this.callbacksSRP.put(getKey(rcPacket), callback);
		this.niot.sendToAddress(Util.offsetPort(
				isa,
				// subtract offset to go to server-server port
				this.sslMode == SSL_MODES.CLEAR ? -ReconfigurationConfig
						.getClientPortClearOffset() : -ReconfigurationConfig
						.getClientPortSSLOffset()), rcPacket.toString());
	}

	/**
	 * @param request
	 * @param callback
	 * @throws IOException
	 */
	public void sendRequest(ClientReconfigurationPacket request,
			RequestCallback callback) throws IOException {
		this.sendRequest(request, callback,
				this.e2eRedirector.getNearest(this.reconfigurators));
	}

	private void sendRequest(ClientReconfigurationPacket request)
			throws IOException {
		this.sendRequest(request, null,
				this.e2eRedirector.getNearest(this.reconfigurators));
	}

	private boolean hasLongTimeout(RequestCallback callback) {
		return callback instanceof TimeoutRequestCallback
				&& ((TimeoutRequestCallback) callback).getTimeout() > CRP_GC_TIMEOUT;
	}

	// we don't initialize a timer unless really needed at least once
	private void initTimerIfNeeded() {
		synchronized (this) {
			if (this.timer == null)
				this.timer = new Timer(this.toString());
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

	private void sendRequest(ClientReconfigurationPacket request,
			RequestCallback callback, InetSocketAddress reconfigurator)
			throws IOException {
		if (callback != null)
			if (!hasLongTimeout(callback))
				this.callbacksCRP.put(getKey(request), callback);
			else if (this.callbacksCRPLongTimeout
					.put(getKey(request), callback) == null)
				spawnGCClientReconfigurationPacket(
						((TimeoutRequestCallback) callback).getTimeout(),
						request);
		this.niot.sendToAddress(reconfigurator != null ? reconfigurator
				: this.e2eRedirector.getNearest(reconfigurators), request
				.toString());
	}

	private InetSocketAddress getRandom(InetSocketAddress[] isas) {
		return isas != null && isas.length > 0 ? isas[(int) (Math.random() * isas.length)]
				: null;
	}

	private String getKey(ClientReconfigurationPacket crp) {
		return crp.getRequestType() + ":" + crp.getServiceName();
	}

	private String getKey(ServerReconfigurationPacket<?> changeRCs) {
		return changeRCs.getRequestType()
				+ ":"
				+ (changeRCs.hasAddedNodes() ? changeRCs.newlyAddedNodes
						.keySet() : "") + ":"
				+ (changeRCs.hasDeletedNodes() ? changeRCs.deletedNodes : "");
	}

	class RequestAndCallback implements RequestCallback {
		long sentTime = System.currentTimeMillis();
		final ClientRequest request;
		final RequestCallback callback;
		final NearestServerSelector redirector;
		InetSocketAddress serverSentTo;

		RequestAndCallback(ClientRequest request, RequestCallback callback) {
			this(request, callback, null);
		}

		RequestAndCallback(ClientRequest request, RequestCallback callback,
				NearestServerSelector redirector) {
			this.request = request;
			this.callback = callback;
			this.redirector = redirector;
		}

		@Override
		public void handleResponse(Request response) {
			this.callback.handleResponse(response);
		}

		public String toString() {
			return this.request.getSummary() + "->" + this.serverSentTo + " "
					+ (System.currentTimeMillis() - this.sentTime) + "ms back";
		}

		RequestAndCallback setServerSentTo(InetSocketAddress isa) {
			this.sentTime = System.currentTimeMillis();
			this.serverSentTo = isa;
			return this;
		}

		public boolean isExpired() {
			return System.currentTimeMillis() - this.sentTime > APP_REQUEST_TIMEOUT;
		}
	}

	/**
	 * @param request
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 */
	public Long sendRequest(ClientRequest request, RequestCallback callback)
			throws IOException {
		return this.sendRequest(request, callback, this.e2eRedirector);
	}

	private static final String ANY_ACTIVE = Config
			.getGlobalString(ReconfigurationConfig.RC.SPECIAL_NAME);

	/**
	 * @param request
	 * @param callback
	 * @return Request ID of sent or enqueued request
	 * @throws IOException
	 */
	public Long sendRequestAnycast(ClientRequest request,
			RequestCallback callback) throws IOException {
		Set<InetSocketAddress> active = null;
		if ((active = this.activeReplicas.get(ANY_ACTIVE)) != null
				&& this.queriedActivesRecently(ANY_ACTIVE))
			return this
					.sendRequest(request, active.iterator().next(), callback);
		// else
		this.enqueueAndQueryForActives(
				new RequestAndCallback(request, callback), true, true);
		return request.getRequestID();
	}

	private boolean queriedActivesRecently(String name) {
		Long lastQueriedTime = null;
		if ((lastQueriedTime = this.lastQueriedActives.get(name)) != null
				&& (System.currentTimeMillis() - lastQueriedTime < MIN_REQUEST_ACTIVES_INTERVAL)) {

			return true;
		}
		Reconfigurator.getLogger().log(Level.FINE,
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
	public Long sendRequest(ClientRequest request, RequestCallback callback,
			NearestServerSelector redirector) throws IOException {

		Set<InetSocketAddress> actives = null;
		// lookup actives in the cache first
		if ((actives = this.activeReplicas.get(request.getServiceName())) != null
				&& this.queriedActivesRecently(request.getServiceName()))
			return this.sendRequest(request,
					redirector != null ? redirector.getNearest(actives)
							: (InetSocketAddress) (Util.selectRandom(actives)),
					callback);

		// else enqueue them
		this.enqueueAndQueryForActives(new RequestAndCallback(request,
				callback, redirector), true, false);
		return request.getRequestID();
	}

	private synchronized boolean enqueueAndQueryForActives(
			RequestAndCallback rc, boolean force, boolean anycast)
			throws IOException {
		boolean queued = this.enqueue(rc, anycast);
		this.queryForActives(
				anycast ? ANY_ACTIVE : rc.request.getServiceName(), force);
		return queued;
	}

	private synchronized boolean enqueue(RequestAndCallback rc, boolean anycast) {
		this.requestsPendingActives.putIfAbsent(anycast ? ANY_ACTIVE
				: rc.request.getServiceName(),
				new LinkedBlockingQueue<RequestAndCallback>());
		LinkedBlockingQueue<RequestAndCallback> pending = this.requestsPendingActives
				.get(anycast ? ANY_ACTIVE : rc.request.getServiceName());
		assert (pending != null);
		return pending.add(rc);
	}

	private void queryForActives(String name, boolean forceRefresh)
			throws IOException {
		if (forceRefresh || !this.queriedActivesRecently(name)) {
			if (forceRefresh)
				this.activeReplicas.remove(name);
			Reconfigurator.getLogger().log(Level.FINE,
					"{0} sending actives request for ",
					new Object[] { this, name });
			this.sendRequest(new RequestActiveReplicas(name));
			this.lastQueriedActives.put(name, System.currentTimeMillis());
		}
	}

	private void sendRequestsPendingActives(RequestActiveReplicas response) {
		// learn sample latency
		this.e2eRedirector.learnSample(response.getMyReceiver(),
				System.currentTimeMillis() - response.getCreateTime());

		Set<InetSocketAddress> actives = response.getActives();
		if (actives != null && !actives.isEmpty())
			this.activeReplicas.put(response.getServiceName(), actives);
		else
			this.activeReplicas.remove(response.getServiceName());

		if (!this.requestsPendingActives.containsKey(response.getServiceName())) {
			log.log(Level.INFO,
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
						// or send error to all pending requests
						log.log(Level.INFO,
								"{0} returning {1} to pending request callbacks {2}",
								new Object[] {
										this,
										ReconfigurationPacket.PacketType.ACTIVE_REPLICA_ERROR,
										pendingRequests });
						for (RequestAndCallback pendingRequest : pendingRequests)
							pendingRequest.callback
									.handleResponse(new ActiveReplicaError(
											response.getServiceName(),
											pendingRequest.request
													.getRequestID(), response)
											.setResponseMessage(ReconfigurationPacket.PacketType.ACTIVE_REPLICA_ERROR
													+ ": No active replicas found for name \""
													+ response.getServiceName()
													+ "\" likely because the name doesn't exist or because this name or"
													+ " active replicas or reconfigurators are being reconfigured."));
						pendingRequests.clear();
						break;
					}
					reqIter.remove();
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
		long id;
		try {
			this.sendRequest(
					new RequestActiveReplicas(
							(id = (long) (Math.random() * Long.MAX_VALUE)) + ""),
					new RequestCallback() {
						@Override
						public void handleResponse(Request response) {
							// any news is good news
							success[0] = true;
							synchronized (monitor) {
								monitor.notify();
							}
						}
					},
					address != null ? address : this
							.getRandom(this.reconfigurators
									.toArray(new InetSocketAddress[0])));

			log.log(Level.INFO, "{0} sent connectivity check {1}",
					new Object[] { this, id });

			if (!success[0])
				synchronized (monitor) {
					monitor.wait(timeout);
				}
		} catch (InterruptedException | IOException e) {
			return false;
		}
		if (success[0])
			log.log(Level.INFO, "{0} connectivity check {1} successful",
					new Object[] { this, id, });
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
						.print((attempts == 1 ? "Retrying connection check..."
								: "") + attempts + " ");
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
		for (InetSocketAddress address : this.reconfigurators)
			if (this.checkConnectivity(CONNECTIVITY_CHECK_TIMEOUT, 2, address))
				return;

		throw new IOException(CONNECTION_CHECK_ERROR);
	}

	/**
	 * 
	 */
	public void close() {
		if (this.timer != null)
			this.timer.cancel();
		this.niot.stop();
	}

	/**
	 * @return The list of default servers.
	 */
	public Set<InetSocketAddress> getDefaultServers() {
		return new HashSet<InetSocketAddress>((this.reconfigurators));
	}

	public String toString() {
		return this.getClass().getSimpleName()
				+ (this.niot != null ? this.niot.getListeningSocketAddress()
						.getPort() : "");
	}

	static class AsyncClient extends ReconfigurableAppClientAsync {

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
	 * If true, the app will only be handed JSON formatter packets via
	 * getJSONRequest(), not via {@link #getRequest(String)}. If this method is
	 * invoked, the protected method {@link #getJSONRequest(JSONObject)} must be
	 * overridden.
	 * 
	 * @return {@code this}.
	 */
	@SuppressWarnings("javadoc")
	public ReconfigurableAppClientAsync enableJSONPackets() {
		this.jsonPackets = true;
		return this;
	}

	private int numOutStandingCRPs() {
		return this.callbacksCRP.size() + this.callbacksCRPLongTimeout.size();
	}

	private int numOutStandingAppRequests() {
		return this.callbacks.size() + this.callbacksLongTimeout.size();
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException {
		ReconfigurableAppClientAsync client = new AsyncClient();
		client.close();
	}
}
