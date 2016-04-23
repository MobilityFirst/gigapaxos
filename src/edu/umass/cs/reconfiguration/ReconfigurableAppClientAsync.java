package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
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
import edu.umass.cs.reconfiguration.reconfigurationpackets.EchoRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureActiveNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureRCNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ServerReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
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
	static {
		ReconfigurationConfig.load();
	}
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
		@Override
		public void callbackGC(Object key, Object value) {
			assert (value instanceof RequestAndCallback);
			ReconfigurableAppClientAsync.this.e2eRedirector.learnSample(
					((RequestAndCallback) value).serverSentTo,
					System.currentTimeMillis()
							- ((RequestAndCallback) value).sentTime);
			ReconfigurableAppClientAsync.this.mostRecentlyWrittenMap
					.remove(((RequestAndCallback) value).request
							.getServiceName());
			log.log(Level.INFO, "{0} timing out {1}:{2}", new Object[] { this,
					key, value });
		}
	};

	final GCConcurrentHashMapCallback crpGCCallback = new GCConcurrentHashMapCallback() {
		@Override
		public void callbackGC(Object key, Object value) {
			assert (value instanceof CRPRequestCallback);
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

	// all app client request callbacks
	private final GCConcurrentHashMap<Long, RequestCallback> callbacks = new GCConcurrentHashMap<Long, RequestCallback>(
			appGCCallback, APP_REQUEST_TIMEOUT);
	private final GCConcurrentHashMap<Long, RequestCallback> callbacksLongTimeout = new GCConcurrentHashMap<Long, RequestCallback>(
			defaultGCCallback, APP_REQUEST_LONG_TIMEOUT);

	// client reconfiguration packet callbacks
	private final GCConcurrentHashMap<String, RequestCallback> callbacksCRP = new GCConcurrentHashMap<String, RequestCallback>(
			crpGCCallback, CRP_GC_TIMEOUT);
	private final GCConcurrentHashMap<String, RequestCallback> callbacksCRPLongTimeout = new GCConcurrentHashMap<String, RequestCallback>(
			crpGCCallback, CRP_GC_TIMEOUT);

	// server reconfiguration packet callbacks,
	private final GCConcurrentHashMap<String, RequestCallback> callbacksSRP = new GCConcurrentHashMap<String, RequestCallback>(
			crpGCCallback, SRP_GC_TIMEOUT);

	// name->actives map
	private final GCConcurrentHashMap<String, ActivesInfo> activeReplicas = new GCConcurrentHashMap<String, ActivesInfo>(
			defaultGCCallback, MIN_REQUEST_ACTIVES_INTERVAL);
	// name->unsent app requests for which active replicas are not yet known
	private final GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>> requestsPendingActives = new GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>>(
			defaultGCCallback, CRP_GC_TIMEOUT); // FIXME: long timeout version

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

	private static final Logger log = Reconfigurator.getLogger();

	private static final int MAX_OUTSTANDING_APP_REQUESTS = 4096;

	private static final int MAX_OUTSTANDING_CRP_REQUESTS = 4096;

	class ActivesInfo {
		Set<InetSocketAddress> actives = null;
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
		(this.niot = (new MessageNIOTransport<String, String>(null, null,
				(new ClientPacketDemultiplexer(getRequestTypes())), true,
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
		this(reconfigurators, sslMode, clientPortOffset, true);
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

	private static Stringifiable<?> unstringer = new StringifiableDefault<String>(
			"");

	class ClientPacketDemultiplexer extends AbstractPacketDemultiplexer<Object> {

		private Logger log = ReconfigurableAppClientAsync.log;

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
							: rcPacket instanceof EchoRequest ? ((EchoRequest) rcPacket)
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
				ReconfigurableAppClientAsync.this.mostRecentlyWrittenMap.put(
						request.getServiceName(), mostRecentReplica);
				log.log(Level.FINE,
						"{0} most recently received a commit for a coordinated request [{1}] from replica {2} [{3}ms]",
						new Object[] {
								this,
								request.getSummary(),
								mostRecentReplica,
								(System.currentTimeMillis() - callback.sentTime) });
			}
			log.log(Level.FINEST,
					"{0} updated latency map with sample {1}:{2} to {3}",
					new Object[] { this, callback.serverSentTo,
							System.currentTimeMillis() - callback.sentTime,
							ReconfigurableAppClientAsync.this.e2eRedirector });
			ReconfigurableAppClientAsync.this.e2eRedirector.learnSample(
					callback.serverSentTo, System.currentTimeMillis()
							- callback.sentTime);
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
					updateBestReplica((RequestAndCallback) callback);
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
					ReconfigurableAppClientAsync.this.mostRecentlyWrittenMap
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

					// remember reconfigurator that confirmed creation
					if (response instanceof CreateServiceName
							&& !((CreateServiceName) response).isFailed())
						ReconfigurableAppClientAsync.this.mostRecentlyCreatedMap
								.put(response.getServiceName(),
										((ClientReconfigurationPacket) response)
												.getSender());

				} else if (response instanceof ServerReconfigurationPacket<?>) {
					if ((callback = ReconfigurableAppClientAsync.this.callbacksSRP
							.remove(getKey((ServerReconfigurationPacket<?>) response))) != null) {
						callback.handleResponse(response);
					}
				} else if (response instanceof EchoRequest) {
					if ((callback = ReconfigurableAppClientAsync.this.callbacks
							.remove((EchoRequest) response)) != null) {
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
		protected Object processHeader(byte[] bytes, NIOHeader header) {
			log.log(Level.FINEST, "{0} received message from {1}",
					new Object[] { this, header.sndr });
			String message = null;
			try {
				message = MessageExtractor.decode(bytes);
				long t = System.nanoTime();
				/* FIXME: This is inefficient and inelegant, but it is unclear
				 * what else we can do to avoid at least one unnecessary
				 * JSON<->string back and forth with the current NIO API and the
				 * fact that apps in general may not use JSON for their packets
				 * but we do. The alternative is to not provide the
				 * sender-address feature to async clients; if so, we don't need
				 * the JSON'ization below. */
				if (JSONPacket.couldBeJSON(message)) {
					JSONObject json = new JSONObject(message);
					MessageExtractor.stampAddressIntoJSONObject(header.sndr,
							header.rcvr, json);
					if (ReconfigurationConfig.instrument(100))
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

	private static final boolean READ_YOUR_WRITES = Config
			.getGlobalBoolean(PC.READ_YOUR_WRITES);

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
		InetSocketAddress mostRecentlyWritten = READ_YOUR_WRITES ? this.mostRecentlyWrittenMap
				.get(request.getServiceName()) : null;
		if (mostRecentlyWritten != null && !mostRecentlyWritten.equals(server)) {
			log.log(Level.FINE,
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
			Level level = request instanceof EchoRequest ? Level.FINE
					: Level.INFO;
			log.log(level,
					"{0} sent request {1} to server {2}",
					new Object[] {
							this,
							ReconfigurationConfig.getSummary(request,
									log.isLoggable(level)), server });
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
		InetSocketAddress server = null;
		this.sendRequest(request,
				callback,
				// to avoid lagging reconfigurators
				(server = this.mostRecentlyCreatedMap.get(request
						.getServiceName())) != null ? server
				// could also select random reconfigurator here
						: this.e2eRedirector.getNearest(this.reconfigurators));
	}

	private void sendRequest(ClientReconfigurationPacket request)
			throws IOException {
		this.sendRequest(request, null);
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
		if (callback != null
				&& (callback = new CRPRequestCallback(request, callback,
						reconfigurator)) != null)
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

	class CRPRequestCallback implements RequestCallback {
		long sentTime = System.currentTimeMillis();
		final ClientReconfigurationPacket request;
		final RequestCallback callback;
		final InetSocketAddress serverSentTo;

		CRPRequestCallback(ClientReconfigurationPacket request,
				RequestCallback callback, InetSocketAddress serverSentTo) {
			this.request = request;
			this.callback = callback;
			this.serverSentTo = serverSentTo;
		}

		@Override
		public void handleResponse(Request response) {
			this.callback.handleResponse(response);
		}
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

		RequestCallback setServerSentTo(InetSocketAddress isa) {
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
		Set<InetSocketAddress> actives = null;
		ActivesInfo activesInfo = null;
		if ((activesInfo = this.activeReplicas.get(ANY_ACTIVE)) != null
				&& (actives = activesInfo.actives) != null
				&& this.queriedActivesRecently(ANY_ACTIVE))
			return this.sendRequest(request, actives.iterator().next(),
					callback);
		// else
		this.enqueueAndQueryForActives(
				new RequestAndCallback(request, callback), true, true);
		return request.getRequestID();
	}

	private boolean queriedActivesRecently(String name) {
		Long lastQueriedTime = null;
		ActivesInfo activesInfo = null;
		if ((activesInfo = this.activeReplicas.get(name)) != null
				&& (lastQueriedTime = activesInfo.createTime) != null
				&& (System.currentTimeMillis() - lastQueriedTime < MIN_REQUEST_ACTIVES_INTERVAL)) {

			return true;
		}
		if (lastQueriedTime != null)
			Reconfigurator.getLogger().log(Level.FINEST,
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
		ActivesInfo activesInfo = null;
		// lookup actives in the cache first
		if ((activesInfo = this.activeReplicas.get(request.getServiceName())) != null
				&& (actives = activesInfo.actives) != null
				&& this.queriedActivesRecently(request.getServiceName()))
			return this.sendRequest(request,
					redirector != null ? redirector.getNearest(actives)
							: (InetSocketAddress) (Util.selectRandom(actives)),
					callback);

		// else enqueue them
		this.enqueueAndQueryForActives(new RequestAndCallback(request,
				callback, redirector), false, false);
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
			if (forceRefresh) {
				this.activeReplicas.remove(name);
				this.mostRecentlyWrittenMap.remove(name);
			}
			Reconfigurator.getLogger().log(Level.FINE,
					"{0} requesting active replicas for {1}",
					new Object[] { this, name });
			this.sendRequest(new RequestActiveReplicas(name));
			// this.lastQueriedActives.put(name, System.currentTimeMillis());
			this.activeReplicas.put(name,
					new ActivesInfo(null, System.currentTimeMillis()));
		}
	}

	private void sendRequestsPendingActives(RequestActiveReplicas response) {
		// learn sample latency
		this.e2eRedirector.learnSample(response.getSender(),
				System.currentTimeMillis() - response.getCreateTime());

		Set<InetSocketAddress> actives = response.getActives();
		if (actives != null && !actives.isEmpty()) {
			this.activeReplicas.put(response.getServiceName(), new ActivesInfo(
					actives, response.getCreateTime()));
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
						log.log(Level.FINE,
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
			log.log(Level.INFO, "{0} sending closest-{1} map {2}",
					new Object[] { this, this.closest.size(), this.closest });
			for (InetSocketAddress address : actives) {
				try {
					this.sendRequest(new EchoRequest((InetSocketAddress) null,
							this.closest), address, new RequestCallback() {
						@Override
						public void handleResponse(Request response) {
							// do nothing
						}
					});
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

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
						address, new RequestCallback() {
							@Override
							public void handleResponse(Request response) {
								log.log(Level.INFO,
										"{0} received response {1} for echo request",
										new Object[] {
												ReconfigurableAppClientAsync.this,
												response.getSummary() });
								assert (response instanceof EchoRequest);
								updateClosest((EchoRequest) response);
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
					},
					address != null ? address : this
							.getRandom(this.reconfigurators
									.toArray(new InetSocketAddress[0])));

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
	 * If true, the app will only be handed JSON formatted packets via
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
