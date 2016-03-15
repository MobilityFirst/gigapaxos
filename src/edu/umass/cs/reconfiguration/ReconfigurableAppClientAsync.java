package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
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
import edu.umass.cs.gigapaxos.paxosutil.E2ELatencyAwareRedirector;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
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
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public abstract class ReconfigurableAppClientAsync implements AppRequestParser {
	// could be any value coz we clear cached entry upon error or deletion
	private static final long MIN_REQUEST_ACTIVES_INTERVAL = 60000;

	private static final long DEFAULT_GC_TIMEOUT = 120000;
	private static final long SRP_GC_TIMEOUT = 120000;

	final MessageNIOTransport<String, String> niot;
	final Set<InetSocketAddress> reconfigurators;
	final SSL_MODES sslMode;
	private final E2ELatencyAwareRedirector redirector;

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
				ReconfigurableAppClientAsync.this.redirector.learnSample(
						((RequestAndCallback) value).serverSentTo,
						System.currentTimeMillis()
								- ((RequestAndCallback) value).sentTime);
			log.log(Level.INFO, "{0} timing out {1}:{2}", new Object[] { this,
					key, value });
		}
	};

	// all app client request callbacks
	final GCConcurrentHashMap<Long, RequestCallback> callbacks = new GCConcurrentHashMap<Long, RequestCallback>(
			defaultGCCallback, DEFAULT_GC_TIMEOUT);

	// client reconfiguration packet callbacks
	final GCConcurrentHashMap<String, RequestCallback> callbacksCRP = new GCConcurrentHashMap<String, RequestCallback>(
			defaultGCCallback, DEFAULT_GC_TIMEOUT);

	// server reconfiguration packet callbacks,
	final GCConcurrentHashMap<String, RequestCallback> callbacksSRP = new GCConcurrentHashMap<String, RequestCallback>(
			defaultGCCallback, SRP_GC_TIMEOUT);

	// name->actives map
	final GCConcurrentHashMap<String, Set<InetSocketAddress>> activeReplicas = new GCConcurrentHashMap<String, Set<InetSocketAddress>>(
			defaultGCCallback, DEFAULT_GC_TIMEOUT);
	// name->unsent app requests for which active replicas are not yet known
	final GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>> requestsPendingActives = new GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>>(
			defaultGCCallback, DEFAULT_GC_TIMEOUT);
	// name->last queried time to rate limit RequestActiveReplicas queries
	final GCConcurrentHashMap<String, Long> lastQueriedActives = new GCConcurrentHashMap<String, Long>(
			defaultGCCallback, DEFAULT_GC_TIMEOUT);

	protected static final Logger log = Reconfigurator.getLogger();

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
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync(Set<InetSocketAddress> reconfigurators,
			SSLDataProcessingWorker.SSL_MODES sslMode, int clientPortOffset)
			throws IOException {
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
		this.redirector = new E2ELatencyAwareRedirector(
				this.niot.getListeningSocketAddress());
	}

	/**
	 * @param reconfigurators
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync(Set<InetSocketAddress> reconfigurators)
			throws IOException {
		this(reconfigurators, ReconfigurationConfig.getClientSSLMode(),
				(ReconfigurationConfig.getClientPortOffset()));
	}

	/**
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync() throws IOException {
		this(ReconfigurationConfig.getReconfiguratorAddresses());
	}

	private static Stringifiable<?> unstringer = new StringifiableDefault<String>(
			"");

	class ClientPacketDemultiplexer extends AbstractPacketDemultiplexer<String> {

		ClientPacketDemultiplexer(Set<IntegerPacketType> types) {
			this.register(ReconfigurationPacket.clientPacketTypes);
			this.register(ReconfigurationPacket.serverPacketTypes);
			this.register(types);
		}

		private BasicReconfigurationPacket<?> parseAsReconfigurationPacket(
				String strMsg) {
			BasicReconfigurationPacket<?> rcPacket = null;
			try {
				rcPacket = ReconfigurationPacket.getReconfigurationPacket(
						new JSONObject(strMsg), unstringer);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return (rcPacket instanceof ClientReconfigurationPacket) ? (ClientReconfigurationPacket) rcPacket
					: rcPacket instanceof ServerReconfigurationPacket ? ((ServerReconfigurationPacket<?>) rcPacket)
							: null;
		}

		private Request parseAsAppRequest(String strMsg) {
			Request request = null;
			try {
				request = getRequest(strMsg);
			} catch (RequestParseException e) {
				// e.printStackTrace();
			}
			assert (request == null || request instanceof ClientRequest);
			return request;
		}

		@Override
		public boolean handleMessage(String strMsg) {
			Request response = null;
			// first try parsing as app request
			if ((response = this.parseAsAppRequest(strMsg)) == null)
				// else try parsing as ClientReconfigurationPacket
				response = parseAsReconfigurationPacket(strMsg);

			RequestCallback callback = null;
			if (response != null) {
				// execute registered callback
				if ((response instanceof ClientRequest)
						&& (callback = callbacks
								.remove(((ClientRequest) response)
										.getRequestID())) != null) {
					callback.handleResponse(((ClientRequest) response));
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
					 * back to the app. */
					callback.handleResponse(response);
				} else if (response instanceof ClientReconfigurationPacket) {
					// call create/delete app callback
					if ((callback = ReconfigurableAppClientAsync.this.callbacksCRP
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
		 * We can simply return a constant integer corresponding to either a
		 * ClientReconfigurationPacket or app request here as this method is
		 * only used to confirm the demultiplexer, which is needed only in the
		 * case of multiple chained demultiplexers, but we only have one
		 * demultiplexer in this simple client.
		 * 
		 * @param strMsg
		 * @return
		 */
		@Override
		protected Integer getPacketType(String strMsg) {
			if (SHORT_CUT_TYPE_CHECK)
				return ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME
						.getInt();
			Request request = this.parseAsAppRequest(strMsg);
			if (request == null)
				request = this.parseAsReconfigurationPacket(strMsg);
			return request != null ? request.getRequestType().getInt() : null;
		}

		@Override
		protected String getMessage(String message) {
			return message;
		}

		@Override
		protected String processHeader(String message, NIOHeader header) {
			log.log(Level.FINEST, "{0} received message from {1}", new Object[] {
					this, header.sndr });
			return message;
		}

		@Override
		protected boolean matchesType(Object message) {
			return message instanceof String;
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
		try {
			prev = this.callbacks.put(
					request.getRequestID(),
					callback = new RequestAndCallback(request, callback)
							.setServerSentTo(server));
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
				return null;
			}
		}
		return request.getRequestID();
	}

	/**
	 * @param request
	 * @param callback
	 * @throws IOException
	 */
	public void sendRequest(ClientReconfigurationPacket request,
			RequestCallback callback) throws IOException {
		// assert (request.getServiceName() != null);
		// overwrite the most recent callback
		this.callbacksCRP.put(getKey(request), callback);
		this.sendRequest(request);
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

	private void sendRequest(ClientReconfigurationPacket request)
			throws IOException {
		InetSocketAddress dest = this.redirector
				.getNearest(this.reconfigurators);// getRandom(this.reconfigurators);
		this.niot.sendToAddress(dest, request.toString());
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
	}

	/**
	 * @param request
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 */
	public Long sendRequest(ClientRequest request, RequestCallback callback)
			throws IOException {
		return this.sendRequest(request, callback, this.redirector);
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
		this.redirector.learnSample(response.getMyReceiver(),
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
					if (actives != null && !actives.isEmpty())
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
					else {
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
	 * 
	 */
	public void close() {
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
}
