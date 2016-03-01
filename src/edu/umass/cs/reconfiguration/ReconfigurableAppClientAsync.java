package edu.umass.cs.reconfiguration;

import java.io.IOException;
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

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.NearestServerSelector;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.interfaces.SummarizableRequest;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
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
	private static final long MIN_RTX_INTERVAL = 1000;
	private static final long GC_TIMEOUT = 120000;

	final MessageNIOTransport<String, String> niot;
	final InetSocketAddress[] reconfigurators;

	final GCConcurrentHashMapCallback defaultGCCallback = new GCConcurrentHashMapCallback() {
		@Override
		public void callbackGC(Object key, Object value) {
			Reconfigurator.getLogger().info(
					this + " garbage-collecting " + key + ":" + value);
		}
	};

	final GCConcurrentHashMap<Long, RequestCallback> callbacks = new GCConcurrentHashMap<Long, RequestCallback>(
			defaultGCCallback, GC_TIMEOUT);

	final GCConcurrentHashMap<String, RequestCallback> callbacksCRP = new GCConcurrentHashMap<String, RequestCallback>(
			defaultGCCallback, GC_TIMEOUT);

	// name->actives map
	final GCConcurrentHashMap<String, Set<InetSocketAddress>> activeReplicas = new GCConcurrentHashMap<String, Set<InetSocketAddress>>(
			defaultGCCallback, GC_TIMEOUT);
	// name->unsent app requests for which active replicas are not yet known
	final GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>> requestsPendingActives = new GCConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>>(
			defaultGCCallback, GC_TIMEOUT);
	// name->last queried time to rate limit RequestActiveReplicas queries
	final GCConcurrentHashMap<String, Long> lastQueriedActives = new GCConcurrentHashMap<String, Long>(
			defaultGCCallback, GC_TIMEOUT);

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
		Reconfigurator.getLogger().fine(this + " ssl mode " + sslMode);
		Reconfigurator.getLogger().fine(
				this + " client port offset " + clientPortOffset);
		this.niot = (new MessageNIOTransport<String, String>(null, null,
				(new ClientPacketDemultiplexer(getRequestTypes())), true,
				// This will be set in the gigapaxos.properties file that we
				// invoke the client using.
				sslMode));
		Reconfigurator.getLogger().fine(
				this + " listening on " + niot.getListeningSocketAddress());
		this.reconfigurators = ReconfigurationConfig.offsetSocketAddresses(
				reconfigurators, clientPortOffset);

		Reconfigurator.getLogger().fine(
				this + " reconfigurators "
						+ Arrays.toString(this.reconfigurators));
	}

	/**
	 * @param reconfigurators
	 * @throws IOException
	 */
	public ReconfigurableAppClientAsync(Set<InetSocketAddress> reconfigurators)
			throws IOException {
		this(
				reconfigurators,
				SSLDataProcessingWorker.SSL_MODES.valueOf(Config.getGlobal(
						ReconfigurationConfig.RC.CLIENT_SSL_MODE).toString()),
				Config.getGlobalInt(ReconfigurationConfig.RC.CLIENT_PORT_OFFSET));
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
			ReconfigurationPacket<?> rcPacket = null;
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
                        Reconfigurator.getLogger().fine("Handling " +  strMsg);
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
					/*
					 * auto-retransmitting can cause an infinite loop, so we
					 * just throw the ball back to the app.
					 */
					callback.handleResponse(response);
				} else if (response instanceof ClientReconfigurationPacket) {

					if ((callback = ReconfigurableAppClientAsync.this.callbacksCRP
							.remove(getKey((ClientReconfigurationPacket) response))) != null) {						
						callback.handleResponse(response);
					} 
					// if RequestActiveReplicas, send pending requests or unpend them
					if (response instanceof RequestActiveReplicas)
						try {
							ReconfigurableAppClientAsync.this
									.sendRequestsPendingActives((RequestActiveReplicas) response);
						} catch (IOException e) {
							e.printStackTrace();
						}
				} else if (response instanceof ServerReconfigurationPacket<?>) {
					if ((callback = ReconfigurableAppClientAsync.this.callbacksCRP
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
			return message;
		}

		@Override
		protected boolean matchesType(Object message) {
			return message instanceof String;
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
			prev = this.callbacks.put(request.getRequestID(),
					callback = new RequestAndCallback(request, callback));
                        Reconfigurator.getLogger().fine("Request: " +  request.toString());
			sendFailed = this.niot.sendToAddress(server, request.toString()) <= 0;
			log.log(Level.FINE,
					"{0} sent request {1} to server {2}; [{3}]",
					new Object[] {
							this,
							ReconfigurationConfig.getSummary(request,
									log.isLoggable(Level.FINE)), server,
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
		assert (request.getServiceName() != null);
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

	private static final int CLIENT_PORT_OFFSET = Config
			.getGlobalInt(RC.CLIENT_PORT_OFFSET);

	/**
	 * @param rcPacket
	 * @param callback
	 * @throws IOException
	 */
	public void sendServerReconfigurationRequest(
			ServerReconfigurationPacket<?> rcPacket, RequestCallback callback)
			throws IOException {
		InetSocketAddress isa = getRandom(this.reconfigurators);
		// overwrite the most recent callback
		this.callbacksCRP.put(getKey(rcPacket), callback);
		this.niot.sendToAddress(Util.offsetPort(isa, -CLIENT_PORT_OFFSET),
				rcPacket.toString());
	}

	private void sendRequest(ClientReconfigurationPacket request)
			throws IOException {
		InetSocketAddress dest = getRandom(this.reconfigurators);
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
		final ClientRequest request;
		final RequestCallback callback;

		RequestAndCallback(ClientRequest request, RequestCallback callback) {
			this.request = request;
			this.callback = callback;
		}

		@Override
		public void handleResponse(Request response) {
			this.callback.handleResponse(response);
		}
		public String toString() {
			return this.request instanceof SummarizableRequest ? ((SummarizableRequest) this.request)
					.getSummary() : this.request.getServiceName()+":"+this.request.getRequestID();
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
		return this.sendRequest(request, callback, null);
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
		if (request instanceof ClientReconfigurationPacket)
			return this
					.sendRequest(
							request,
							reconfigurators[(int) (Math.random() * this.reconfigurators.length)],
							callback);

		// lookup actives in the cache first
		if (this.activeReplicas.containsKey(request.getServiceName())) {
			Set<InetSocketAddress> actives = this.activeReplicas.get(request
					.getServiceName());
			return this.sendRequest(request,
					redirector != null ? redirector.getNearest(actives)
							: (InetSocketAddress) (Util.selectRandom(actives)),
					callback);
		}
		// else enqueue them
		this.enqueueAndQueryForActives(
				new RequestAndCallback(request, callback), false);
		return request.getRequestID();
	}

	private synchronized boolean enqueueAndQueryForActives(
			RequestAndCallback rc, boolean force) throws IOException {
		boolean queued = this.enqueue(rc);
		this.queryForActives(rc.request.getServiceName(), force);
		return queued;
	}

	private synchronized boolean enqueue(RequestAndCallback rc) {
		// if(!this.requestsPendingActives.containsKey(rc.request.getServiceName()))
		this.requestsPendingActives.putIfAbsent(rc.request.getServiceName(),
				new LinkedBlockingQueue<RequestAndCallback>());
		LinkedBlockingQueue<RequestAndCallback> pending = this.requestsPendingActives
				.get(rc.request.getServiceName());
		assert (pending != null);
		return pending.add(rc);
	}

	private void queryForActives(String name, boolean forceRefresh)
			throws IOException {
		Long lastQueriedTime = this.lastQueriedActives.get(name);
		if (lastQueriedTime == null)
			lastQueriedTime = 0L;
		Reconfigurator.getLogger().fine(
				this + " last quiried time for " + name + " is "
						+ lastQueriedTime);
		if (System.currentTimeMillis() - lastQueriedTime > MIN_RTX_INTERVAL
				|| forceRefresh) {
			if (forceRefresh)
				this.activeReplicas.remove(name);
			Reconfigurator.getLogger().fine(
					this + " sending actives request for " + name);
			this.sendRequest(new RequestActiveReplicas(name));
			this.lastQueriedActives.put(name, System.currentTimeMillis());
		}
	}

	// FIXME: untested
	private void sendRequestsPendingActives(RequestActiveReplicas response)
			throws IOException {
		Set<InetSocketAddress> actives = response.getActives();

		if (actives != null)
			this.activeReplicas.put(response.getServiceName(), actives);
		if (this.requestsPendingActives.containsKey(response.getServiceName())) {
			log.log(Level.FINE,
					"{0} trying to release requests pending actives for {1} : {2}",
					new Object[] {
							this,
							response.getSummary(),
							Util.truncatedLog(this.requestsPendingActives
									.get(response.getServiceName()), 8) });
			for (Iterator<RequestAndCallback> reqIter = this.requestsPendingActives
					.get(response.getServiceName()).iterator(); reqIter
					.hasNext();) {
				RequestAndCallback rc = reqIter.next();
				if (actives != null && !actives.isEmpty())
					this.sendRequest(rc.request,
							(InetSocketAddress) (Util.selectRandom(actives)),
							rc.callback);
				else {
					// send error to all pending requests
					LinkedBlockingQueue<RequestAndCallback> pendingRequests = this.requestsPendingActives
							.remove(response.getServiceName());
					if (pendingRequests != null) {
						for (RequestAndCallback pendingRequest : pendingRequests)
							pendingRequest.callback
									.handleResponse(new ActiveReplicaError(
											null, response.getServiceName(),
											pendingRequest.request
													.getRequestID())
											.setResponseMessage("No active replicas found for name "
													+ response.getServiceName()
													+ " likely because the name doesn't exist or (less likely) because this name or  "
													+ " active replicas or reconfigurators are being reconfigured."));
						pendingRequests.clear();
					}
				}
				reqIter.remove();
			}
		} else 
			log.log(Level.INFO,
					"{0} found no requests pending actives for {1}",
					new Object[] { this, response.getSummary() });
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
		return new HashSet<InetSocketAddress>(
				Arrays.asList(this.reconfigurators));
	}

	public String toString() {
		return this.getClass().getSimpleName();
	}
}
