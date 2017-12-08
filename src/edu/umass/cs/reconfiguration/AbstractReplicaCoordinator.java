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
package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.GigapaxosShutdownable;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorCallback;
import edu.umass.cs.reconfiguration.interfaces.ReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.interfaces.Repliconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DefaultAppRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.reconfiguration.reconfigurationutils.TrivialRepliconfigurable;

/**
 * @author V. Arun
 * @param <NodeIDType>
 *            <p>
 *            This abstract class should be inherited by replica coordination
 *            protocols. The only abstract method the inheritor needs to
 *            implement is coordinateRequest(.). For example, this method could
 *            lazily propagate the request to all replicas; or it could ensure
 *            reliable receipt from a threshold number of replicas for
 *            durability, and so on.
 * 
 *            Notes:
 * 
 *            In general, coordinateRequest(.) is app-specific logic. But most
 *            common cases, e.g., linearizability, sequential, causal, eventual,
 *            etc. can be implemented agnostic to app-specific details with
 *            supporting libraries that we can provide.
 * 
 *            A request is not an entity that definitively does or does not need
 *            coordination. A request, e.g., a read request, may or may not need
 *            coordination depending on the replica coordination protocol, which
 *            is why it is here.
 * 
 */
public abstract class AbstractReplicaCoordinator<NodeIDType> implements
		Repliconfigurable, ReplicaCoordinator<NodeIDType>,
		AppRequestParserBytes {
	protected final Repliconfigurable app;
	private final ConcurrentHashMap<IntegerPacketType, Boolean> coordinationTypes = new ConcurrentHashMap<IntegerPacketType, Boolean>();

	private ReconfiguratorCallback callback = null;
	private ReconfiguratorCallback stopCallback = null; // for stops
	private boolean largeCheckpoints = false;
	protected Messenger<NodeIDType, ?> messenger;

	/*********************
	 * Start of abstract methods
	 * 
	 * @param request
	 * @param callback
	 * @return Success of coordination.
	 * @throws IOException
	 * @throws RequestParseException
	 **********************************************/
	/* This method performs whatever replica coordination action is necessary to
	 * handle the request. */
	public abstract boolean coordinateRequest(Request request,
			ExecutedCallback callback) throws IOException,
			RequestParseException;

	/* This method should return true if the replica group is successfully
	 * created or one already exists with the same set of nodes. It should
	 * return false otherwise. */
	public abstract boolean createReplicaGroup(String serviceName, int epoch,
			String state, Set<NodeIDType> nodes);

	/* This method should result in all state corresponding to serviceName being
	 * deleted. It is meant to be called only after a replica group has been
	 * stopped by committing a stop request in a coordinated manner. */
	public abstract boolean deleteReplicaGroup(String serviceName, int epoch);

	/* This method must return the replica group that was most recently
	 * successfully created for the serviceName using createReplicaGroup. */
	public abstract Set<NodeIDType> getReplicaGroup(String serviceName);

	/********************* End of abstract methods ***********************************************/

	private static ConcurrentMap<Replicable, AbstractReplicaCoordinator<?>> appCoordMap = new ConcurrentHashMap<Replicable, AbstractReplicaCoordinator<?>>();

	// A replica coordinator is meaningless without an underlying app
	protected AbstractReplicaCoordinator(Replicable app) {
		appCoordMap.putIfAbsent(app, this);
		this.app = app instanceof AbstractReplicaCoordinator ? ((AbstractReplicaCoordinator<?>) app).app
				: new TrivialRepliconfigurable(app);
		this.messenger = null;
	}

	
	/**
	 * @param app
	 * @param messenger
	 */
	public AbstractReplicaCoordinator(Replicable app,
			Messenger<NodeIDType, ?> messenger) {
		this(app);
		this.messenger = messenger;
	}
	
	private Set<IntegerPacketType> cachedAppCoordTypes = null;
	
	// temporary state holders during recovery
	private String arar;
	private String arrc;
	/**
	 * @return Request types for coordination requests. By default, it is 
	 * the {@link #getRequestTypes()} - {@link #getAppRequestTypes()}. This
	 * method must be overridden for non-default behavior.
	 */
	public Set<IntegerPacketType> getCoordinatorRequestTypes() {
		if(cachedAppCoordTypes!=null) return cachedAppCoordTypes;
		Set<IntegerPacketType> types = this.getRequestTypes();
		if(types==null) types = new HashSet<IntegerPacketType>();
		types.removeAll(this.getAppRequestTypes());
		return cachedAppCoordTypes=types;
	}

	protected void setMessenger(Messenger<NodeIDType, ?> messenger) {
		this.messenger = messenger;
	}

	protected Messenger<NodeIDType, ?> getMessenger() {
		return this.messenger;
	}

	protected void registerCoordination(IntegerPacketType... types) {
		for (IntegerPacketType type : types)
			this.coordinationTypes.put(type, true);
	}

	/**
	 * This "default" callback will be called after every request execution. If
	 * there is a request-specific callback also specified, this callback will
	 * be called before the request-specific callback.
	 * 
	 * @param callback
	 * @return {@code this}
	 */
	protected final AbstractReplicaCoordinator<NodeIDType> setCallback(
			ReconfiguratorCallback callback) {
		/**
		 * The correctness of Reconfigurator relies on the following as
		 * Reconfigurator sets the callback before its
		 * getReconfigurableReconfiguratorAsActiveReplica.
		 */
		if (this.callback == null)
			this.callback = callback;
		return this;
	}

	protected final ReconfiguratorCallback getCallback() {
		return this.callback;
	}

	/**
	 * Used only at active replicas.
	 * 
	 * @param callback
	 * @return {@code this}
	 */
	protected final AbstractReplicaCoordinator<NodeIDType> setStopCallback(
			ReconfiguratorCallback callback) {
		this.stopCallback = callback;
		return this;
	}

	private Request unwrapIfNeeded(Request request) {
		return request instanceof ReplicableClientRequest
				&& !
				// cache-optimized request types
				(myRequestTypes != null ? myRequestTypes
						: (myRequestTypes = this.getRequestTypes()))
						.contains(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST) ? ((ReplicableClientRequest) request)
				.getRequest() : request;

	}

	private static Set<IntegerPacketType> myRequestTypes = null;

	/**
	 * Coordinate if needed, else hand over to app.
	 * 
	 * @param request
	 * @return True if coordinated successfully or handled successfully
	 *         (locally), false otherwise.
	 */
	@SuppressWarnings("deprecation")
	// only for backwards compatibility
	protected boolean handleIncoming(Request request, ExecutedCallback callback) {
		boolean handled = false;
		// check if coordination on request before unwrapping

		if (needsCoordination(request)) {
			try {
				if (request instanceof ReplicableRequest)
					((ReplicableRequest) request).setNeedsCoordination(false);
				handled = coordinateRequest(unwrapIfNeeded(request), callback);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} catch (RequestParseException rpe) {
				rpe.printStackTrace();
			}
		} else {
			handled = this.execute(unwrapIfNeeded(request), callback);
		}
		return handled;
	}

	@Override
	public boolean execute(Request request) {
		return this.execute(request, null);
	}

	@Override
	public boolean execute(Request request, boolean noReplyToClient) {
		return this.execute(request, noReplyToClient, null);
	}

	private boolean execute(Request request, ExecutedCallback callback) {
		return this.execute(request, false, callback);
	}

	/** This method is a wrapper for Application.handleRequest and meant to be
	 * invoked by the class that implements this AbstractReplicaCoordinator or
	 * its helper classes.
	 * 
	 * We need control over this method in order to call the callback after the
	 * app's handleRequest method has been executed. An alternative would have
	 * been to enforce the callback as part of the Reconfigurable interface.
	 * However, this is a less preferred design because it depends more on the
	 * app's support for stop requests even though a stop request is really
	 * meaningless to an app.
	 * 
	 * Should we add support for response messaging here using the
	 * ClientMessgenser and ClientRequest interfaces similar to that in
	 * gigapaxos? No, because response messaging details are specific to the
	 * coordination protocol. 
	 * @param request 
	 * @param noReplyToClient 
	 * @param requestCallback 
	 * @return True 
	 * 
	 */
	public boolean execute(Request request, boolean noReplyToClient,
			ExecutedCallback requestCallback) {

		if (this.callback != null && this.callback.preExecuted(request))
			// no further execution
			return true;
		
		boolean handled = request.getRequestType()==ReconfigurationPacket.PacketType.NO_TYPE ||
				(((this.app instanceof Replicable) ? ((Replicable) (this.app))
				.execute(request, noReplyToClient) : this.app.execute(request)));
		callCallback(request, handled, requestCallback);
		/* We always return true because the return value here is a no-op. It
		 * might as well be void. Returning anything but true will ensure that a
		 * paxos coordinator will get stuck on this request forever. The app can
		 * still convey false if needed to the caller via the callback. */
		return true;
	}

	public final Request getRequest(String stringified)
			throws RequestParseException {
		if (JSONPacket.couldBeJSON(stringified)) {
			boolean internal = false;
			try {
				JSONObject json = new JSONObject(stringified);
				Integer type =  JSONPacket.getPacketType(json);
				if((type == null ||  type == ReconfigurationPacket.PacketType.NO_TYPE
						.getInt()) && (internal=true)) 
					// used by default stop
					return new DefaultAppRequest(json); 
				else if ((type == ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST
						.getInt()) && (internal=true))
					return new ReplicableClientRequest(json, null);
			} catch (JSONException | UnsupportedEncodingException e) {
				if(internal) throw new RequestParseException(e);
				// else ignore and treat as app request
			}
		}
		Request request = null;
		if(this.parser!=null && (request = this.parser.getRequest(stringified))
				!=null)
			return request;
		// else
		return this.app.getRequest(stringified);
	}

	private AppRequestParser parser=null;
	private AppRequestParserBytes parserBytes=null;

	public void setGetRequestImpl(AppRequestParser parser) {
		this.parser = parser;
	}
	public void setGetRequestImpl(AppRequestParserBytes parserBytes) {
		this.parserBytes = parserBytes;
	}

	public final Request getRequest(byte[] bytes, NIOHeader header)
			throws RequestParseException {
		try {
			return ByteBuffer.wrap(bytes).getInt() == ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST
					.getInt() ? (this.app instanceof AppRequestParserBytes ? new ReplicableClientRequest(
					bytes, header, (AppRequestParserBytes) this.app)
					: new ReplicableClientRequest(bytes,
							(AppRequestParser) this.app))

					: this.app instanceof AppRequestParserBytes ? ((AppRequestParserBytes) this.app)
							.getRequest(bytes, header) : this.app
							.getRequest(new String(bytes, NIOHeader.CHARSET));
		} catch (UnsupportedEncodingException e) {
			throw new RequestParseException(e);
		}
	}

	protected final Request getRequest(ReplicableClientRequest rcr,
			NIOHeader header) throws RequestParseException {
		try {
			return this.app instanceof AppRequestParserBytes ? rcr.getRequest(
					(AppRequestParserBytes) this.app, header) : rcr
					.getRequest(this.app);
		} catch (UnsupportedEncodingException e) {
			throw new RequestParseException(e);
		}
	}

	/**
	 * Need to return just app's request types. Coordination packets can go
	 * directly to the coordination module and don't have to be known to
	 * ActiveReplica.
	 * 
	 * @return Set of request types that the app is designed to handle.
	 */
	public Set<IntegerPacketType> getAppRequestTypes() {
		return this.app.getRequestTypes();
	}

	@Override
	public ReconfigurableRequest getStopRequest(String name, int epoch) {
		return this.app.getStopRequest(name, epoch);
	}

	@Override
	public String getFinalState(String name, int epoch) {
		return this.app.getFinalState(name, epoch);
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		this.app.putInitialState(name, epoch, state);
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		return this.app.deleteFinalState(name, epoch);
	}

	@Override
	public Integer getEpoch(String name) {
		return this.app.getEpoch(name);
	}

	@Override
	public String checkpoint(String name) {
		String state = null;
		return this.stopCallback != null
				&& (state = this.stopCallback.preCheckpoint(name)) != null ? state
				: app.checkpoint(name);
	}

	@Override
	public boolean restore(String name, String state) {
		return this.stopCallback != null
				&& this.stopCallback.preRestore(name, state) ? true

		/* Will be a no-op except during recovery when stopCallback will be null
		 * as it wouldn't yet have been set. */
		: this.preRestore(name, state) ? true

		: app.restore(name, state);
	}

	/* Call back active replica for stop requests, else call default callback.
	 * Should really be private, but sometimes we may need to trigger a callback
	 * for an older request. */
	protected final void callCallback(Request request, boolean handled,
			ExecutedCallback requestCallback) {
		if (this.stopCallback != null
				&& request instanceof ReconfigurableRequest
				&& ((ReconfigurableRequest) request).isStop()) {
			// no longer used (by ActiveReplica)
			this.stopCallback.executed(request, handled);
		} else if (requestCallback != null)
			// request-specific callback
			requestCallback.executed(request, handled);
		else if (this.callback != null)
			// used by reconfigurator
			this.callback.executed(request, handled);
	}

	/*********************** Start of private helper methods **********************/

	private boolean needsCoordination(Request request) {
		if (request instanceof ReplicableRequest
				&& ((ReplicableRequest) request).needsCoordination()) {
			return true;
		}
		/* No need for setNeedsCoordination as a request will necessarily get
		 * converted to a proposal or accept when coordinated, so there is no
		 * need to worry about inifinite looping. */
		else if (request instanceof RequestPacket && request.getRequestType()==PaxosPacket.PaxosPacketType.REQUEST)
			return true;
		return false;
	}

	/**
	 * @return My node ID.
	 */
	public NodeIDType getMyID() {
		assert (this.messenger != null);
		return this.messenger.getMyID();
	}

	/**
	 * @return True if large checkpoints are enabled.
	 */
	public boolean hasLargeCheckpoints() {
		return this.largeCheckpoints;
	}

	/**
	 * Enables large checkpoints. Large checkpoints means that the file system
	 * will be used to store or retrieve remote checkpoints. This is the only
	 * way to do checkpointing if the checkpoint state size exceeds the amount
	 * of available memory.
	 */
	public void setLargeCheckpoints() {
		this.largeCheckpoints = true;
	}

	/**
	 * Default implementation that can be overridden for more batching
	 * optimization.
	 * 
	 * @param nameStates
	 * @param nodes
	 * @return True if all groups successfully created.
	 */
	public boolean createReplicaGroup(Map<String, String> nameStates,
			Set<NodeIDType> nodes) {
		boolean created = true;
		for (String name : nameStates.keySet()) {
			created = created
					&& this.createReplicaGroup(name, 0, nameStates.get(name),
							nodes);
		}
		return created;
	}

	/*********************** End of private helper methods ************************/

	public void stop() {
		this.messenger.stop();
		if(this.app instanceof GigapaxosShutdownable)
			((GigapaxosShutdownable)this.app).shutdown();
	}

	/********************** Request propagation helper methods ******************/
	/* A simple utility method for lazy propagation, a simplistic coordination
	 * protocol. This is the only place where this class uses Messenger. */
	protected void sendAllLazy(Request request) throws IOException,
			RequestParseException, JSONException {
		assert (request.getServiceName() != null);
		assert (this.getReplicaGroup(request.getServiceName()) != null) : "ARC"
				+ getMyID() + " has no group for " + request.getServiceName();
		GenericMessagingTask<NodeIDType, Object> mtask = new GenericMessagingTask<NodeIDType, Object>(
				this.getReplicaGroup(request.getServiceName()).toArray(),
				(request.toString()));
		if (this.messenger == null)
			return;
		this.messenger.send(mtask);
	}

	/**
	 * @return Refer {@link AppRequestParser#getMutualAuthRequestTypes()}
	 */
	protected Set<IntegerPacketType> getMutualAuthAppRequestTypes() {
		Set<IntegerPacketType> types = this.app.getMutualAuthRequestTypes();
		return types != null ? types : new HashSet<IntegerPacketType>();
	}
	
	protected String getARARNodesAsString() {
		return this.arar;
	}

	protected String getARRCNodesAsString() {
		return this.arrc;
	}

	private boolean preRestore(String name, String state) {
		if(name.equals(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString())) {
			this.arar = state;
			return true;
		}
		else if(name.equals(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString())) {
			this.arrc = state;
			return true;
		}
		return false;
	}
}
