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
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestIdentifier;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.OverloadException;
import edu.umass.cs.gigapaxos.paxosutil.PaxosPacketDemultiplexer;
import edu.umass.cs.gigapaxos.paxosutil.StringContainer;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageExtractor;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.AddressMessenger;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.RTTEstimator;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.http.HttpActiveReplica;
import edu.umass.cs.reconfiguration.interfaces.ActiveReplicaFunctions;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorCallback;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckDropEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckStopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DefaultAppRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DropEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.EchoRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.EpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.HelloRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.ActiveReplicaProtocolTask;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.WaitEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.AggregateDemandProfiler;
import edu.umass.cs.reconfiguration.reconfigurationutils.AppInstrumenter;
import edu.umass.cs.reconfiguration.reconfigurationutils.CallbackMap;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.UtilServer;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This class is the main wrapper around active replicas of
 *            reconfigurable app instances. It processes requests both from the
 *            Reconfigurator as well as the underlying app's clients. This class
 *            is also use to wrap the Reconfigurator itself in order to
 *            reconfigure Reconfigurators (to correctly perform reconfigurator
 *            add/remove operations).
 * 
 *            <p>
 * 
 *            This class handles the following ReconfigurationPackets:
 *            {@code STOP_EPOCH,
 *            START_EPOCH, REQUEST_EPOCH_FINAL_STATE, DROP_EPOCH} as also listed
 *            in {@link ActiveReplicaProtocolTask ActiveReplicaProtocolTask}. It
 *            relies upon the app's implementation of AbstractDemandProfile in
 *            order to determine how frequenltly to report demand statistics to
 *            Reconfigurators and whether and how to reconfigure the current
 *            active replica placement.
 */
public class ActiveReplica<NodeIDType> implements ReconfiguratorCallback,
		PacketDemultiplexer<Request>, ActiveReplicaFunctions {
	/**
	 * Offset for client facing port that may in general be different from
	 * server-to-server communication as we may need different transport-layer
	 * security schemes for server-server compared to client-server
	 * communication.
	 * 
	 * The commented DEFAULT_CLIENT_PORT_OFFSET field below has been moved to
	 * ReconfigurationConfig.
	 */

	private final AbstractReplicaCoordinator<NodeIDType> appCoordinator;
	private final AbstractReplicaCoordinator<NodeIDType> originalAppCoordinator;
	private final ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig;
	private final ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String> protocolExecutor;
	private final ActiveReplicaProtocolTask<NodeIDType> protocolTask;
	private final SSLMessenger<NodeIDType, ?> messenger;

	private final AggregateDemandProfiler demandProfiler;
	private final boolean noReporting;
	private boolean recovering = true;

	private static final Logger log = (ReconfigurationConfig.getLogger());

	/* Stores only those requests for which a callback is desired after
	 * (coordinated) execution. StopEpoch is the only example of such a request
	 * in ActiveReplica. */
	private final CallbackMap<NodeIDType> callbackMap = new CallbackMap<NodeIDType>();

	@SuppressWarnings("unchecked")
	private ActiveReplica(AbstractReplicaCoordinator<NodeIDType> appC,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig,
			SSLMessenger<NodeIDType, ?> messenger, boolean noReporting) {
		this.originalAppCoordinator = appC;
		this.appCoordinator = (AbstractReplicaCoordinator<NodeIDType>)
				wrapCoordinator(appC.setStopCallback(
				// setting default callback is optional
				// (ReconfiguratorCallback) this).setCallback(
						(ReconfiguratorCallback) this));
		this.nodeConfig = new ConsistentReconfigurableNodeConfig<NodeIDType>(
				nodeConfig);
		this.demandProfiler = new AggregateDemandProfiler(getReconfigurableAppInfo());
		this.messenger = messenger;
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String>(
				messenger);
		this.protocolTask = new ActiveReplicaProtocolTask<NodeIDType>(
				getMyID(), this.nodeConfig, this);
		this.protocolExecutor.register(this.protocolTask.getDefaultTypes(),
				this.protocolTask);
		this.appCoordinator.setMessenger(this.messenger);
		this.noReporting = noReporting;
		initClientMessenger(false);
		if (ReconfigurationConfig.getClientSSLMode() != SSL_MODES.CLEAR)
			initClientMessenger(true);
		assert (this.messenger.getClientMessenger() != null);
		assert (this.appCoordinator.getMessenger() == this.messenger);
		this.recovering = false;
		
		this.preRestore(
				AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString(),
				this.appCoordinator.getARARNodesAsString());
		this.preRestore(
				AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString(),
				this.appCoordinator.getARRCNodesAsString());

		initInstrumenter();
		if (Config.getGlobalBoolean(ReconfigurationConfig.RC.ENABLE_NAT)) {
			sendHelloRequest();
		}
		
		// ENABLE_ACTIVE_REPLICA_HTTP is true
		if (Config.getGlobalBoolean(RC.ENABLE_ACTIVE_REPLICA_HTTP) 
				// and this node is not a reconfigurator
				&& !(nodeConfig.getReconfigurators().contains(this.getMyID()))) {
			InetSocketAddress me = this.messenger.getListeningSocketAddress();
			final InetSocketAddress addr = new InetSocketAddress(me.getAddress(),
					ReconfigurationConfig.getHTTPPort( me.getPort()) );
			
			this.protocolExecutor.submit(new Runnable() {
				@Override
				public void run() {
					initHTTPServer(false, addr);
				}
			});
		}
	}

	private void initHTTPServer(boolean ssl, InetSocketAddress addr){
		
		try {
			// initialize HTTP server
			new HttpActiveReplica(this, addr, ssl);
			
		} catch (Exception e) {
			if (!(e instanceof InterruptedException)) // close
				e.printStackTrace();
		}	
	}
	
	protected static AbstractReplicaCoordinator<?> wrapCoordinator(
			AbstractReplicaCoordinator<?> coordinator) {
		Class<?> clazz = null;
		try {
			clazz = Class.forName(Config
					.getGlobalString(RC.COORDINATOR_WRAPPER));
		} catch (ClassNotFoundException e) {
			// eat up exception, normal case
		}
		if (clazz == null)
			return coordinator;
		// reflectively instantiate
		try {
			return (AbstractReplicaCoordinator<?>) clazz.getConstructor(
					AbstractReplicaCoordinator.class).newInstance(coordinator);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		return coordinator;
	}

	/**
	 * @param name
	 * @return Refer {@link Replicable#checkpoint(String)}.
	 */
	@Override
	public String preCheckpoint(String name) {
		return name.equals(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES
				.toString()) ? this.nodeConfig.getActiveReplicasReadOnly()
				.toString() : name
				.equals(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES
						.toString()) ? this.nodeConfig
				.getReconfiguratorsReadOnly().toString() 
//					: name.equals(ReconfigurationConfig.getDefaultServiceName())? 
//							"random_gibberish_initial_state"
							:null;
	}
	
	private static Map<String, InetSocketAddress> sockAddrMapFromStringified(String state) {
		HashMap<String, InetSocketAddress> map = new HashMap<String, InetSocketAddress>();
		for (String entry : state.replaceAll("\\{", "")
				.replaceAll("\\}", "").split(",")) {
			String[] pieces = entry.split("=");
			assert (pieces.length == 2);
			map.put(pieces[0].trim(),
					Util.getInetSocketAddressFromString(pieces[1].trim()));
		}
		return map;
	}
	
	@Override
	public boolean preRestore(String name, String state) {
		if (state == null)
			return false;
		try {
			if (name.equals(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES
					.toString())) {
				this.reconcileARNodes(state);
				return true;
			} else if (name
					.equals(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES
							.toString())) {
				this.reconcileRCNodes(state);
				return true;
			}
//			else if(name.equals(ReconfigurationConfig.getDefaultServiceName()))
//				return true;
			
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		return false;
	}
	
	private void reconcileARNodes(String state) throws IOException {
		Map<String, InetSocketAddress> map = sockAddrMapFromStringified(state);
		this.addMissingToNodeConfig(map);
		this.deleteFromNodeConfig(map);
	}

	private void reconcileRCNodes(String state) throws IOException {
		Map<String, InetSocketAddress> map = sockAddrMapFromStringified(state);
		this.addMissingToNodeConfig(map, true); // isReconfigurator true
		this.deleteFromNodeConfig(map, true);
	}

	// default is active, i.e., isReconfigurator is false
	private void addMissingToNodeConfig(Map<String, InetSocketAddress> map) throws IOException {
		this.addMissingToNodeConfig(map, false);
	}
	private void addMissingToNodeConfig(Map<String, InetSocketAddress> map, boolean isReconfigurator) throws IOException {
		for (String strNode : map.keySet())
			if (!this.nodeConfig.nodeExists(this.nodeConfig
					.valueOf(strNode))) {
				if (isReconfigurator)
					this.nodeConfig.addReconfigurator(
							this.nodeConfig.valueOf(strNode), map.get(strNode));
				else
					this.nodeConfig.addActiveReplica(
							this.nodeConfig.valueOf(strNode), map.get(strNode));

				log.log(Level.INFO, "{0} adding {1} {2}:{3}", new Object[] {
						this,
						isReconfigurator ? "reconfigurator" : "active replica",
						strNode, map.get(strNode) });
				String propertiesFile = PaxosConfig.getPropertiesFile();
				/* Writing to the properties file is needed for safety, not just
				 * as an administrative convenience. Unlike reconfigurators,
				 * actives have no persistent database for storing nodeConfig
				 * information, so they simply use the file system and the 
				 * existing properties file. */
				if(propertiesFile!=null)
				UtilServer
						.writeProperty(
									(isReconfigurator ? ReconfigurationConfig.DEFAULT_RECONFIGURATOR_PREFIX
											: PaxosConfig.DEFAULT_SERVER_PREFIX)
											+ strNode,
								map.get(strNode).getAddress().getHostAddress()+":"+map.get(strNode).getPort(),
								propertiesFile,
								isReconfigurator ? ReconfigurationConfig.DEFAULT_RECONFIGURATOR_PREFIX
										.toString()
										: PaxosConfig.DEFAULT_SERVER_PREFIX
												.toString());
			}
	}

	private void deleteFromNodeConfig(Map<String, InetSocketAddress> map)
			throws IOException {
		this.deleteFromNodeConfig(map, false);
	}

	private void deleteFromNodeConfig(Map<String, InetSocketAddress> map,
			boolean isReconfigurator) throws IOException {
		for (NodeIDType node : (isReconfigurator ? this.nodeConfig.getReconfigurators() : this.nodeConfig.getActiveReplicas()))
			if (!map.containsKey(node.toString())) {
				if (isReconfigurator)
					this.nodeConfig.removeReconfigurator(node);
				else
					this.nodeConfig.removeActiveReplica(node);
				log.log(Level.INFO, "{0} removing {1} {2}", new Object[] {
						this,
						isReconfigurator ? "reconfigurator" : "active replica",
						node });
				String propertiesFile = PaxosConfig.getPropertiesFile();
				if(propertiesFile!=null)
				UtilServer
						.writeProperty(
								node.toString(),
								null,
								propertiesFile,
								isReconfigurator ? ReconfigurationConfig.DEFAULT_RECONFIGURATOR_PREFIX
										.toString()
										: PaxosConfig.DEFAULT_SERVER_PREFIX
												.toString());
			}
	}

	private  ReconfigurableAppInfo getReconfigurableAppInfo() {
		return new ReconfigurableAppInfo() {

			@Override
			public Set<String> getReplicaGroup(String serviceName) {
				Set<String> group = new HashSet<String>();
				for (NodeIDType node : ActiveReplica.this.appCoordinator
						.getReplicaGroup(serviceName))
					group.add(node.toString());
				return group;
			}

			@Override
			public String snapshot(String serviceName) {
				return ActiveReplica.this.appCoordinator.checkpoint(serviceName);
			}

			@Override
			public Map<String, InetSocketAddress> getAllActiveReplicas() {
				return ActiveReplica.this.nodeConfig.getAllActiveReplicas();
			}
		};
	}

	/* This should be false because the outstanding requests queue does not
	 * handle request ID conflicts correctly. It is also unnecessary to handle
	 * conflicts here as replica coordinators such as paxos need to do it anyway
	 * and we have support for request-specific callbacks that are simpler and
	 * slightly more efficient. */
	private static final boolean ENQUEUE_REQUEST = false;

	protected ActiveReplica(AbstractReplicaCoordinator<NodeIDType> appC,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig,
			SSLMessenger<NodeIDType, ?> messenger) {
		this(appC, nodeConfig, messenger, false);
	}

	class SenderAndRequest implements ExecutedCallback {
		final InetSocketAddress csa;
		final Request request;
		final boolean isCoordinated;
		final InetSocketAddress mysa;
		final long recvTime;

		SenderAndRequest(Request request, InetSocketAddress isa,
				InetSocketAddress mysa, long recvTime) {
			this.csa = isa;
			this.request = request;
			this.mysa = mysa;
			this.recvTime = recvTime;
			this.isCoordinated = isCoordinated(request);
		}

		@Override
		public void executed(Request request, boolean handled) {
			ActiveReplica.this.executedInternal(request, handled, this);
		}
	}

	static final MessageDigest md = initMD();

	static MessageDigest initMD() {
		try {
			return MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	static BigInteger toBigInt(byte[] request) {
		synchronized (md) {
			byte[] digest = md.digest(request);
			md.reset();
			return new BigInteger(digest);
		}
	}

	/**
	 * The outstanding queue is no longer used as ENQUEUE_REQUEST is false by
	 * default. So this timeout value doesn't really matter for anything.
	 */
	private static final long REQUEST_TIMEOUT = 5000;
	private final GCConcurrentHashMap<Long, SenderAndRequest> outstanding = new GCConcurrentHashMap<Long, SenderAndRequest>(
			new GCConcurrentHashMapCallback() {
				@Override
				public void callbackGC(Object key, Object value) {
				}
			}, REQUEST_TIMEOUT);

	/* enqueue can be invoked only for ReplicableRequest that extends
	 * RequestIdentifier. */
	private SenderAndRequest enqueue(SenderAndRequest senderAndRequest) {
		return this.outstanding
				.put(((RequestIdentifier) (senderAndRequest.request))
						.getRequestID(), senderAndRequest);
	}

	private SenderAndRequest dequeue(RequestIdentifier request) {
		return this.outstanding.remove(request.getRequestID());
	}

	private SenderAndRequest dequeueAndSendResponse(RequestIdentifier request) {
		return this.sendResponse((Request) request,
				this.outstanding.remove(request.getRequestID()));
	}

	private SenderAndRequest sendResponse(Request request,
			SenderAndRequest senderAndRequest) {
		return this.sendResponseAndDemandStats(request, senderAndRequest,
				senderAndRequest != null ? senderAndRequest.isCoordinated
						: true);
	}

	private ClientRequest makeClientResponse(Request response, long requestID) {
		return response == null || response instanceof ClientRequest
				&& ((ClientRequest) response).getRequestID() == requestID ? (ClientRequest) response
				: ReplicableClientRequest.wrap(response, requestID);
	}

	// used for both local and coordinated requests
	private SenderAndRequest sendResponseAndDemandStats(Request request,
			SenderAndRequest senderAndRequest, boolean isCoordinated) {
		if (senderAndRequest == null)
			return null;
		// else
		// send demand report
		this.updateDemandStats(request, senderAndRequest.csa.getAddress());
		instrumentNanoApp(isCoordinated ? Instrument.replicable
				: Instrument.local, senderAndRequest.recvTime);

		long t = System.nanoTime();
		ClientRequest response = null;
		// send response to originating client
		if (request instanceof ClientRequest
				&& (response = makeClientResponse(
						((ClientRequest) request).getResponse(),
						((ClientRequest) request).getRequestID())) != null) {
			try {
				log.log(Level.FINE,
						"{0} sending response {1} back to requesting client {2} for {3} request {4}",
						new Object[] {
								this,
								((ClientRequest) request).getResponse()
										.getSummary(), senderAndRequest.csa,
								isCoordinated ? "coordinated" : "",
								request.getSummary() });
				int written = ((JSONMessenger<?>) this.messenger).sendClient(
						senderAndRequest.csa,
						response instanceof Byteable ? ((Byteable) response)
								.toBytes() : response, senderAndRequest.mysa);
				if (written > 0)
					if (isCoordinated)
						AppInstrumenter.sentResponseCoordinated(request);
					else
						AppInstrumenter.sentResponseLocal(request);
			} catch (IOException | JSONException e) {
				e.printStackTrace();
			}
		}
		instrumentNanoApp(Instrument.reply, t);
		return senderAndRequest;
	}

	private static final String appName = ReconfigurationConfig.application
			.getSimpleName();

	
	/**
	 * The interval to send {@link HelloRequest} to update
	 * the NIO socket address on the other replicas.
	 */
	private static final int helloRequestInterval = 10; // seconds
	
	protected void sendHelloRequest() {
		this.protocolExecutor.scheduleWithFixedDelay(
				new HelloRunnable(messenger, nodeConfig), 0, helloRequestInterval, TimeUnit.SECONDS);
	}
	
	private class HelloRunnable implements Runnable {
		
		private final SSLMessenger<NodeIDType, ?> messenger;
		private final ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig;
		private final NodeIDType myID;
		
		HelloRunnable(SSLMessenger<NodeIDType, ?> messenger, 
				ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig) {
			this.messenger = messenger;
			this.nodeConfig = nodeConfig;
			this.myID = messenger.getMyID();			
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public void run() {
			// nodeConfig.
			for (Object id : nodeConfig.getActiveReplicasMap().keySet()){
				if ( !id.toString().equals(myID.toString())) {
					@SuppressWarnings({ "unchecked" })
					HelloRequest<NodeIDType> request = new HelloRequest(this.messenger.getMyID().toString());		
					try {						
						this.messenger.sendToAddress((InetSocketAddress) nodeConfig.getActiveReplicasMap().get(id.toString()), 
								request.toBytes());								
					} catch ( IOException e ) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			for (Object id : nodeConfig.getReconfiguratorsReadOnly().keySet() ){
				InetSocketAddress addr = nodeConfig.getReconfiguratorsReadOnly().get(id.toString());
				
				@SuppressWarnings("unchecked")
				HelloRequest<NodeIDType> request = new HelloRequest(this.messenger.getMyID().toString());		
				try {						
					this.messenger.sendToAddress(addr, 
							request.toBytes());								
				} catch ( IOException e ) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	// to print instrumentation stats periodically
	protected void initInstrumenter() {
		if (Config.getGlobalBoolean(RC.ENABLE_INSTRUMENTATION))
			this.protocolExecutor.scheduleWithFixedDelay(new Runnable() {
				public void run() {
					System.out.println(DelayProfiler.getStats()
							+ AppInstrumenter.getStats());
				}
			}, 0, 5, TimeUnit.SECONDS);
	}

	private static final boolean isCoordinated(Request request) {
		return request instanceof ReplicableRequest
				&& ((ReplicableRequest) request).needsCoordination();
	}

	private static final Level debug = Level.FINER;
	private static final Level unhandled = Level.FINE;

	@Override
	public boolean handleMessage(Request incoming, NIOHeader header) {

		log.log(debug, "{0} handleMessage received {1}", new Object[] { this,
				incoming.getSummary(log.isLoggable(debug)) });

		long entryTime = System.nanoTime();
		@SuppressWarnings("unchecked")
		BasicReconfigurationPacket<NodeIDType> rcPacket = incoming instanceof BasicReconfigurationPacket ? (BasicReconfigurationPacket<NodeIDType>) incoming
				: null;
		
		try {
			// try handling as reconfiguration packet through protocol task
			if (rcPacket != null) {
				if ( rcPacket instanceof HelloRequest ){
					headerMap.put(rcPacket.getInitiator().toString(), header);
				}
				if (!this.protocolExecutor.handleEvent(rcPacket))
					// do nothing, just log
					log.log(unhandled,
							"{0} unable to handle packet {1}",
							new Object[] {
									this,
									incoming.getSummary(log
											.isLoggable(unhandled)) });
			}
			// else must be app request
			else if (assertAppRequest(incoming)) {
				log.log(Level.FINE,
						"{0} handleMessage received appRequest {1}",
						new Object[] {
								this,
								incoming.getSummary(log.isLoggable(Level.FINE)) });
				// long startTime = System.currentTimeMillis();
				Request request = incoming;
				boolean isCoordinatedRequest = isCoordinated(request);

				SenderAndRequest senderAndRequest = new SenderAndRequest(
						request, header.sndr, header.rcvr,
						// startTime
						entryTime);
				// enqueue demand stats sending callback
				if (ENQUEUE_REQUEST)
					if (isCoordinatedRequest)
						enqueue(senderAndRequest);

				// app doesn't understand ReplicableClientRequest
				if (!isCoordinatedRequest
						&& request instanceof ReplicableClientRequest)
					request = this.appCoordinator.getRequest(
							(ReplicableClientRequest) request, header);

				// send to app via its coordinator
				boolean handled = this.handRequestToApp(
						makeClientRequest(request, header.sndr),
						ENQUEUE_REQUEST ?
						// generic callback
						this
								:
								// request-specific callback
								senderAndRequest);

				InetSocketAddress sender = header.sndr, receiver = header.rcvr;
				if (handled) {
					if (ENQUEUE_REQUEST)
						if (!isCoordinatedRequest)
							this.sendResponseAndDemandStats(request,
									senderAndRequest, false);
					// else do nothing as coordinated callback will be called
				} else {
					// if failed, dequeue useless enqueue
					if (isCoordinatedRequest)
						this.dequeue(((ReplicableRequest) request));

					// send error message to sender
					((JSONMessenger<?>) this.messenger).sendClient(
							sender,
							new ActiveReplicaError(this.nodeConfig
									.getNodeSocketAddress(getMyID()), request
									.getServiceName(),
									((ClientRequest) request).getRequestID()),
							receiver);
					AppInstrumenter.sentActiveReplicaError();
				}
				if (instrument(Instrument.getStats)) {
					System.out.println(DelayProfiler.getStats());
					log.log(Level.INFO, "{0} {1}", new Object[] { this,
							DelayProfiler.getStats() });
				}
			}
		} catch (RequestParseException rpe) {
			rpe.printStackTrace();
		} catch (JSONException je) {
			je.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false; // neither reconfiguration packet nor app request
	}

	// TODO: unused, remove
	@SuppressWarnings("unused")
	private Request getRequest(JSONObject jsonObject)
			throws RequestParseException, JSONException {
		long t = System.nanoTime();
		if (jsonObject instanceof JSONMessenger.JSONObjectWrapper)
			try {
				byte[] bytes = (byte[]) ((JSONMessenger.JSONObjectWrapper) jsonObject)
						.getObj();
				Request request = this.appCoordinator.getRequest(Arrays
						.copyOfRange(bytes, NIOHeader.BYTES, bytes.length),
						NIOHeader.getNIOHeader(bytes));
				instrumentNanoApp(Instrument.getRequest, t);
				return request;
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		// else slow path
		String stringified = STAMP_SENDER_ADDRESS_JSON
				|| !jsonObject.has(MessageExtractor.STRINGIFIED) ? jsonObject
				.toString() : jsonObject
				.getString(MessageExtractor.STRINGIFIED);
		instrumentNano(Instrument.restringification, t);

		Request request = this.appCoordinator.getRequest(stringified);
		instrumentNanoApp(Instrument.getRequest, t);
		return request;
	}

	private static final boolean STAMP_SENDER_ADDRESS_JSON = Config
			.getGlobalBoolean(RC.STAMP_SENDER_ADDRESS_JSON);

	@Override
	public void executed(Request request, boolean handled) {
		if (this.isRecovering())
			return;

		// stop callback is not handled here
		if (request instanceof ReplicableRequest)
			this.dequeueAndSendResponse(((ReplicableRequest) request));

		/* We need to handle the callback in a separate thread, otherwise we
		 * risk sending the ackStop before the epoch final state has been
		 * checkpointed that in turn has to happen strictly before the app
		 * itself has dropped the state. If we send the ackStop early for a
		 * delete request and the reconfigurator sends a delete confirmation to
		 * the client, a client read request may find the record undeleted (at
		 * each and every active replica) despite receiving the delete
		 * confirmation. This scenario is unlikely but can happen, especially if
		 * creating the final epoch state checkpoint takes long. The thread
		 * below waits for the app state to be deleted before sending out the
		 * ackStop. We need a separate thread because "this" thread is the one
		 * executing the stop request and is the one responsible for creating
		 * the epoch final state checkpoint, so it can not itself wait for that
		 * to complete without getting stuck.
		 * 
		 * Note that a client read may still find a record undeleted despite
		 * receiving a delete confirmation if it sends the read to an active
		 * replica other than the one that sent the ackStop to the
		 * reconfigurator as that other active replica may still be catching up.
		 * But the point above is that this scenario can happen even with a
		 * single replica, which is "wrong", unless we spawn a separate thread. */
		if (handled && request instanceof ReconfigurableRequest
				&& ((ReconfigurableRequest) request).isStop())
			// protocol executor also allows us to just submit a Runnable
			this.protocolExecutor.submit(new AckStopNotifier(request));
	}

	// FIXME: overlaps with executed(.) above
	private void executedInternal(Request request, boolean handled,
			SenderAndRequest senderAndRequest) {
		if (this.isRecovering())
			return;

		this.sendResponse(request, senderAndRequest);

		if (handled && request instanceof ReconfigurableRequest
				&& ((ReconfigurableRequest) request).isStop())
			// protocol executor also allows us to just submit a Runnable
			this.protocolExecutor.submit(new AckStopNotifier(request));
	}

	/* This class is a task to notify reconfigurators of a successfully stopped
	 * replica group. It deletes the replica group and then sends the ackStop. */
	class AckStopNotifier implements Runnable {
		Request request;

		AckStopNotifier(Request request) {
			this.request = request;
		}

		public void run() {
			StopEpoch<NodeIDType> stopEpoch = null;
			int epoch = ((ReconfigurableRequest) request).getEpochNumber();
			while ((stopEpoch = callbackMap.notifyStop(
					request.getServiceName(), epoch)) != null) {
				appCoordinator.deleteReplicaGroup(request.getServiceName(),
						epoch);
				sendAckStopEpoch(stopEpoch);
			}
			/* Stops are coordinated exactly once for a paxos group, so we must
			 * always find a match here.
			 * 
			 * Unless the app itself coordinated the stop in which case this
			 * assert should not hold. */
			// assert (stopEpoch != null);
		}
	}

	/**
	 * @return Set of packet types processed by ActiveReplica on the server
	 *         port. These include active replica types plus app or app
	 *         coordinator types allowed on the server port.
	 */
 	protected Set<IntegerPacketType> getPacketTypes(boolean allow) {
		Set<IntegerPacketType> types = allow ? this.appCoordinator
				.getRequestTypes() : this.appCoordinator
				.getCoordinatorRequestTypes();
		if (types == null)
			types = new HashSet<IntegerPacketType>();
		types.addAll(this.appCoordinator.getMutualAuthAppRequestTypes());
		/* We remove paxos packets because that is added as a separate
		 * demultiplexer by PaxosManager. */
		types.remove(PaxosPacket.PaxosPacketType.PAXOS_PACKET);

		types.addAll(this.getActiveReplicaPacketTypes());

		return types;
	}
 	protected Set<IntegerPacketType> getPacketTypes() {
 		return getPacketTypes(Config
				.getGlobalBoolean(RC.ALLOW_APP_TYPES_ON_SERVER_PORT));
 	}
	/**
	 * For graceful closure.
	 */
	public void close() {
		this.protocolExecutor.stop();
		this.messenger.stop();
		this.appCoordinator.stop();
		this.originalAppCoordinator.stop();
	}

	// /////////////// Start of protocol task handler
	// methods//////////////////////

	/**
	 * Will spawn FetchEpochFinalState to fetch the final state of the previous
	 * epoch if one existed, else will locally create the current epoch with an
	 * empty initial state.
	 * 
	 * @param event
	 * @param ptasks
	 * @return Messaging task, typically null as we spawn a protocol task to
	 *         fetch the previous epoch's final state.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleStartEpoch(
			StartEpoch<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		StartEpoch<NodeIDType> startEpoch = ((StartEpoch<NodeIDType>) event);
		this.logEvent(event, Level.FINE);
		AckStartEpoch<NodeIDType> ackStart = new AckStartEpoch<NodeIDType>(
				startEpoch.getSender(), startEpoch.getServiceName(),
				startEpoch.getEpochNumber(), getMyID());
		GenericMessagingTask<NodeIDType, ?>[] mtasks = (new GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>(
				startEpoch.getSender(), ackStart)).toArray();
		// send positive ack even if app has moved on
		if (this.alreadyMovedOn(startEpoch)) {
			log.log(Level.FINE,
					"{0} sending to {1}: {2} as paxos group has already moved on to version {3}",
					new Object[] {
							this,
							startEpoch.getSender(),
							ackStart.getSummary(),
							this.appCoordinator.getEpoch(startEpoch
									.getServiceName()) });
			return mtasks;
		}
		// else
		if (startEpoch.isPassive()) {
			// do nothing
			return null;
		}
		// if no previous group, create replica group with empty state
		else if (startEpoch.noPrevEpochGroup()
				|| AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString().equals(startEpoch.getServiceName())
				|| AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString().equals(startEpoch.getServiceName())
				) {
			boolean created = false;
			try {
				// createReplicaGroup is a local operation (but may fail)
				created = startEpoch.isBatchedCreate() ? this
						.batchedCreate(startEpoch) : this.appCoordinator
						.createReplicaGroup(startEpoch.getServiceName(),
								startEpoch.getEpochNumber(),
								startEpoch.getInitialState(),
								startEpoch.getCurEpochGroup());
			} catch (Error e) {
				log.severe(this
						+ " received null state in non-passive startEpoch "
						+ startEpoch);
				e.printStackTrace();
			}
			/* FIXME: This assertion can fail if there is a reconfiguration
			 * immmediately after creation. the creation above will throw an
			 * exception if it fails */
			assert (!created || startEpoch.getCurEpochGroup().equals(
					this.appCoordinator.getReplicaGroup(startEpoch
							.getServiceName()))) : this
					+ " unable to get replica group right after creation for startEpoch "
					+ startEpoch.getSummary()
					+ ": created="
					+ created
					+ "; startEpoch.getCurEpochGroup()="
					+ startEpoch.getCurEpochGroup()
					+ "; this.appCoordinator.getReplicaGroup="
					+ this.appCoordinator.getReplicaGroup(startEpoch
							.getServiceName());
			log.log(Level.FINE, "{0} sending to {1}: {2}", new Object[] { this,
					startEpoch.getSender(), ackStart.getSummary() });

			return created ? mtasks : null; // and also send positive ack
		}

		/* Else request previous epoch state using a threshold protocoltask. We
		 * spawn WaitEpochFinalState as opposed to simply returning it in
		 * ptasks[0] as otherwise we could end up creating tasks with duplicate
		 * keys. */
		this.spawnWaitEpochFinalState(startEpoch);
		return null; // no messaging if asynchronously fetching state
	}

	private static final boolean BATCH_CREATION = true;

	/* This could be optimized further by supporting batch creation in
	 * PaxosManager. */
	private boolean batchedCreate(StartEpoch<NodeIDType> startEpoch) {
		boolean created = true;
		if (BATCH_CREATION)
			return this.appCoordinator.createReplicaGroup(
					startEpoch.getNameStates(), startEpoch.getCurEpochGroup());
		else
			for (String name : startEpoch.getNameStates().keySet()) {
				assert (startEpoch.getEpochNumber() == 0);
				created = created
						&& this.appCoordinator.createReplicaGroup(
								name,
								startEpoch.getEpochNumber(), // 0
								startEpoch.getNameStates().get(name),
								startEpoch.getCurEpochGroup());
			}
		return created;
	}

	// synchronized to ensure atomic testAndStart property
	private synchronized void spawnWaitEpochFinalState(
			StartEpoch<NodeIDType> startEpoch) {
		WaitEpochFinalState<NodeIDType> waitFinal = new WaitEpochFinalState<NodeIDType>(
				getMyID(), startEpoch, this.appCoordinator);
		if (!this.protocolExecutor.isRunning(waitFinal.getKey()))
			this.protocolExecutor.spawn(waitFinal);
		else {
			WaitEpochFinalState<NodeIDType> running = (WaitEpochFinalState<NodeIDType>) this.protocolExecutor
					.getTask(waitFinal.getKey());
			if (running != null)
				running.addNotifiee(startEpoch.getInitiator(),
						startEpoch.getKey());
		}
	}

	/**
	 * @param stopEpoch
	 * @param ptasks
	 * @return Messaging task, typically null as we coordinate the stop request
	 *         and use a callback to notify the reconfigurator that issued the
	 *         {@link StopEpoch}.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleStopEpoch(
			StopEpoch<NodeIDType> stopEpoch,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		this.logEvent(stopEpoch);
		if (this.stoppedOrMovedOn(stopEpoch))
			return this.sendAckStopEpoch(stopEpoch).toArray(); // still send ack
		if (!stopEpoch.shouldExecuteStop())
			return null;
		// else coordinate stop with callback
		this.callbackMap.addStopNotifiee(stopEpoch);
		ReconfigurableRequest appStop = this.getAppStopRequest(
				stopEpoch.getServiceName(), stopEpoch.getEpochNumber());
		log.log(Level.INFO,
				"{0} coordinating {1} while replica group = {2}:{3} {4}",
				new Object[] {
						this,
						stopEpoch.getSummary(),
						stopEpoch.getServiceName(),
						this.appCoordinator.getEpoch(stopEpoch.getServiceName()),
						this.appCoordinator.getReplicaGroup(stopEpoch
								.getServiceName()) });
		this.appCoordinator.handleIncoming(
				makeClientRequest(appStop, this.nodeConfig
						.getNodeSocketAddress(stopEpoch.getInitiator())), this);
		return null; // need to wait until callback
	}

	private static final Request makeClientRequest(Request request,
			InetSocketAddress csa) {

		if (request instanceof ReplicableClientRequest)
			return ((ReplicableClientRequest) request).setClientAddress(csa);
		else if (request instanceof RequestPacket)
			return request;

		// else
		if (!(request instanceof ClientRequest)
				|| !(request instanceof ReplicableRequest && ((ReplicableRequest) request)
						.needsCoordination())
				|| request instanceof ReconfigurationPacket)
			return request;

		return ReplicableClientRequest.wrap(request).setClientAddress(csa);
	}

	/**
	 * @param event
	 * @param ptasks
	 * @return Messaging task to send AckDropEpochFinalState to reconfigurator
	 *         that issued the corresponding DropEpochFinalState.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleDropEpochFinalState(
			DropEpochFinalState<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		Level level = event.getServiceName().equals(
				AbstractReconfiguratorDB.RecordNames.RC_NODES.toString())
				|| event.getServiceName().equals(
						AbstractReconfiguratorDB.RecordNames.AR_NODES
								.toString()) ? Level.INFO : Level.FINE;
		this.logEvent(event, level);
		DropEpochFinalState<NodeIDType> dropEpoch = (DropEpochFinalState<NodeIDType>) event;
		boolean deleted = this.appCoordinator.deleteFinalState(
				dropEpoch.getServiceName(), dropEpoch.getEpochNumber());
		AckDropEpochFinalState<NodeIDType> ackDrop = new AckDropEpochFinalState<NodeIDType>(
				getMyID(), dropEpoch);
		GenericMessagingTask<NodeIDType, AckDropEpochFinalState<NodeIDType>> mtask = new GenericMessagingTask<NodeIDType, AckDropEpochFinalState<NodeIDType>>(
				dropEpoch.getInitiator(), ackDrop);
		assert (ackDrop.getInitiator().equals(dropEpoch.getInitiator()));
		log.log(deleted ? level : Level.FINE,
				"{0} {1} sending {2} to {3}",
				new Object[] { this, deleted ? "" : "*NOT*",
						ackDrop.getSummary(), ackDrop.getInitiator() });
		this.garbageCollectPendingTasks(dropEpoch);
		return deleted ? mtask.toArray() : null;
	}

	private ConcurrentHashMap<String, NIOHeader> headerMap = new ConcurrentHashMap<>();
	
	/**
	 * @param hello
	 * @param ptasks
	 * @return result
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleHelloRequest(
			HelloRequest<NodeIDType> hello,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		
		ReconfigurationConfig.log.log(Level.FINE, "{0} received hello request {1}", new Object[] {
				this, hello.getSummary() });
		
		NodeIDType nodeID = hello.myID;
		if (nodeID == null) {
			ReconfigurationConfig.log.log(Level.FINE, "{0} has no initiator in it", new Object[] {
				 hello.getSummary() });
			return null;
		}
		
		InetSocketAddress address = headerMap.get(nodeID.toString()).sndr;
		if (address != null) {
			synchronized(nodeConfig) {
				nodeConfig.removeActiveReplica(nodeID);
				nodeConfig.addActiveReplica(nodeID, address);
			}
		}
		ReconfigurationConfig.log.log(Level.FINE, "NodeConfig gets updated to {0}", new Object[] {
			nodeConfig });
		return null;
	}
	
	/**
	 * @param echo
	 * @param ptasks
	 * @return null
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleEchoRequest(
			EchoRequest echo,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.FINE, "{0} received echo request {1}", new Object[] {
				this, echo.getSummary() });
		if (echo.isRequest()) {
			try {
				((JSONMessenger<?>) this.messenger).sendClient(
						echo.getSender(), // can't use null initiator
						echo.makeResponse(this.nodeConfig
								.getNodeSocketAddress(getMyID()))
						// .toJSONObject()
						, echo.myReceiver);
			} catch (JSONException | IOException e) {
				e.printStackTrace();
			}
		} else if (echo.hasClosest()) {
			RTTEstimator.closest(echo.getSender(), echo.getClosest());
			log.log(Level.INFO,
					"{0} received closest map {1} from {2}; RTTEstimator.closest={3}",
					new Object[] {
							this,
							echo.getClosest(),
							echo.getSender(),
							RTTEstimator.getClosest(echo.getSender()
									.getAddress()) });
		}
		// else
		return null;
	}

	// drop any pending task (only WaitEpochFinalState possible) upon dropEpoch
	private void garbageCollectPendingTasks(
			DropEpochFinalState<NodeIDType> dropEpoch) {
		/* Can drop waiting on epoch final state of the epoch just before the
		 * epoch being dropped as we don't have to bother starting the dropped
		 * epoch after all. */
		boolean removed = (this.protocolExecutor.remove(Reconfigurator
				.getTaskKeyPrev(WaitEpochFinalState.class, dropEpoch, this
						.getMyID().toString())) != null);
		if (removed)
			log.log(Level.FINEST,
					"{0} removed WaitEpochFinalState {1}:{2}",
					new Object[] { this, dropEpoch.getServiceName(),
							(dropEpoch.getEpochNumber() - 1) });
	}

	/**
	 * @param event
	 * @param ptasks
	 * @return Messaging task returning the requested epoch final state to the
	 *         requesting ActiveReplica.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleRequestEpochFinalState(
			RequestEpochFinalState<NodeIDType> event,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		RequestEpochFinalState<NodeIDType> request = (RequestEpochFinalState<NodeIDType>) event;
		this.logEvent(event);
		StringContainer stateContainer = this.getFinalStateContainer(
				request.getServiceName(), request.getEpochNumber());
		if (stateContainer == null) {
			log.log(Level.INFO,
					"{0} ####did not find any epoch final state for#### {1}",
					new Object[] { this, request.getSummary() });
			return null;
		}

		EpochFinalState<NodeIDType> epochState = new EpochFinalState<NodeIDType>(
				request.getInitiator(), request.getServiceName(),
				request.getEpochNumber(), stateContainer.state, this.getMyID());
		GenericMessagingTask<NodeIDType, EpochFinalState<NodeIDType>> mtask = null;

		log.log(Level.INFO, "{0} returning epoch final state to {1} {2}",
				new Object[] { this, event.getKey(), epochState.getSummary() });
		mtask = new GenericMessagingTask<NodeIDType, EpochFinalState<NodeIDType>>(
				request.getInitiator(), epochState);

		return (mtask != null ? mtask.toArray() : null);
	}

	private StringContainer getFinalStateContainer(String name, int epoch) {
		if (this.appCoordinator instanceof PaxosReplicaCoordinator)
			return ((PaxosReplicaCoordinator<NodeIDType>) (this.appCoordinator))
					.getFinalStateContainer(name, epoch);
		String finalState = this.appCoordinator.getFinalState(name, epoch);
		return finalState == null ? null : new StringContainer(finalState);
	}

	public String toString() {
		return "AR." + this.messenger.getMyID();
	}

	/* ************ End of protocol task handler methods ************* */

	/* ****************** Private methods below ******************* */

	private boolean handRequestToApp(Request request, ExecutedCallback callback) {
		long t = System.nanoTime();
		boolean handled = false;
		try {
			AppInstrumenter.rcvdRequest(request);
			handled = this.appCoordinator.handleIncoming(request, callback);
		} catch (OverloadException re) {
			PaxosPacketDemultiplexer.throttleExcessiveLoad();
		}
		instrumentNanoApp(Instrument.handleIncoming, t);
		return handled;
	}

	private boolean stoppedOrMovedOn(BasicReconfigurationPacket<?> packet) {
		boolean retval = false;
		Integer epoch = this.appCoordinator.getEpoch(packet.getServiceName());
		if (epoch != null // has state
				// and moved on
				&& ((epoch - packet.getEpochNumber() > 0)
				// or same epoch but no replica group (because already stopped)
				|| (epoch == packet.getEpochNumber() && this.appCoordinator
						.getReplicaGroup(packet.getServiceName()) == null)))
			retval = true;
		if (retval)
			log.log(Level.INFO,
					"{0} has no state or has already moved on to epoch {1} upon receiving {2}",
					new Object[] { this, epoch, packet.getSummary() });
		else
			log.log(Level.FINE,
					"{0} has {1}:{2} with replica group {3} when asked to stop {4}:{5}",
					new Object[] {
							this,
							packet.getServiceName(),
							epoch,
							this.appCoordinator.getReplicaGroup(packet
									.getServiceName()),
							packet.getServiceName(), packet.getEpochNumber() });
		return retval;
	}

	// true means move on, so the replica group won't get created
	private boolean alreadyMovedOn(BasicReconfigurationPacket<?> packet) {
		Integer epoch = this.appCoordinator.getEpoch(packet.getServiceName());
		if (epoch != null)
			if (epoch - packet.getEpochNumber() > 0)
				return true;
			/* If state for this epoch exists and the replica group is not null,
			 * this is an active replica group, so we return true so that the
			 * replica group is not created; else the replica group is stopped
			 * locally and the only reason we got a re-creation request from
			 * reconfigurators is because it is legitimate to do so (i..e, the
			 * delete pending period has passed), so we return false so that the
			 * replica group is created after forcibly wiping out previous epoch
			 * final state that must be necessarily outdated at this point. */
			else if (epoch == packet.getEpochNumber())
				// active epoch
				return (this.appCoordinator.getReplicaGroup(packet
						.getServiceName()) != null);
		return false;
	}

	private NodeIDType getMyID() {
		return this.messenger.getMyID();
	}

	private Set<ReconfigurationPacket.PacketType> getActiveReplicaPacketTypes() {
		return this.protocolTask.getEventTypes();
	}

	// must not be static
	private Set<IntegerPacketType> cachedTypes = null;

	private Set<IntegerPacketType> getAppRequestTypes() {
		return cachedTypes != null ? cachedTypes
				: ((cachedTypes = this.appCoordinator.getAppRequestTypes()) != null ? cachedTypes
						: new HashSet<IntegerPacketType>());
	}

	private boolean assertAppRequest(Request request) throws JSONException {
		assert (request.getRequestType() == ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST || this
				.getAppRequestTypes().contains(request.getRequestType()) 
				// mutual auth types need to be separately accounted for
				|| this.appCoordinator.getMutualAuthAppRequestTypes().contains(request.getRequestType())
				) : request
				+ " not in " + cachedTypes;
		return true;
	}

	/* TODO: unused, remove. This ugly method existed in order to maintain
	 * efficient backwards compatibility with poor legacy gigapaxos apps. */
	@SuppressWarnings("unused")
	private boolean isAppRequest(JSONObject jsonObject) throws JSONException {
		byte[] bytes;

		Integer type = jsonObject instanceof JSONMessenger.JSONObjectWrapper ?
		// for apps that don't use Byteable
		(JSONPacket
				.couldBeJSON(
						bytes = (byte[]) (((JSONMessenger.JSONObjectWrapper) jsonObject)
								.getObj()), NIOHeader.BYTES) ? 0 :
		// else int right after NIOHeader
				ByteBuffer.wrap(bytes, NIOHeader.BYTES, Integer.BYTES).getInt())
				// else parse as regular json
				: JSONPacket.getPacketType(jsonObject);
		if (appTypesAsIntegers == null)
			appTypesAsIntegers = toIntegerSet(this
					.getAppRequestTypes());
		return type == 0 || appTypesAsIntegers.contains(type);
	}

	private static Set<Integer> appTypesAsIntegers = null;

	private static final Set<Integer> toIntegerSet(Set<IntegerPacketType> types) {
		Set<Integer> integers = new HashSet<Integer>();
		for (IntegerPacketType type : types)
			integers.add(type.getInt());
		return integers;
	}

	private static enum Instrument {
		updateDemandStats(Integer.MAX_VALUE), restringification(100), getRequest(
				100), local(100), replicable(100), getStats(1), handleIncoming(
				100), reply(100);

		private final int val;

		Instrument(int val) {
			this.val = val;
		}
	}

	private static final boolean ENABLE_INSTRUMENTATION = Config
			.getGlobalBoolean(RC.ENABLE_INSTRUMENTATION);
	private static final long MIN_INTER_DUMP_TIME = 2000;
	private static long lastDumpedTime = 0;

	private static final boolean instrument(int n) {
		return ENABLE_INSTRUMENTATION && n < Integer.MAX_VALUE && Util.oneIn(n);
	}

	private static final boolean instrument(Instrument param) {
		if (!ENABLE_INSTRUMENTATION)
			return false;
		else if (param == Instrument.getStats)
			if (System.currentTimeMillis() - lastDumpedTime < MIN_INTER_DUMP_TIME)
				return false;
			else {
				lastDumpedTime = System.currentTimeMillis();
				return true;
			}
		else
			return instrument(param.val);
	}

	private static final void instrumentNano(Instrument param, long t) {
		if (instrument(param.val))
			DelayProfiler.updateDelayNano(param.toString(), t);
	}

	private static final void instrumentNanoApp(Instrument param, long t) {
		if (instrument(param.val))
			DelayProfiler.updateDelayNano(appName + "." + param.toString(), t);
	}

	@SuppressWarnings("unused")
	private static final void instrumentApp(Instrument param, long t) {
		if (instrument(param.val))
			DelayProfiler.updateDelay(appName + "." + param.toString(), t);
	}

	/* Demand stats are updated upon every request. Demand reports are
	 * dispatched to reconfigurators only if warranted by the shouldReport
	 * method. This allows for reporting policies that locally aggregate some
	 * stats based on a threshold number of requests before reporting to
	 * reconfigurators. */
	private void updateDemandStats(Request request, InetAddress sender) {
		long t = System.nanoTime();
		if (this.noReporting)
			return;

		String name = request.getServiceName();
		if (request instanceof ReconfigurableRequest
				&& ((ReconfigurableRequest) request).isStop())
			return; // no reporting on stop
		if (this.demandProfiler.shouldSendDemandReport(request, sender))
			report(this.demandProfiler.pluckDemandProfile(name));
		else
			report(this.demandProfiler.trim());
		if (instrument(Instrument.updateDemandStats))
			DelayProfiler.updateDelayNano("updatedDemandStats", t);
	}

	/* Report demand stats to reconfigurators. This method will necessarily
	 * result in a stats message being sent out to reconfigurators. */
	private void report(AbstractDemandProfile demand) {
		try {
			NodeIDType reportee = selectReconfigurator(demand.getName());
			assert (reportee != null);
			/* We don't strictly need the epoch number in demand reports, but it
			 * is useful for debugging purposes. */
			Integer epoch = this.appCoordinator.getEpoch(demand.getName());
			GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, Object>(
					reportee, (new DemandReport<NodeIDType>(getMyID(),
							demand.getName(), (epoch == null ? 0 : epoch),
							demand)));
			this.send(mtask);
		} catch (Exception je) {
			je.printStackTrace();
		}
	}

	/* Returns a random reconfigurator. Util.selectRandom is designed to return
	 * a value of the same type as the objects in the input set, so it is okay
	 * to suppress the warning. */
	@SuppressWarnings("unchecked")
	private NodeIDType selectReconfigurator(String name) {
		Set<NodeIDType> reconfigurators = this.getReconfigurators(name);
		return (NodeIDType) Util.selectRandom(reconfigurators);
	}

	private Set<NodeIDType> getReconfigurators(String name) {
		return this.nodeConfig.getReplicatedReconfigurators(name);
	}

	private void send(GenericMessagingTask<NodeIDType, ?> mtask) {
		try {
			this.messenger.send(mtask);
		} catch (JSONException je) {
			je.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	// TODO: remove. This method is replaced by sendClient.
	@SuppressWarnings({ "unchecked", "unused" })
	private void send(InetSocketAddress isa, JSONObject msg) {
		try {
			((AddressMessenger<JSONObject>) this.messenger).sendToAddress(isa,
					msg);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void report(Set<AbstractDemandProfile> demands) {
		if (demands != null && !demands.isEmpty())
			for (AbstractDemandProfile demand : demands)
				this.report(demand);
	}

	private GenericMessagingTask<NodeIDType, ?> sendAckStopEpoch(
			StopEpoch<NodeIDType> stopEpoch) {
		// inform reconfigurator
		AckStopEpoch<NodeIDType> ackStopEpoch = new AckStopEpoch<NodeIDType>(
				this.getMyID(), stopEpoch,
				(stopEpoch.shouldGetFinalState() ? this.appCoordinator
						.getFinalState(stopEpoch.getServiceName(),
								stopEpoch.getEpochNumber()) : null));
		GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, Object>(
				(stopEpoch.getInitiator()), ackStopEpoch);
		log.log(Level.FINE, "{0} sending {1} to {2}", new Object[] { this,
				ackStopEpoch.getSummary(), stopEpoch.getInitiator() });
		this.send(mtask);
		return mtask;
	}

	private ReconfigurableRequest getAppStopRequest(String name, int epoch) {
		ReconfigurableRequest appStop = this.appCoordinator.getStopRequest(
				name, epoch);
		return appStop == null ? new DefaultAppRequest(name, epoch, true)
				: appStop;
	}

	@SuppressWarnings("unchecked")
	private AddressMessenger<?> initClientMessenger(boolean ssl) {
		AbstractPacketDemultiplexer<Request> pd = null;
		Messenger<InetSocketAddress, JSONObject> cMsgr = null;
		try {
			int myPort = (this.nodeConfig.getNodePort(getMyID()));
			if ((ssl ? getClientFacingSSLPort(myPort)
					: getClientFacingClearPort(myPort)) != myPort) {
				log.log(Level.INFO,
						"{0} maybe creating {1} client messenger at {2}:{3}",
						new Object[] {
								this,
								ssl ? "SSL" : "",
								this.nodeConfig.getBindAddress(getMyID()),
								""
										+ (ssl ? getClientFacingSSLPort(myPort)
												: getClientFacingClearPort(myPort)) });

				AddressMessenger<?> existing = (ssl ? this.messenger
						.getSSLClientMessenger() : this.messenger
						.getClientMessenger());
				if (existing == null || existing == this.messenger) {
					MessageNIOTransport<InetSocketAddress, JSONObject> niot = null;
					InetSocketAddress isa = new InetSocketAddress(
							this.nodeConfig.getBindAddress(getMyID()),
							ssl ? getClientFacingSSLPort(myPort)
									: getClientFacingClearPort(myPort));
					// only receives
					cMsgr = new JSONMessenger<InetSocketAddress>(
							(niot = new MessageNIOTransport<InetSocketAddress, JSONObject>(
									isa.getAddress(),
									isa.getPort(),
									/* Client facing demultiplexer is single
									 * threaded to keep clients from
									 * overwhelming the system with request
									 * load. */
									(pd = new ReconfigurationPacketDemultiplexer(
											this.nodeConfig,
											this.appCoordinator)),
									ssl ? ReconfigurationConfig
											.getClientSSLMode()
											: SSL_MODES.CLEAR))
									.setName(this.appCoordinator.app.toString()
											+ ":" + (ssl ? "SSL" : "")
											+ "ClientMessenger"));
					if (!niot.getListeningSocketAddress().equals(isa))
						throw new IOException(
								"Unable to listen on specified client facing socket address "
										+ isa
										+ "; created messenger listening instead on "
										+ niot.getListeningSocketAddress());
				} else if (!ssl) {
					log.log(Level.INFO,
							"{0} adding self as demultiplexer to existing {1} client messenger",
							new Object[] { this, ssl ? "SSL" : "" });
					if (this.messenger.getClientMessenger() instanceof Messenger)
						((Messenger<NodeIDType, ?>) existing)
								.addPacketDemultiplexer(pd = new ReconfigurationPacketDemultiplexer(
										this.nodeConfig, this.appCoordinator).setThreadName(this.getMyID().toString()+"-clientFacing"));
				} else {
					log.log(Level.INFO,
							"{0} adding self as demultiplexer to existing {1} client messenger",
							new Object[] { this, ssl ? "SSL" : "" });
					if (this.messenger.getSSLClientMessenger() instanceof Messenger)
						((Messenger<NodeIDType, ?>) existing)
								.addPacketDemultiplexer(pd = new ReconfigurationPacketDemultiplexer(
										this.nodeConfig, this.appCoordinator).setThreadName(this.getMyID().toString()+"-clientFacingSSL"));
				}

				Set<IntegerPacketType> appTypes = this.getAppRequestTypes();
				appTypes.removeAll(this.appCoordinator
						.getMutualAuthAppRequestTypes());
				pd.register(appTypes, this);
				
				/* These two requests are not app requests but are expected to be received on
				 * client-facing ports, so we need to register them here.
				 */
				pd.register(PacketType.ECHO_REQUEST, this);
				pd.register(PacketType.REPLICABLE_CLIENT_REQUEST, this);
			}
		} catch (IOException e) {
			e.printStackTrace();
			log.severe(this + ":" + e.getMessage());
			System.exit(1);
		}

		if (cMsgr != null)
			if (ssl && this.messenger.getSSLClientMessenger() == null)
				this.messenger.setSSLClientMessenger(cMsgr);
			else if (!ssl && this.messenger.getClientMessenger() == null)
				this.messenger.setClientMessenger(cMsgr);

		return cMsgr != null ? cMsgr : (AddressMessenger<?>) this.messenger;
	}

	private boolean isRecovering() {
		return this.recovering;
	}

	/**
	 * @param port
	 * @return The client facing port number corresponding to port.
	 */
	public static final int getClientFacingPort(int port) {
		return ReconfigurationConfig.getClientFacingPort(port);
	}

	/**
	 * @param port
	 * @return The client facing clear port corresponding to port.
	 */
	public static final int getClientFacingClearPort(int port) {
		return ReconfigurationConfig.getClientFacingClearPort(port);
	}

	/**
	 * @param port
	 * @return The client facing ssl port number corresponding to port.
	 */
	public static final int getClientFacingSSLPort(int port) {
		return ReconfigurationConfig.getClientFacingSSLPort(port);
	}

	private void logEvent(BasicReconfigurationPacket<NodeIDType> event) {
		log.log(Level.FINE, "{0} received {1} from {2}", new Object[] { this,
				event.getSummary(), event.getSender() });
	}

	private void logEvent(BasicReconfigurationPacket<NodeIDType> event,
			Level level) {
		log.log(level, "{0} received {1} from {2}",
				new Object[] { this, event.getSummary(), event.getSender() });
	}
	
	@Override
	public boolean preExecuted(Request request) {
		return request.getServiceName().equals(
				AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString()) 
				|| request.getServiceName().equals(
						AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString())
//				|| request.getServiceName().equals(ReconfigurationConfig.getDefaultServiceName())
				? true: false;
	}

	/**
	 * A wrapper method for handRequestToApp, should only be used by {@link HttpActiveReplica}.
	 */
	@Override
	public boolean handRequestToAppForHttp(Request request, ExecutedCallback callback) {
		return handRequestToApp(request, callback);
	}
	
	/**
	 * A wrapper method for updateDemandStats, should only be used by {@link HttpActiveReplica}.
	 */
	@Override
	public void updateDemandStatsFromHttp(Request request, InetAddress addr) {
		updateDemandStats(request, addr);
	}
}
