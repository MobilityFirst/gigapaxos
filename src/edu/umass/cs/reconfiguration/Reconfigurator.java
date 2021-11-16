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
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.net.ssl.SSLException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.async.RequestCallbackFuture;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.AddressMessenger;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.RTTEstimator;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.ProtocolTaskCreationException;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.http.HttpReconfigurator;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorCallback;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorFunctions;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationpackets.EchoRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.HelloRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest.RequestTypes;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureActiveNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureRCNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ServerReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.CommitWorker;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.ReconfiguratorProtocolTask;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.WaitAckDropEpoch;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.WaitAckStartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.WaitAckStopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationprotocoltasks.WaitPrimaryExecution;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.AggregateDemandProfiler;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.ReconfigureUponActivesChange;
import edu.umass.cs.reconfiguration.dns.DnsReconfigurator;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;
import edu.umass.cs.utils.MyLogger;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.UtilServer;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This class is the main Reconfigurator module. It issues
 *            reconfiguration commands to ActiveReplicas and also responds to
 *            client requests to create or delete names or request the list of
 *            active replicas for a name.
 * 
 *            It relies on the following helper protocol tasks:
 *            {@link WaitAckStopEpoch WaitAckStopEpoch},
 *            {@link WaitAckStartEpoch}, {@link WaitAckDropEpoch},
 *            {@link CommitWorker}, {@link WaitPrimaryExecution}. The last one
 *            is to enable exactly one primary Reconfigurator in the common case
 *            to conduct reconfigurations but ensure that others safely complete
 *            the reconfiguration if the primary fails to do so. CommitWorker is
 *            a worker that is needed to ensure that a paxos-coordinated request
 *            eventually gets committed; we need this property to ensure that a
 *            reconfiguration operation terminates, but paxos itself provides us
 *            no such liveness guarantee.
 * 
 *            This class now supports add/remove operations for the set of
 *            Reconfigurator nodes. This is somewhat tricky, but works
 *            correctly. A detailed, formal description of the protocol is TBD.
 *            The documentation further below in this class explains the main
 *            ideas.
 * 
 */
public class Reconfigurator<NodeIDType> implements
		PacketDemultiplexer<Request>, ReconfiguratorCallback,
		ReconfiguratorFunctions {

	private final SSLMessenger<NodeIDType, JSONObject> messenger;
	private final ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String> protocolExecutor;
	protected final ReconfiguratorProtocolTask<NodeIDType> protocolTask;
	private final RepliconfigurableReconfiguratorDB<NodeIDType> DB;
	private final ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig;
	private final AggregateDemandProfiler demandProfiler = new AggregateDemandProfiler();
	private final CommitWorker<NodeIDType> commitWorker;
	private PendingBatchCreates pendingBatchCreations = new PendingBatchCreates();
	private boolean recovering = true;

	/**
	 * For profiling statistics in {@link DelayProfiler}.
	 */
	public static enum ProfilerKeys {
		/**
		 * 
		 */
		stop_epoch,
		/**
		 * 
		 */
		create,
		/**
		 * 
		 */
		delete,
	};

	/* Any id-based communication requires NodeConfig and Messenger. In general,
	 * the former may be a subset of the NodeConfig used by the latter, so they
	 * are separate arguments. */
	protected Reconfigurator(ReconfigurableNodeConfig<NodeIDType> nc,
			SSLMessenger<NodeIDType, JSONObject> m, boolean startCleanSlate)
			throws IOException {
		this.messenger = m;
		this.consistentNodeConfig = new ConsistentReconfigurableNodeConfig<NodeIDType>(
				nc);
		this.DB = new RepliconfigurableReconfiguratorDB<NodeIDType>(
				new SQLReconfiguratorDB<NodeIDType>(this.messenger.getMyID(),
						this.consistentNodeConfig), getMyID(),
				this.consistentNodeConfig, this.messenger, startCleanSlate);
		// recovery complete at this point
		/* We need to set a callback explicitly in AbstractReplicaCoordinator
		 * instead of just passing self with each coordinateRequest because the
		 * AbstractReconfiguratorDB "app" sometimes returns false, which can be
		 * detected and passed back here as-is, but paxos always expects execute
		 * to return true by design and itself invokes the callback with
		 * handled=true. */
		this.DB.setCallback(this); // no callbacks will happen during recovery

		// protocol executor not needed until recovery complete
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ReconfigurationPacket.PacketType, String>(
				messenger);
		this.protocolTask = new ReconfiguratorProtocolTask<NodeIDType>(
				getMyID(), this);
		// non default types will be registered by spawned tasks
		this.protocolExecutor.register(this.protocolTask.getDefaultTypes(),
				this.protocolTask);
		this.commitWorker = new CommitWorker<NodeIDType>(this.DB, null);
		this.initFinishPendingReconfigurations();
		this.initClientMessenger(false);
		if (ReconfigurationConfig.getClientSSLMode() != SSL_MODES.CLEAR)
			this.initClientMessenger(true);

		assert (this.getClientMessenger() != null || this
				.clientFacingPortIsMyPort());

		// if here, recovery must be complete
		this.DB.setRecovering(this.recovering = false);
		ReconfigurationConfig.log.log(Level.FINE,
				"{0} finished recovery with NodeConfig = {1}",
				new Object[] { this,
						this.consistentNodeConfig.getReconfigurators() });

		// we don't keep a handle to http servers here.
		this.protocolExecutor.submit(new Runnable() {
			@Override
			public void run() {
				initHTTPServer(false);
			}
		});
		
		this.protocolExecutor.submit(new Runnable() {
			@Override
			public void run() {
				initDnsServer();
			}		
		});
	}

	private void initHTTPServer(boolean ssl) {
		if (!Config.getGlobalBoolean(RC.ENABLE_RECONFIGURATOR_HTTP))
			return;
		InetSocketAddress me = this.messenger.getListeningSocketAddress();
		try {
			/* We don't really need to hold a pointer to the HTTP server except
			 * maybe for instrumentation purposes. */
			new HttpReconfigurator(this, new InetSocketAddress(me.getAddress(),
					ReconfigurationConfig.getHTTPPort(me.getPort())), ssl);

			// FIXME: start HTTPS server here as well

		} catch (CertificateException | InterruptedException | SSLException e) {
			if (!(e instanceof InterruptedException)) // close
				e.printStackTrace();
			// throw new IOException(e);
			// eat up exceptions until HTTP server is stable
		}
	}
	
	private void initDnsServer() {
		if (!Config.getGlobalBoolean(RC.ENABLE_RECONFIGURATOR_DNS))
			return;
		new DnsReconfigurator(this);
	}

	/**
	 * This treats the reconfigurator itself as an "active replica" in order to
	 * be able to reconfigure reconfigurator groups.
	 */
	protected ActiveReplica<NodeIDType> getReconfigurableReconfiguratorAsActiveReplica() {
		return new ActiveReplica<NodeIDType>(this.DB,
				this.consistentNodeConfig.getUnderlyingNodeConfig(),
				this.messenger);
	}

	private static final Level debug = Level.FINE;

	private ConcurrentHashMap<String, NIOHeader> headerMap = new ConcurrentHashMap<String, NIOHeader>();
	@Override
	public boolean handleMessage(Request incoming,
			edu.umass.cs.nio.nioutils.NIOHeader header) {
		try {
			// otherwise demultiplexer wouldn't come here
			assert (incoming instanceof BasicReconfigurationPacket) : incoming
					.getSummary();
			@SuppressWarnings("unchecked")
			ReconfigurationPacket.PacketType rcType = ((BasicReconfigurationPacket<NodeIDType>) incoming)
					.getReconfigurationPacketType();
			ReconfigurationConfig.log.log(debug, "{0} received {1} {2}", new Object[] { this, rcType,
					incoming.getSummary(ReconfigurationConfig.log.isLoggable(debug)) });

			// try handling as reconfiguration packet through protocol task
			@SuppressWarnings("unchecked")
			// checked by assert above
			BasicReconfigurationPacket<NodeIDType> rcPacket = (BasicReconfigurationPacket<NodeIDType>) incoming;
			
			if ( rcPacket instanceof HelloRequest ){
				headerMap.put(rcPacket.getInitiator().toString(), header);
			}
			// all packets are handled through executor, nice and simple
			if (!this.protocolExecutor.handleEvent(rcPacket))
				// do nothing
				ReconfigurationConfig.log.log(debug, MyLogger.FORMAT[2],
						new Object[] { this, "unable to handle packet",
								incoming.getSummary(ReconfigurationConfig.log.isLoggable(debug)) });
		} catch (ProtocolTaskCreationException e) {
			ReconfigurationConfig.log.severe(this + " incurred exception " + e.getMessage()
					+ " while trying to handle message " + incoming);
			e.printStackTrace();
		}
		return false; // neither reconfiguration packet nor app request
	}

	/**
	 * @return Packet types handled by Reconfigurator. Refer
	 *         {@link ReconfigurationPacket}.
	 */
	public Set<ReconfigurationPacket.PacketType> getPacketTypes() {
		return this.protocolTask.getEventTypes();
	}

	public String toString() {
		return "RC." + getMyID();
	}

	/**
	 * Close gracefully.
	 */
	public void close() {
		this.commitWorker.close();
		this.protocolExecutor.stop();
		this.messenger.stop();
		this.DB.close();
		HttpReconfigurator.closeAll();
		ReconfigurationConfig.log.log(Level.INFO, "{0} closing with nodeConfig = {1}", new Object[] {
				this, this.consistentNodeConfig });
	}

	/* *********** Start of protocol task handler methods ***************** */
	/**
	 * Incorporates demand reports (possibly but not necessarily with replica
	 * coordination), checks for reconfiguration triggers, and initiates
	 * reconfiguration if needed.
	 * 
	 * @param report
	 * @param ptasks
	 * @return MessagingTask, typically null. No protocol tasks spawned.
	 */
	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType, ?>[] handleDemandReport(
			DemandReport<NodeIDType> report,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		ReconfigurationConfig.log.log(Level.FINEST, "{0} received {1} {2}", new Object[] { this,
				report.getType(), report });
		/* Forward if I am not responsible and return. Demand reports can be
		 * misdirected if actives have an inconsistent view of the current set
		 * of reconfigurators. */
		if (!this.consistentNodeConfig.getReplicatedReconfigurators(
				report.getServiceName()).contains(this.getMyID())) {
			return new GenericMessagingTask<NodeIDType, DemandReport<NodeIDType>>(
					(NodeIDType) (Util.selectRandom(this.consistentNodeConfig
							.getReplicatedReconfigurators(report
									.getServiceName()))), report).toArray();
		}
		if (report.needsCoordination())
			this.DB.handleIncoming(report, null); // coordinated
		else
			this.updateDemandProfile(report); // no coordination
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(report.getServiceName());
		if (record != null)
			// coordinate and commit reconfiguration intent
			this.initiateReconfiguration(report.getServiceName(), record,
					shouldReconfigure(report.getServiceName()), null, null,
					null, null, null, null, ReconfigurationConfig.ReconfigureUponActivesChange.DEFAULT); // coordinated
		trimAggregateDemandProfile();
		return null; // never any messaging or ptasks
	}

	private boolean isLegitimateCreateRequest(CreateServiceName create) {
		if (!create.isBatched())
			return true;
		return this.consistentNodeConfig.checkSameGroup(create.nameStates
				.keySet());
	}

	private CreateServiceName[] makeCreateServiceNameBatches(
			CreateServiceName batchedCreate) {
		CreateServiceName[] creates = ReconfigurationConfig
				.makeCreateNameRequest(batchedCreate.nameStates, Config
						.getGlobalInt(ReconfigurationConfig.RC.MAX_BATCH_SIZE),
						Util.setToStringSet(this.consistentNodeConfig
								.getReconfigurators()));
		for (int i = 0; i < creates.length; i++) {
			creates[i] = new CreateServiceName(creates[i].nameStates,
					batchedCreate);
			assert (this.isLegitimateCreateRequest(creates[i]));
		}
		return creates;
	}

	class BatchCreateJobs {
		final long creationTime;
		final String headName;
		final CreateServiceName original;
		final Set<CreateServiceName> parts = new HashSet<CreateServiceName>();
		final Set<String> remaining = new HashSet<String>();

		BatchCreateJobs(String headName, CreateServiceName[] creates,
				CreateServiceName original) {
			creationTime = System.currentTimeMillis();
			this.headName = headName;
			this.original = original;
			for (CreateServiceName create : creates) {
				parts.add(create);
				remaining.add(create.getServiceName());
			}
		}

		int totalNames() {
			int total = 0;
			for (CreateServiceName part : parts)
				total += part.size();
			return total;
		}
	}

	class PendingBatchCreates {
		/**
		 * Splits a batch creation jobs into smaller batch creates each of which
		 * is consistent. The set {@link BatchCreateJobs#parts} holds those
		 * parts.
		 */
		private ConcurrentHashMap<String, BatchCreateJobs> consistentBatchCreateJobs = new ConcurrentHashMap<String, BatchCreateJobs>();
		/**
		 * Maps the headname of a part to the overall head name of the original
		 * client-issued batch create request.
		 */
		private ConcurrentHashMap<String, String> headnameToOverallHeadname = new ConcurrentHashMap<String, String>();
	}

	private void failLongPendingBatchCreate(String name, long timeout,
			Callback<Request,ReconfiguratorRequest> callback) {
		for (Iterator<String> strIter = this.pendingBatchCreations.consistentBatchCreateJobs
				.keySet().iterator(); strIter.hasNext();) {
			String headName = strIter.next();
			BatchCreateJobs jobs = this.pendingBatchCreations.consistentBatchCreateJobs
					.get(headName);
			if (System.currentTimeMillis() - jobs.creationTime > timeout) {
				Map<String, String> nameStates = jobs.original.nameStates;
				Set<String> failedCreates = new HashSet<String>();
				for (CreateServiceName part : jobs.parts) {
					if (jobs.remaining.contains(part.getServiceName())) {
						failedCreates.addAll(part.nameStates.keySet());
						nameStates.remove(part.nameStates.keySet());
					}
				}
				// this.sendClientReconfigurationPacket
				callback.processResponse(new CreateServiceName(nameStates,
						failedCreates, jobs.parts.iterator().next())
						.setFailed()
						.setResponseMessage(
								"Batch create failed to create the names listed in the field "
										+ CreateServiceName.Keys.FAILED_CREATES
												.toString()
										+ (!nameStates.isEmpty() ? " (but did successfully create the names in the field "
												+ CreateServiceName.Keys.NAME_STATE_ARRAY
												+ ")"
												: "")));
			}
		}
	}

	private static final long BATCH_CREATE_TIMEOUT = 30 * 1000;

	private CreateServiceName updateAndCheckComplete(String batchCreateHeadName) {
		String headName = this.pendingBatchCreations.headnameToOverallHeadname
				.remove(batchCreateHeadName);
		if (headName == null)
			return null;
		// else
		BatchCreateJobs jobs = this.pendingBatchCreations.consistentBatchCreateJobs
				.get(headName);
		jobs.remaining.remove(batchCreateHeadName);
		if (jobs.remaining.isEmpty()) {
			ReconfigurationConfig.log.log(Level.INFO,
					"{0} returning completed batch create with head name {1} with {2} parts and {3} total constituent names",
					new Object[] { this, headName, jobs.parts.size(),
							jobs.totalNames() });
			return this.pendingBatchCreations.consistentBatchCreateJobs
					.remove(headName).original;
		}
		// else
		ReconfigurationConfig.log.log(Level.INFO,
				"{0} completed batch create part job with head name {1}; jobs remaining = {2}",
				new Object[] { this, batchCreateHeadName, jobs.remaining });
		return null;
	}

	private String updateAndCheckComplete(StartEpoch<NodeIDType> startEpoch) {
		boolean hasBatchCreatePartJobs = this.pendingBatchCreations.headnameToOverallHeadname
				.containsKey(startEpoch.getServiceName());
		CreateServiceName original = this.updateAndCheckComplete(startEpoch
				.getServiceName());
		if (original != null)
			return original.getServiceName();
		// remaining batch job parts
		else if (hasBatchCreatePartJobs)
			return null;
		// no batch job parts to begin with
		else
			return startEpoch.getServiceName();
	}

	/**
	 * Create service name is a special case of reconfiguration where the
	 * previous group is non-existent.
	 * 
	 * @param create
	 * @param ptasks
	 * @return Messaging task, typically null. No protocol tasks spawned.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleCreateServiceName(
			CreateServiceName create,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		return this.handleCreateServiceName(create, ptasks, defaultCallback);
	}

	/**
	 * @param create
	 * @param ptasks
	 * @param callback
	 * @return Messaging task, typically null. No protocol tasks spawned.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleCreateServiceName(
			CreateServiceName create,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks,
			Callback<Request, ReconfiguratorRequest> callback) {
		ReconfigurationConfig.log.log(!this.pendingBatchCreations.headnameToOverallHeadname
				.containsKey(create.getServiceName()) ? Level.INFO : Level.INFO,
				"{0} received {1} from client {2} {3}",
				new Object[] {
						this,
						create.getSummary(),
						create.getCreator(),
						create.isForwarded() ? "from reconfigurator "
								+ create.getSender() : "" });

		/* If create is batched but all names do not belong to the same
		 * reconfigurator group, we automatically split the batch into smaller
		 * batches, issue the constituent creates, wait for the corresponding
		 * responses, and send a single success or failure response (upon a
		 * timeout) back to the issuing client. If it is a legitimate batch
		 * create, i.e., all names hash to the same reconfigurator group, then
		 * we don't have to maintain any state and can try to commit it like a
		 * usual unbatched create. */
		if (create.isBatched()
				&& !this.isLegitimateCreateRequest(create)
				&& !this.pendingBatchCreations.consistentBatchCreateJobs
						.containsKey(create.getServiceName())
				&& !this.pendingBatchCreations.headnameToOverallHeadname
						.containsKey(create.getServiceName())) {
			BatchCreateJobs jobs = null;
			this.pendingBatchCreations.consistentBatchCreateJobs.put(
					create.getServiceName(),
					jobs = new BatchCreateJobs(create.getServiceName(), this
							.makeCreateServiceNameBatches(create), create));
			ReconfigurationConfig.log.log(Level.INFO,
					"{0} inserted batch create with {1} parts ({2}) and {3} total constituent names",
					new Object[] { this, jobs.parts.size(), jobs.remaining,
							jobs.totalNames() });
			for (CreateServiceName part : jobs.parts) {
				this.pendingBatchCreations.headnameToOverallHeadname.put(
						part.getServiceName(), create.getServiceName());
				this.handleCreateServiceName(part, ptasks, callback); // recursive
			}

			// fail if it doesn't complete within timeout
			this.protocolExecutor.scheduleSimple(new Runnable() {
				@Override
				public void run() {
					Reconfigurator.this.failLongPendingBatchCreate(
							create.getServiceName(), BATCH_CREATE_TIMEOUT,
							callback);
				}
			}, BATCH_CREATE_TIMEOUT, TimeUnit.MILLISECONDS);
			return null;
		}
		// responses for forwarded batch create jobs coming back
		else if (this.pendingBatchCreations.headnameToOverallHeadname
				.containsKey(create.getServiceName())
				&& create.isForwarded()
				&& create.getForwader().equals(
						this.consistentNodeConfig
								.getNodeSocketAddress(getMyID()))) {
			assert (!create.isRequest()); // response
			CreateServiceName original = this.updateAndCheckComplete(create
					.getServiceName());
			if (original != null)
				// all done, send success response to client
				// this.sendClientReconfigurationPacket
				callback.processResponse(original.getHeadOnly()
						.setResponseMessage(
								"Successfully batch-created " + original.size()
										+ " names with head name "
										+ original.getServiceName()));
			// else wait for more responses
			return null;
		}

		// quick reject for bad batched create
		if (!this.isLegitimateCreateRequest(create))
			// this.sendClientReconfigurationPacket
			callback.processResponse(create
					.setFailed()
					.setResponseMessage(
							"The names in this create request do not all map to the same reconfigurator group"));

		if (this.processRedirection(create)) {
			if (callback != this.defaultCallback)
				this.callbacksCRP.put(getCRPKey(create), callback);
			return null;
		}
		// else I am responsible for handling this (possibly forwarded) request

		/* Commit initial "reconfiguration" intent. If the record can be created
		 * at all default actives, this operation will succeed, and fail
		 * otherwise; in either case, the reconfigurators will have an
		 * eventually consistent view of whether the record got created or not.
		 * 
		 * Note that we need to maintain this consistency property even when
		 * nodeConfig may be in flux, i.e., different reconfigurators may have
		 * temporarily different views of what the current set of
		 * reconfigurators is. But this is okay as app record creations (as well
		 * as all app record reconfigurations) are done by an RC paxos group
		 * that agrees on whether the creation completed or not; this claim is
		 * true even if that RC group is itself undergoing reconfiguration. If
		 * nodeConfig is outdated at some node, that only affects the choice of
		 * active replicas below, not their consistency. */
		ReconfigurationConfig.log.log(Level.FINE, "{0} processing {1} from creator {2} {3}",
				new Object[] {
						this,
						create.getSummary(),
						create.getCreator(),
						create.getForwader() != null ? " forwarded by "
								+ create.getForwader() : "" });

		ReconfigurationRecord<NodeIDType> record = null;
		/* Check if record doesn't already exist. This check is meaningful only
		 * for unbatched create requests. For batched creates, we optimistically
		 * assume that none of the batched names already exist and let the
		 * create fail later if that is not the case. */
		if ((record = this.DB.getReconfigurationRecord(create.getServiceName())) == null) {
			if (callback != this.defaultCallback)
				this.callbacksCRP.put(getCRPKey(create), callback);
			
			
			this.initiateReconfiguration(create.getServiceName(), record, 
					create.getInitGroup()!=null?getNodeIDsFromSocketAddresses(create.getInitGroup()):
						this.consistentNodeConfig.getReplicatedActives(create.getServiceName()), 
						create.getCreator(), create.getMyReceiver(),
						create.getForwader(), create.getInitialState(), 
						create.getNameStates(), null, create.getReconfigureUponActivesChangePolicy());
		}
		else if(!record.isReady()) {
			// drop silently so sender can time out
		}

		// record already exists, so return error message
		else if (record.isReady())
			// this.sendClientReconfigurationPacket
			callback.processResponse(create
					.setFailed(
							ClientReconfigurationPacket.ResponseCodes.DUPLICATE_ERROR)
					.setResponseMessage(
							"Can not (re-)create an already "
									+ (record.isDeletePending() ? "deleted name "
											+ create.getServiceName()
											+ " until "
											+ ReconfigurationConfig
													.getMaxFinalStateAge()
											/ 1000
											+ " seconds have elapsed after the most recent deletion."
											: "existing name "
													+ create.getServiceName()
													+ ".")));
		return null;
	}
	
	private Set<NodeIDType> getNodeIDsFromSocketAddresses(Set<InetSocketAddress> socketAddresses)
	{
		Map<NodeIDType, InetSocketAddress> nodeidMap 
							= this.consistentNodeConfig.getActiveReplicasReadOnly();
		
		Set<NodeIDType> nodeIdSet = new HashSet<NodeIDType>();
		Iterator<NodeIDType> iter = nodeidMap.keySet().iterator();
		
		while(iter.hasNext())
		{
			NodeIDType id = iter.next();
			if( socketAddresses.contains(nodeidMap.get(id)) )
			{
				nodeIdSet.add(id);
			}
		}
		return nodeIdSet;
	}

	private static final boolean TWO_PAXOS_RC = Config
			.getGlobalBoolean(ReconfigurationConfig.RC.TWO_PAXOS_RC);

	/**
	 * Simply hand over DB request to DB. The only type of RC record that can
	 * come here is one announcing reconfiguration completion. Reconfiguration
	 * initiation messages are derived locally and coordinated through paxos,
	 * not received from outside.
	 * 
	 * @param rcRecReq
	 * @param ptasks
	 * @return Messaging task, typically null unless TWO_PAXOS_RC.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleRCRecordRequest(
			RCRecordRequest<NodeIDType> rcRecReq,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		ReconfigurationConfig.log.log(Level.FINE, "{0} receievd {1}",
				new Object[] { this, rcRecReq.getSummary() });

		GenericMessagingTask<NodeIDType, ?> mtask = null;
		// for NC changes, prev drop complete signifies everyone's on board
		if (rcRecReq.isReconfigurationPrevDropComplete()
				&& rcRecReq.getServiceName().equals(
						AbstractReconfiguratorDB.RecordNames.RC_NODES
								.toString())
				&& this.changeRCNodeConfigAtActives(rcRecReq)
				)
			this.sendReconfigureRCNodeConfigConfirmationToInitiator(rcRecReq);
		else if (rcRecReq.isReconfigurationPrevDropComplete()
				&& rcRecReq.getServiceName().equals(
						AbstractReconfiguratorDB.RecordNames.AR_NODES
								.toString()))
			this.sendReconfigureActiveNodeConfigConfirmationToInitiator(rcRecReq);

		// single paxos reconfiguration allowed only for non-RC-group names
		else if (!TWO_PAXOS_RC
				&& !this.DB.isRCGroupName(rcRecReq.getServiceName())
				&& !rcRecReq.isNodeConfigChange()) {
			if (rcRecReq.isReconfigurationComplete()
					|| rcRecReq.isReconfigurationPrevDropComplete()) {
				if (rcRecReq.getInitiator().equals(getMyID()))
					mtask = new GenericMessagingTask<NodeIDType, RCRecordRequest<NodeIDType>>(
							getOthers(this.consistentNodeConfig.getReplicatedReconfigurators(rcRecReq
									.getServiceName())), rcRecReq);
				// no coordination
				boolean handled = this.DB.execute(rcRecReq);
				if (handled)
					this.garbageCollectPendingTasks(rcRecReq);
			}
		} else {
			// commit until committed by default
			this.repeatUntilObviated(rcRecReq);
		}

		return mtask != null ? mtask.toArray() : null;
	}

	Object[] getOthers(Set<NodeIDType> nodes) {
		Set<NodeIDType> others = new HashSet<NodeIDType>();
		for (NodeIDType node : nodes)
			if (!node.equals(getMyID()))
				others.add(node);
		return others.toArray();
	}

	/**
	 * We need to ensure that both the stop/drop at actives happens atomically
	 * with the removal of the record at reconfigurators. To accomplish this, we
	 * first mark the record as stopped at reconfigurators, then wait for the
	 * stop/drop tasks to finish, and finally coordinate the completion
	 * notification so that reconfigurators can completely remove the record
	 * from their DB.
	 * 
	 * @param delete
	 * @param ptasks
	 * @return Messaging task, typically null. No protocol tasks spawned.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleDeleteServiceName(
			DeleteServiceName delete,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		return this.handleDeleteServiceName(delete, ptasks, defaultCallback);
	}

	/**
	 * @param delete
	 * @param ptasks
	 * @param callback
	 * @return Messaging task, typically null. No protocol tasks spawned.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleDeleteServiceName(
			DeleteServiceName delete,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks,
			Callback<Request,ReconfiguratorRequest> callback) {
		ReconfigurationConfig.log.log(Level.INFO, "{0} received {1} from creator {2}", new Object[] {
				this, delete.getSummary(), delete.getCreator() });

		if (this.processRedirection(delete)) {
			if (callback != defaultCallback)
				this.callbacksCRP.put(getCRPKey(delete), callback);
			return null;
		}
		ReconfigurationConfig.log.log(Level.FINE,
				"{0} processing delete request {1} from creator {2} {3}",
				new Object[] {
						this,
						delete.getSummary(),
						delete.getCreator(),
						delete.getForwader() != null ? " forwarded by "
								+ delete.getForwader() : "" });
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(delete.getServiceName());
		RCRecordRequest<NodeIDType> rcRecReq = null;
		// coordinate delete intent, response will be sent in callback
		if (record != null
				&& this.isReadyForReconfiguration(
						rcRecReq = new RCRecordRequest<NodeIDType>(this
								.getMyID(), this.formStartEpoch(
								delete.getServiceName(), record, null,
								delete.getCreator(), delete.getMyReceiver(),
								delete.getForwader(), null, null, null),
								RequestTypes.RECONFIGURATION_INTENT), record)) {
			if (callback != defaultCallback)
				this.callbacksCRP.put(getCRPKey(delete), callback);
			this.DB.handleIncoming(rcRecReq, null);
			return null;
		}
		// WAIT_DELETE state also means success
		else if (this.isWaitingDelete(delete)) {
			// this.sendClientReconfigurationPacket
			callback.processResponse(delete.setResponseMessage(delete
					.getServiceName() + " already pending deletion"));
			return null;
		}
		// else failure
		// this.sendClientReconfigurationPacket
		callback.processResponse(delete
				.setFailed(
						ClientReconfigurationPacket.ResponseCodes.NONEXISTENT_NAME_ERROR)
				.setResponseMessage(
						delete.getServiceName()
								+ (record != null ? " is being reconfigured and can not be deleted just yet."
										: " does not exist")));
		ReconfigurationConfig.log.log(Level.FINE,
				"{0} discarded {1} because RC record is not reconfiguration ready.",
				new Object[] { this, delete.getSummary() });
		return null;
	}

	private boolean isWaitingDelete(DeleteServiceName delete) {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(delete.getServiceName());
		return record != null && record.getState().equals(RCStates.WAIT_DELETE);
	}

	private static final String ANYCAST_NAME = Config
			.getGlobalString(RC.SPECIAL_NAME);
	private static final String BROADCAST_NAME = Config
			.getGlobalString(RC.BROADCAST_NAME);

	private static final long CRP_GC_TIMEOUT = 8000;

	/**
	 * Client reconfiguration packet callbacks. We need callbacks primarily to
	 * support other front-end APIs like HTTP and create/delete-like
	 * asynchronous requests that return only after coordination.
	 */
	private final GCConcurrentHashMap<String, Callback<Request,ReconfiguratorRequest>> callbacksCRP = new GCConcurrentHashMap<String, Callback<Request,ReconfiguratorRequest>>(
			new GCConcurrentHashMapCallback() {
				@Override
				public void callbackGC(Object key, Object value) {
					assert (value instanceof RequestCallback);
					ReconfigurationConfig.log.log(Level.INFO, "{0} timing out {1}:{2}", new Object[] {
							this, key, value });
				}
			}, CRP_GC_TIMEOUT);

	/**
	 * Default response is to simply invoke
	 * {@link #sendClientReconfigurationPacket(ClientReconfigurationPacket)}.
	 */
	private final Callback<Request,ReconfiguratorRequest> defaultCallback = new Callback<Request,ReconfiguratorRequest>() {

		@Override
		public ReconfiguratorRequest processResponse(Request response) {
			if (response != null
					&& response instanceof ClientReconfigurationPacket) {
				Reconfigurator.this
						.sendClientReconfigurationPacket((ClientReconfigurationPacket) response);
				return (ClientReconfigurationPacket) response;
			}
			else
				assert (false);
			return null;
		}
	};

	/**
	 * @return Default callback for auto-invoked methods. Meant only for
	 *         internal use.
	 */
	public Callback<Request,ReconfiguratorRequest> getDefaultCallback() {
		return this.defaultCallback;
	}

	private static final String getCRPKey(ClientReconfigurationPacket crp) {
		return crp.getRequestType() + ":" + crp.getServiceName() + ":"
				+ crp.getCreator();
	}

	/**
	 * This method simply looks up and returns the current set of active
	 * replicas. Maintaining this state consistently is the primary and only
	 * existential purpose of reconfigurators.
	 * 
	 * @param request
	 * @param ptasks
	 * @return Messaging task returning the set of active replicas to the
	 *         requestor. No protocol tasks spawned.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleRequestActiveReplicas(
			RequestActiveReplicas request,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		return this.handleRequestActiveReplicas(request, ptasks,
				defaultCallback);
	}

	/**
	 * @param request
	 * @param ptasks
	 * @param callback
	 * @return Messaging task returning the set of active replicas to the
	 *         requestor. No protocol tasks spawned.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleRequestActiveReplicas(
			RequestActiveReplicas request,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks,
			Callback<Request,ReconfiguratorRequest> callback) {

		ReconfigurationConfig.log.log(Level.INFO,
				"{0} received {1} {2} from client {3} {4}",
				new Object[] {
						this,
						request.isRequest() ? "" : "RESPONSE",
						request.getSummary(),
						request.getCreator(),
						request.isForwarded() ? "from reconfigurator "
								+ request.getSender() : "" });
		if (request.getServiceName().equals(ANYCAST_NAME)) {
			// this.sendClientReconfigurationPacket
			callback.processResponse(request.setActives(modifyPortsForSSL(
					this.consistentNodeConfig.getRandomActiveReplica(),
					receivedOnSSLPort(request))));
			return null;
		} else if (request.getServiceName().equals(BROADCAST_NAME)) {
			// this.sendClientReconfigurationPacket
			callback.processResponse(request.setActives(modifyPortsForSSL(
					this.consistentNodeConfig.getActiveReplicaSocketAddresses(),
					receivedOnSSLPort(request))));
			return null;
		}

		if (this.processRedirection(request)) {
			/**
			 * Asynchronous step here, but we enqueue only if the callback is
			 * non-default because the default callback action of sending
			 * responses back to the client will be done anyway if no explicit
			 * callback is found. This way, we avoid unnecessary
			 * enqueue/dequeue/GC operations in the common case.
			 */
			if (callback != this.defaultCallback)
				this.callbacksCRP.put(getCRPKey(request), callback);
			return null;
		}
		
		if (request.needsCoordination()
				&& this.DB.handleIncoming(request, this))
			return null;

		// else at an appropriate replica
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(request.getServiceName());
		if (record == null || record.getActiveReplicas() == null
				|| record.isDeletePending()) {
			ReconfigurationConfig.log.log(Level.FINE,
					"{0} returning null active replicas for name {1}; record = {2}",
					new Object[] { this, request.getServiceName(), record });
			// I am responsible but can't find actives for the name
			String responseMessage = "No state found for name "
					+ request.getServiceName();
			request.setResponseMessage(responseMessage
					+ " probably because the name has not yet been created or is pending deletion");
			// this.sendClientReconfigurationPacket
			callback.processResponse(this
					.amReceiver(request
							.setFailed(
									ClientReconfigurationPacket.ResponseCodes.NONEXISTENT_NAME_ERROR)
							.makeResponse()) ? (RequestActiveReplicas) request
					.setHashRCs(modifyPortsForSSL(this
							.getSocketAddresses(this.consistentNodeConfig
									.getReplicatedReconfigurators(request
											.getServiceName())),
							receivedOnSSLPort(request))) : request);
			return null;
		}

		// else
		Set<InetSocketAddress> activeIPs = new HashSet<InetSocketAddress>();
		/* It is important to ensure that the mapping between active nodeIDs and
		 * their socket addresses does not change or changes very infrequently.
		 * Otherwise, in-flux copies of nodeConfig can produce wrong answers
		 * here. This assumption is reasonable and will hold as long as active
		 * nodeIDs are re-used with the same socket address or removed and
		 * re-added after a long time if at all by which time all nodes have
		 * forgotten about the old id-to-address mapping. */
		for (NodeIDType node : record.getActiveReplicas())
			activeIPs.add(this.consistentNodeConfig.getNodeSocketAddress(node));
		// to support different client facing ports
		request.setActives(modifyPortsForSSL(activeIPs,
				receivedOnSSLPort(request)));
		// this.sendClientReconfigurationPacket
		callback.processResponse(request.makeResponse());
		/* We message using sendActiveReplicasToClient above as opposed to
		 * returning a messaging task below because protocolExecutor's messenger
		 * may not be usable for client facing requests. */
		return null;
	}

	private boolean amReceiver(ClientReconfigurationPacket response) {
		InetSocketAddress incoming = response.getMyReceiver();
		InetSocketAddress me = this.consistentNodeConfig.getNodeSocketAddress(this.getMyID());
		return me.equals(incoming) 
				
				||
				me.getAddress().equals(incoming.getAddress()) &&  
				ReconfigurationConfig.getClientFacingClearPort(me.getPort())==incoming.getPort() 
				
				||
				me.getAddress().equals(incoming.getAddress()) &&  
				ReconfigurationConfig.getClientFacingSSLPort(me.getPort())==incoming.getPort();

	}

	/**
	 * Handles a request to add or delete a reconfigurator from the set of all
	 * reconfigurators in NodeConfig. The reconfiguration record corresponding
	 * to NodeConfig is stored in the RC records table and the
	 * "active replica state" or the NodeConfig info itself is stored in a
	 * separate NodeConfig table in the DB.
	 * 
	 * @param changeRC
	 * @param ptasks
	 * @return Messaging task typically null. No protocol tasks spawned.
	 */
	public GenericMessagingTask<?, ?>[] handleReconfigureRCNodeConfig(
			ReconfigureRCNodeConfig<NodeIDType> changeRC,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		assert (changeRC.getServiceName()
				.equals(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString()));
		ReconfigurationConfig.log.log(Level.INFO,
				"\n\n{0}\n{1} received {2} request {3} from initiator {4}\n{5}",
				new Object[] { separator, this, changeRC.getType(),
						changeRC.getSummary(), changeRC.getIssuer(), separator });
		if (!this.isPermitted(changeRC)) {
			String errorMessage = " Impermissible node config change request";
			ReconfigurationConfig.log.severe(this + errorMessage + ": " + changeRC);
			// this.sendRCReconfigurationErrorToInitiator(changeRC).setFailed().setResponseMessage(errorMessage);
			return (new GenericMessagingTask<InetSocketAddress, ServerReconfigurationPacket<NodeIDType>>(
					changeRC.getIssuer(), changeRC.setFailed()
							.setResponseMessage(errorMessage))).toArray();
		}
		// check first if NC is ready for reconfiguration
		ReconfigurationRecord<NodeIDType> ncRecord = this.DB
				.getReconfigurationRecord(changeRC.getServiceName());
		if (ncRecord == null)
			return null; // possible if startCleanSlate

		if (!ncRecord.isReady()) {
			String errorMessage = " Trying to conduct concurrent node config changes";
			ReconfigurationConfig.log.warning(this + errorMessage + ": " + changeRC);
			return (new GenericMessagingTask<InetSocketAddress, ServerReconfigurationPacket<NodeIDType>>(
					changeRC.getIssuer(), changeRC.setFailed()
							.setResponseMessage(errorMessage))).toArray();
		}
		// else try to reconfigure even though it may still fail
		Set<NodeIDType> curRCs = ncRecord.getActiveReplicas();
		Set<NodeIDType> newRCs = new HashSet<NodeIDType>(curRCs);
		newRCs.addAll(changeRC.getAddedNodeIDs());
		newRCs.removeAll(changeRC.getDeletedNodeIDs());
		// will use the nodeConfig before the change below.
		if (changeRC.newlyAddedNodes != null || changeRC.deletedNodes != null)
			this.initiateReconfiguration(
					AbstractReconfiguratorDB.RecordNames.RC_NODES.toString(),
					ncRecord,
					newRCs, // this.consistentNodeConfig.getNodeSocketAddress
					(changeRC.getIssuer()), changeRC.getMyReceiver(), null,
					null, null, changeRC.newlyAddedNodes, ReconfigurationConfig.ReconfigureUponActivesChange.DEFAULT);
		return null;
	}

	/**
	 * @param changeActives
	 * @param ptasks
	 * @return Messaging task if any.
	 */
	public GenericMessagingTask<?, ?>[] handleReconfigureActiveNodeConfig(
			ReconfigureActiveNodeConfig<NodeIDType> changeActives,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {

		assert (changeActives.getServiceName()
				.equals(AbstractReconfiguratorDB.RecordNames.AR_NODES
						.toString()));
		ReconfigurationConfig.log.log(Level.INFO,
				"\n\n{0}\n{1} received {2} request {3} from initiator {4}\n{5}",
				new Object[] { separator, this, changeActives.getType(),
						changeActives.getSummary(), changeActives.getIssuer(),
						separator });

		if (!this.isPermitted(changeActives)) {
			String errorMessage = " Impermissible node config change request";
			ReconfigurationConfig.log.severe(this + errorMessage + ": " + changeActives);
			// this.sendRCReconfigurationErrorToInitiator(changeRC).setFailed().setResponseMessage(errorMessage);
			return (new GenericMessagingTask<InetSocketAddress, ServerReconfigurationPacket<NodeIDType>>(
					changeActives.getIssuer(), changeActives.setFailed()
							.setResponseMessage(errorMessage))).toArray();
		} else if (this.nothingToDo(changeActives)) {
			String errorMessage = " Requested node additions or deletions already in place";
			ReconfigurationConfig.log.log(Level.INFO, "{0} {1} : {2}", new Object[] { this,
					errorMessage, changeActives });
			// do not setFailed() in this case
			return (new GenericMessagingTask<InetSocketAddress, ServerReconfigurationPacket<NodeIDType>>(
					changeActives.getIssuer(),
					changeActives.setResponseMessage(errorMessage))).toArray();

		}
		// check first if NC is ready for reconfiguration
		ReconfigurationRecord<NodeIDType> activeNCRecord = this.DB
				.getReconfigurationRecord(changeActives.getServiceName());
		if (activeNCRecord == null)
			return null; // possible if startCleanSlate

		if (!activeNCRecord.isReady()) {
			String errorMessage = " Trying to conduct concurrent node config changes";
			ReconfigurationConfig.log.warning(this + errorMessage + ": " + changeActives.getSummary()
					+ "\n when activeNCRecord = " + activeNCRecord.getSummary());
			return (new GenericMessagingTask<InetSocketAddress, ServerReconfigurationPacket<NodeIDType>>(
					changeActives.getIssuer(), changeActives.setFailed()
							.setResponseMessage(errorMessage))).toArray();
		}

		// else try to reconfigure even though it may still fail
		Set<NodeIDType> curActives = activeNCRecord.getActiveReplicas();
		Set<NodeIDType> newActives = new HashSet<NodeIDType>(curActives);
		newActives.addAll(changeActives.getAddedNodeIDs());
		newActives.removeAll(changeActives.getDeletedNodeIDs());
		// will use the nodeConfig before the change below.
		if (changeActives.newlyAddedNodes != null
				|| changeActives.deletedNodes != null)
			this.initiateReconfiguration(
					AbstractReconfiguratorDB.RecordNames.AR_NODES.toString(),
					activeNCRecord,
					newActives, // this.consistentNodeConfig.getNodeSocketAddress
					(changeActives.getIssuer()), changeActives.getMyReceiver(),
					null, null, null, changeActives.newlyAddedNodes, ReconfigurationConfig.ReconfigureUponActivesChange.DEFAULT);

		return null;
	}

	private boolean isPermitted(
			ReconfigureActiveNodeConfig<NodeIDType> changeActives) {
		return changeActives.hasDeletedNodes() ? changeActives.deletedNodes
				.size() == 1 : true;
	}

	private boolean nothingToDo(
			ReconfigureActiveNodeConfig<NodeIDType> changeActives) {
		boolean nothing = true;
		nothing = nothing
				&& (changeActives.hasAddedNodes() ? this.consistentNodeConfig
						.getActiveReplicas().containsAll(
								changeActives.newlyAddedNodes.keySet()) : true);
		if (changeActives.hasDeletedNodes())
			for (NodeIDType node : changeActives.deletedNodes)
				nothing = nothing
						&& !this.consistentNodeConfig.getActiveReplicas()
								.contains(node);
		return nothing;
	}

	/**
	 * Reconfiguration is initiated using a callback because the intent to
	 * conduct a reconfiguration must be persistently committed before
	 * initiating the reconfiguration. Otherwise, the failure of say the
	 * initiating reconfigurator can leave an active replica group stopped
	 * indefinitely. Exactly one reconfigurator, the one that proposes the
	 * request initiating reconfiguration registers the callback. This
	 * initiating reconfigurator will spawn a WaitAckStopEpoch task when the
	 * initiating request is locally executed. The other replicas only spawn a
	 * WaitPrimaryExecution task as a double check that the initiating
	 * reconfigurator does complete the reconfiguration; if it does not, they
	 * will follow up with their own attempt after a timeout. This works because
	 * all three steps: WaitAckStopEpoch, WaitAckStartEpoch, and
	 * WaitAckDropEpoch are idempotent operations.
	 * 
	 * A reconfiguration attempt can still get stuck if all reconfigurators
	 * crash or the only reconfigurators that committed the intent crash. So, a
	 * replica recovery procedure should ensure that replicas eventually execute
	 * committed but unexecuted requests. This naturally happens with paxos.
	 */
	@Override
	public void executed(Request request, boolean handled) {
		if (this.isRecovering()
				&& !((request instanceof RCRecordRequest<?>) && ((RCRecordRequest<?>) request)
						.isReconfigurationMerge()))
			return; // no messaging during recovery
		BasicReconfigurationPacket<?> rcPacket = null;
		try {
			rcPacket = ReconfigurationPacket.getReconfigurationPacket(request,
					getUnstringer());
		} catch (JSONException e) {
			if (!request.toString().equals(Request.NO_OP))
				e.printStackTrace();
		}
		if (rcPacket == null
				|| (!rcPacket.getType().equals(
						ReconfigurationPacket.PacketType.RC_RECORD_REQUEST)
						&& !rcPacket.getType().equals(
								ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS)))
			return;
		
		if (request instanceof RequestActiveReplicas) {
			this.handleRequestActiveReplicas(
					((RequestActiveReplicas) request).unsetNeedsCoordination(),
					null);
			return;
		}
		
		@SuppressWarnings("unchecked")
		// checked right above
		RCRecordRequest<NodeIDType> rcRecReq = (RCRecordRequest<NodeIDType>) rcPacket;

		if (this.isCommitWorkerCoordinated(rcRecReq)) {
			ReconfigurationConfig.log.log(Level.FINE,
					"{0} executing CommitWorker callback after {1} execution of {2}",
					new Object[] { this, (handled ? "successful" : "failed"),
							rcRecReq.getSummary() });
			this.commitWorker.executedCallback(rcRecReq, handled);
		}

		// handled is true when reconfiguration intent causes state change
		if (handled && rcRecReq.isReconfigurationIntent()
				&& !rcRecReq.isNodeConfigChange()
				&& !rcRecReq.isActiveNodeConfigChange()) {
			// if I initiated this, spawn reconfiguration task
			if (rcRecReq.startEpoch.getInitiator().equals(getMyID())
			// but spawn anyway for my RC group reconfigurations
					|| (this.DB.isRCGroupName(rcRecReq.getServiceName()) && rcRecReq.startEpoch
							.getCurEpochGroup().contains(getMyID())))
				this.spawnPrimaryReconfiguratorTask(rcRecReq);
			// else I am secondary, so wait for primary's execution
			else if (!this.DB.isRCGroupName(rcRecReq.getServiceName()))
				this.spawnSecondaryReconfiguratorTask(rcRecReq);

		} else if (handled
				&& (rcRecReq.isReconfigurationComplete() || rcRecReq
						.isDeleteIntentOrPrevDropComplete())) {
			// send delete confirmation to deleting client
			if (rcRecReq.isDeleteIntent()
					&& rcRecReq.startEpoch.isDeleteRequest())
				sendDeleteConfirmationToClient(rcRecReq);
			// send response back to creating client
			else if (rcRecReq.isReconfigurationComplete()
					&& rcRecReq.startEpoch.isCreateRequest()) {
				this.sendCreateConfirmationToClient(rcRecReq,
						updateAndCheckComplete(rcRecReq.startEpoch));
			}
			// send response back to RCReconfigure initiator
			else if (rcRecReq.isReconfigurationComplete()
					&& rcRecReq.isNodeConfigChange())
				// checkpoint and garbage collect
				this.postCompleteNodeConfigChange(rcRecReq);

			if (this.DB.outstandingContains(rcRecReq.getServiceName()))
				this.DB.notifyOutstanding(rcRecReq.getServiceName());

			/* If reconfiguration is complete, remove any previously spawned
			 * secondary tasks for the same reconfiguration. We do not remove
			 * WaitAckDropEpoch here because that might still be waiting for
			 * drop ack messages. If they don't arrive in a reasonable period of
			 * time, WaitAckDropEpoch is designed to self-destruct. But we do
			 * remove all tasks corresponding to the previous epoch at this
			 * point. */
			this.garbageCollectPendingTasks(rcRecReq);
		} else if (handled && rcRecReq.isNodeConfigChange()) {
			if (rcRecReq.isReconfigurationIntent()) {
				ncAssert(rcRecReq, handled);
				// initiate and complete reconfiguring RC groups here
				executeNodeConfigChange(rcRecReq);
			}
		} else if (handled && rcRecReq.isActiveNodeConfigChange()) {
			if (rcRecReq.isReconfigurationIntent()) {
				this.spawnExecuteActiveNodeConfigChange(rcRecReq);
			}
		} else if (rcRecReq.isReconfigurationMerge()) {
			if (!handled) {
				/* Merge was unsuccessful probably because the node that
				 * responded with the checkpoint handle did not deliver on the
				 * actual checkpoint, so we need to start from WaitAckStopEpoch
				 * all over again. Note that it is inconvenient to do something
				 * similar to WaitEpochFinalState and merge upon successfully
				 * getting the state because the merge needs a coordinated
				 * commit task that is asynchronous. The only way to know if the
				 * merge succeeded or failed is in this Reconfigurator
				 * executed() callback function but WaitEpochFinalState by
				 * design belongs to ActiveReplica. */
				ReconfigurationConfig.log.log(Level.INFO, "{0} restarting failed merge {1}",
						new Object[] { this, rcRecReq.getSummary() });
				this.protocolExecutor
						.spawnIfNotRunning(new WaitAckStopEpoch<NodeIDType>(
								rcRecReq.startEpoch, this.DB));
			}

			else if (handled && rcRecReq.getInitiator().equals(getMyID()))
				/* We shoudln't explicitly drop the mergee's final epoch state
				 * as other nodes may not have completed the merge and the node
				 * that first supplied the final checkpoint handle may have
				 * crashed. If so, we need to resume WaitAckStopEpoch and for it
				 * to succeed, we need the final checkpoints to not be dropped.
				 * The mergee's final state can be left around hanging and will
				 * eventually become unusable after MAX_FINAL_STATE_AGE. The
				 * actual checkpoints via the file system will be deleted by the
				 * garbage collector eventually, but the final checkpoint
				 * handles in the DB will remain forever or at least until a
				 * node with this name is re-added to the system. */
				;
		}
	}

	private boolean isCommitWorkerCoordinated(
			RCRecordRequest<NodeIDType> rcRecReq) {
		return (TWO_PAXOS_RC && (rcRecReq.isReconfigurationComplete() || rcRecReq
				.isReconfigurationPrevDropComplete()))
				|| rcRecReq.isReconfigurationMerge();
	}

	private void ncAssert(RCRecordRequest<NodeIDType> rcRecReq, boolean handled) {
		ReconfigurationRecord<NodeIDType> ncRecord = this.DB
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());
		assert (!ncRecord.getActiveReplicas().equals(ncRecord.getNewActives())) : this
				+ " : handled="
				+ handled
				+ "; "
				+ ncRecord
				+ "\n  upon \n"
				+ rcRecReq;
	}

	@Override
	public boolean preExecuted(Request request) {
		if (this.isRecovering())
			return false;
		// checked right above
		RCRecordRequest<NodeIDType> rcRecReq = this
				.requestToRCRecordRequest(request);

		// this method is currently used for NC record completions
		if (rcRecReq == null || !this.DB.isNCRecord(rcRecReq.getServiceName())
				|| !rcRecReq.isReconfigurationComplete())
			return false;

		/* Only newly added nodes need to do this as they received a
		 * reconfiguration complete out of the blue and may not even know the
		 * socket addresses of other newly added nodes. */
		if (rcRecReq.startEpoch.getNewlyAddedNodes().contains(this.getMyID()))
			this.executeNodeConfigChange(rcRecReq);
		
		return false;
	}

	/****************************** End of protocol task handler methods *********************/

	/*********************** Private methods below **************************/

	@SuppressWarnings("unchecked")
	private RCRecordRequest<NodeIDType> requestToRCRecordRequest(Request request) {
		if (request instanceof RCRecordRequest<?>)
			return (RCRecordRequest<NodeIDType>) request;
		BasicReconfigurationPacket<?> rcPacket = null;
		try {
			rcPacket = ReconfigurationPacket.getReconfigurationPacket(request,
					getUnstringer());
		} catch (JSONException e) {
			if (!request.toString().equals(Request.NO_OP))
				e.printStackTrace();
		}
		if (rcPacket == null
				|| !rcPacket.getType().equals(
						ReconfigurationPacket.PacketType.RC_RECORD_REQUEST))
			return null;
		// checked right above
		RCRecordRequest<NodeIDType> rcRecReq = (RCRecordRequest<NodeIDType>) rcPacket;
		return rcRecReq;
	}

	private void spawnPrimaryReconfiguratorTask(
			RCRecordRequest<NodeIDType> rcRecReq) {
		/* This assert follows from the fact that the return value handled can
		 * be true for a reconfiguration intent packet exactly once. */
		// MOB-504: Fix 2: This is more of a hack for now.
		// if(this.isTaskRunning(this.getTaskKey(WaitAckStopEpoch.class,
		// rcRecReq)) && !rcRecReq.isSplitIntent()) return;

		assert (!this.isTaskRunning(this.getTaskKey(WaitAckStopEpoch.class,
				rcRecReq)));

		ReconfigurationConfig.log.log(Level.FINE,
				MyLogger.FORMAT[8],
				new Object[] { this, "spawning WaitAckStopEpoch for",
						rcRecReq.startEpoch.getPrevGroupName(), ":",
						rcRecReq.getEpochNumber() - 1, "for starting",
						rcRecReq.getServiceName(), ":",
						rcRecReq.getEpochNumber() });
		// the main stop/start/drop sequence begins here
		if (!rcRecReq.isSplitIntent())
			this.protocolExecutor
					.spawnIfNotRunning(new WaitAckStopEpoch<NodeIDType>(
							rcRecReq.startEpoch, this.DB));
		else
			// split reconfigurations should skip the stop phase
			this.protocolExecutor
					.spawnIfNotRunning(new WaitAckStartEpoch<NodeIDType>(
							rcRecReq.startEpoch, this.DB));

	}

	// utility method used to determine how to offset ports in responses
	private Boolean receivedOnSSLPort(ClientReconfigurationPacket request) {
		return request.getMyReceiver().getPort() == ReconfigurationConfig
				.getClientFacingSSLPort(this.consistentNodeConfig
						.getNodePort(getMyID())) ? (Boolean) true : (request
				.getMyReceiver().getPort() == ReconfigurationConfig
				.getClientFacingClearPort(this.consistentNodeConfig
						.getNodePort(getMyID())) ? (Boolean) false
								// null means ports are unmodified
				: (Boolean) null);
	}

	private void spawnSecondaryReconfiguratorTask(
			RCRecordRequest<NodeIDType> rcRecReq) {
		/* This assert follows from the fact that the return value handled can
		 * be true for a reconfiguration intent packet exactly once. */
		if (this.isTaskRunning(this.getTaskKey(WaitPrimaryExecution.class,
				rcRecReq))) {
			ReconfigurationConfig.log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] { this,
					" TASK IS ALREADY RUNNING: ", rcRecReq.getSummary() });
		}
		// disable
		assert (!this.isTaskRunning(this.getTaskKey(WaitPrimaryExecution.class,
				rcRecReq)));
		//

		ReconfigurationConfig.log.log(Level.FINE, MyLogger.FORMAT[3],
				new Object[] { this, " spawning WaitPrimaryExecution for ",
						rcRecReq.getServiceName(),
						rcRecReq.getEpochNumber() - 1 });
		/* If nodeConfig is under flux, we could be wrong on the set of peer
		 * reconfigurators below, but this information is only used to get
		 * confirmation from the primary, so in the worst case, the secondary
		 * will not hear from any primary and will itself complete the
		 * reconfiguration, which will be consistent thanks to paxos. */
		this.protocolExecutor.schedule(new WaitPrimaryExecution<NodeIDType>(
				getMyID(), rcRecReq.startEpoch, this.DB,
				this.consistentNodeConfig.getReplicatedReconfigurators(rcRecReq
						.getServiceName())));
	}

	/**
	 * These are the only request types which {@link Reconfigurator} will accept
	 * on the client facing port.
	 */
	private ReconfigurationPacket.PacketType[] clientRequestTypes = {
			ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME,
			ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME,
			ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS, };

	private ReconfigurationPacket.PacketType[] clientRequestTypesNoCreateDelete = { ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS, };

	// put anything needing periodic instrumentation here
	private void instrument(Level level) {
		ReconfigurationConfig.log.log(level,
				"{0} activeThreadCount = {1}; taskCount = {2}; completedTaskCount = {3}",
				new Object[] { this, this.protocolExecutor.getActiveCount(),
						this.protocolExecutor.getTaskCount(),
						this.protocolExecutor.getCompletedTaskCount() });
	}

	class Instrumenter implements Runnable {
		public void run() {
			instrument(Level.FINE);
		}
	}

	private AddressMessenger<JSONObject> getClientMessenger() {
		return this.messenger.getClientMessenger();
	}

	private AddressMessenger<JSONObject> getClientMessenger(
			InetSocketAddress listenSockAddr) {
		AddressMessenger<JSONObject> cMsgr = this.messenger
				.getClientMessenger(listenSockAddr);
		cMsgr = cMsgr != null ? cMsgr : this.messenger;
		ReconfigurationConfig.log.log(Level.FINEST,
				"{0} returning messenger listening on address {1}",
				new Object[] { this, listenSockAddr, cMsgr });
		return cMsgr;
	}

	/**
	 * Returns true if this request's handling is complete, i.e., it has either
	 * been forwarded to another server or it was a response to a request
	 * forwarded earlier and the response has now been forwarded to the
	 * end-client.
	 * 
	 * @param clientRCPacket
	 * @return
	 */
	private boolean processRedirection(
			ClientReconfigurationPacket clientRCPacket) {
		/* Received response from responsible reconfigurator to which I
		 * previously forwarded this client request. Need to check whether I
		 * received a redirected response before checking whether I am
		 * responsible, otherwise there will be an infinite forwarding loop. */
		if (clientRCPacket.isRedirectedResponse()) {
			ReconfigurationConfig.log.log(Level.FINE,
					"{0} relaying RESPONSE for forwarded request {1} to {2}",
					new Object[] { this, clientRCPacket.getSummary(),
							clientRCPacket.getCreator() });
			// invoke callback if any
			Callback<Request,ReconfiguratorRequest> callback;
			// this.sendClientReconfigurationPacket
			if ((callback = this.callbacksCRP.remove(getCRPKey(clientRCPacket))) != null)
				callback.processResponse(clientRCPacket);
			else
				// just relay response to the client
				this.sendClientReconfigurationPacket(modifyPortsForSSLIfNeeded(clientRCPacket));
			return true;
		}
		// forward if I am not responsible
		else
			return (this.redirectableRequest(clientRCPacket));
	}

	private ClientReconfigurationPacket modifyPortsForSSLIfNeeded(
			ClientReconfigurationPacket clientRCPacket) {
		return clientRCPacket.getRequestType() == ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS
				&& this.amReceiver(((RequestActiveReplicas) clientRCPacket)) ? (((RequestActiveReplicas) clientRCPacket)
				.setActives(
						modifyPortsForSSL(
								((RequestActiveReplicas) clientRCPacket)
										.getActives(),
								receivedOnSSLPort(clientRCPacket))).isFailed() ? clientRCPacket
				.setHashRCs(modifyPortsForSSL(this
						.getSocketAddresses(this.consistentNodeConfig
								.getReplicatedReconfigurators(clientRCPacket
										.getServiceName())),
						receivedOnSSLPort(clientRCPacket))) : clientRCPacket)

				: clientRCPacket;
	}

	private static final Set<InetSocketAddress> modifyPortsForSSL(
			Set<InetSocketAddress> replicas, Boolean ssl) {
		if (ssl == null || ReconfigurationConfig.getClientPortOffset() == 0
				|| replicas == null || replicas.isEmpty())
			return replicas;
		Set<InetSocketAddress> modified = new HashSet<InetSocketAddress>();
		for (InetSocketAddress sockAddr : replicas)
			modified.add(new InetSocketAddress(sockAddr.getAddress(),
					ssl ? ReconfigurationConfig.getClientFacingSSLPort(sockAddr
							.getPort()) : ReconfigurationConfig
							.getClientFacingClearPort(sockAddr.getPort())));
		return modified;
	}

	private boolean clientFacingPortIsMyPort() {
		return getClientFacingClearPort(this.consistentNodeConfig
				.getNodePort(getMyID())) == this.consistentNodeConfig
				.getNodePort(getMyID());
	}

	/**
	 * Refer {@link ActiveReplica#getClientFacingSSLPort(int)}.
	 * 
	 * @param port
	 * @return The client facing ssl port.
	 */
	public static final int getClientFacingSSLPort(int port) {
		return ActiveReplica.getClientFacingSSLPort(port);
	}

	/**
	 * @param port
	 * @return The client facing clear port.
	 */
	public static final int getClientFacingClearPort(int port) {
		return ActiveReplica.getClientFacingClearPort(port);
	}

	private boolean redirectableRequest(ClientReconfigurationPacket request) {
		// I am responsible
		if (this.consistentNodeConfig.getReplicatedReconfigurators(
				request.getServiceName()).contains(getMyID()))
			return false;

		// else if forwardable
		if (request.isForwardable())
			// forward to a random responsible reconfigurator
			this.forwardClientReconfigurationPacket(request);
		else
			// error with redirection hints
			this.sendClientReconfigurationPacket(this
					.amReceiver(request
							.setFailed()
							.setResponseMessage(
									" <Wrong number! I am not the reconfigurator responsible>")) ?

			request.setHashRCs(modifyPortsForSSL(this
					.getSocketAddresses(this.consistentNodeConfig
							.getReplicatedReconfigurators(request
									.getServiceName())),
					receivedOnSSLPort(request)))

			: request);
		return true;
	}

	private Set<InetSocketAddress> getSocketAddresses(Set<NodeIDType> nodes) {
		Set<InetSocketAddress> sockAddrs = new HashSet<InetSocketAddress>();
		for (NodeIDType node : nodes)
			sockAddrs.add(this.consistentNodeConfig.getNodeSocketAddress(node));
		return sockAddrs;
	}

	private static Set<String> getStringSet(Set<?> nodes) {
		if(nodes==null) return null;
		Set<String> strNodes = new HashSet<String>();
		for (Object node : nodes)
			strNodes.add(node.toString());
		return strNodes;
	}

	/**
	 * Initiates clear or SSL client messenger based on {@code ssl}.
	 * 
	 * @param ssl
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private AddressMessenger<JSONObject> initClientMessenger(boolean ssl) {
		AbstractPacketDemultiplexer<Request> pd = null;
		Messenger<InetSocketAddress, JSONObject> cMsgr = null;
		try {
			int myPort = (this.consistentNodeConfig.getNodePort(getMyID()));
			if ((ssl ? getClientFacingSSLPort(myPort)
					: getClientFacingClearPort(myPort)) != myPort) {
				ReconfigurationConfig.log.log(Level.INFO,
						"{0} creating {1} client messenger at {2}:{3}",
						new Object[] {
								this,
								ssl ? "SSL" : "",
								this.consistentNodeConfig
										.getBindAddress(getMyID()),
								""
										+ (ssl ? getClientFacingSSLPort(myPort)
												: getClientFacingClearPort(myPort)) });
				AddressMessenger<?> existing = (ssl ? this.messenger
						.getSSLClientMessenger() : this.messenger
						.getClientMessenger());

				if (existing == null || existing == this.messenger) {
					MessageNIOTransport<InetSocketAddress, JSONObject> niot = null;
					InetSocketAddress isa = new InetSocketAddress(
							this.consistentNodeConfig.getBindAddress(getMyID()),
							ssl ? getClientFacingSSLPort(myPort)
									: getClientFacingClearPort(myPort));
					// only receives
					cMsgr = new JSONMessenger<InetSocketAddress>(
							niot = new MessageNIOTransport<InetSocketAddress, JSONObject>(
									isa.getAddress(),
									isa.getPort(),
									(pd = new ReconfigurationPacketDemultiplexer(
											this.getUnstringer(), this.DB)),
									ssl ? ReconfigurationConfig
											.getClientSSLMode()
											: SSL_MODES.CLEAR));
					if (!niot.getListeningSocketAddress().equals(isa))
						throw new IOException(
								"Unable to listen on specified socket address at "
										+ isa
										+ "; created messenger listening instead on "
										+ niot.getListeningSocketAddress());
				} else if (!ssl) {
					ReconfigurationConfig.log.log(Level.INFO,
							"{0} adding self as demultiplexer to existing {1} client messenger",
							new Object[] { this, ssl ? "SSL" : "" });
					if (this.messenger.getClientMessenger() instanceof Messenger)
						((Messenger<NodeIDType, ?>) this.messenger
								.getClientMessenger())
								.addPacketDemultiplexer(pd = new ReconfigurationPacketDemultiplexer(
										this.getUnstringer(), this.DB).setThreadName(this.getMyID().toString()+"-clientFacing"));
				} else {
					ReconfigurationConfig.log.log(Level.INFO,
							"{0} adding self as demultiplexer to existing {1} client messenger",
							new Object[] { this, ssl ? "SSL" : "" });
					if (this.messenger.getSSLClientMessenger() instanceof Messenger)
						((Messenger<NodeIDType, ?>) this.messenger
								.getSSLClientMessenger())
								.addPacketDemultiplexer(pd = new ReconfigurationPacketDemultiplexer(
										this.getUnstringer(), this.DB).setThreadName(this.getMyID().toString()+"-clientFacingSSL"));
				}
				assert (pd != null);
				pd.register(
						Config.getGlobalBoolean(RC.ALLOW_CLIENT_TO_CREATE_DELETE) ? clientRequestTypes
								: clientRequestTypesNoCreateDelete, this);
				if (!Config.getGlobalBoolean(RC.ALLOW_CLIENT_TO_CREATE_DELETE)
						&& ReconfigurationConfig.getServerSSLMode() != SSL_MODES.MUTUAL_AUTH)
					Util.suicide("Can not prevent clients from creating and deleting names "
							+ "(ALLOW_CLIENT_TO_CREATE_DELETE=false) unless SERVER_SSL_MODE=MUTUAL_AUTH");
			}
		} catch (IOException e) {
			e.printStackTrace();
			ReconfigurationConfig.log.severe(this + " failed to initialize client messenger: "
					+ e.getMessage());
			System.exit(1);
		}

		if (cMsgr != null)
			if (ssl && this.messenger.getSSLClientMessenger() == null)
				this.messenger.setSSLClientMessenger(cMsgr);
			else if (!ssl && this.messenger.getClientMessenger() == null)
				this.messenger.setClientMessenger(cMsgr);

		return cMsgr != null ? cMsgr
				: (AddressMessenger<JSONObject>) this.messenger;
	}

	private boolean isTaskRunning(String key) {
		return this.protocolExecutor.isRunning(key);
	}

	/* Check for and invoke reconfiguration policy. The reconfiguration policy
	 * is in AbstractDemandProfile and by design only deals with IP addresses,
	 * not node IDs, so we have utility methods in ConsistentNodeConfig to go
	 * back and forth between collections of NodeIDType and InetAddress taking
	 * into account the many-to-one mapping from the former to the latter. A
	 * good reconfiguration policy should try to return a set of IPs that only
	 * minimally modifies the current set of IPs; if so, ConsistentNodeConfig
	 * will ensure a similar property for the corresponding NodeIDType set.
	 * 
	 * If nodeConfig is under flux, this will affect the selection of actives,
	 * but not correctness. */
	private Set<NodeIDType> shouldReconfigure(String name) {
		// return null if no current actives
		Set<NodeIDType> oldActives = this.DB.getActiveReplicas(name);
		if (oldActives == null || oldActives.isEmpty())
			return null;
		// get new IP addresses (via consistent hashing if no oldActives
		Set<String> newActiveIPs = this.demandProfiler
				.testAndSetReconfigured(name, 
						getStringSet(oldActives), this.getReconfigurableAppInfo());
		if (newActiveIPs == null)
			return null;
		// get new actives based on new IP addresses
		Set<NodeIDType> newActives = this.consistentNodeConfig
				.getNodeIDs(newActiveIPs);
		return (!newActives.equals(oldActives) || ReconfigurationConfig
				.shouldReconfigureInPlace()) ? newActives : null;
	}
	
	private ReconfigurableAppInfo getReconfigurableAppInfo() {
		return new ReconfigurableAppInfo() {

			@Override
			public Set<String> getReplicaGroup(String serviceName) {
				ReconfigurationRecord<NodeIDType> record = Reconfigurator.this.DB
						.getReconfigurationRecord(serviceName);
				return record != null ? getStringSet(record.getActiveReplicas())
						: null;
			}

			@Override
			public String snapshot(String serviceName) {
				throw new UnsupportedOperationException(
						"Can not obtain application state at reconfigurator");
			}

			@Override
			public Map<String, InetSocketAddress> getAllActiveReplicas() {
				return Reconfigurator.this.consistentNodeConfig
						.getAllActiveReplicas();
			}
		};
	}

	// combine json stats from report into existing demand profile
	private void updateDemandProfile(DemandReport<NodeIDType> report) {
		// if no entry for name, try to read and refresh from DB
		if (!this.demandProfiler.contains(report.getServiceName())) {
			String statsStr = this.DB.getDemandStats(report.getServiceName());
			JSONObject statsJSON = null;
			try {
				if (statsStr != null)
					statsJSON = new JSONObject(statsStr);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			if (statsJSON != null)
				this.demandProfiler.putIfEmpty(AbstractDemandProfile
						.createDemandProfile(statsJSON));
		}
		this.demandProfiler.combine(AbstractDemandProfile
				.createDemandProfile(report.getStats()));
	}

	/* Stow away to disk if the size of the memory map becomes large. We will
	 * refresh in the updateDemandProfile method if needed. */
	private void trimAggregateDemandProfile() {
		Set<AbstractDemandProfile> profiles = this.demandProfiler.trim();
		for (AbstractDemandProfile profile : profiles) {
			// initiator and epoch are irrelevant in this report
			DemandReport<NodeIDType> report = new DemandReport<NodeIDType>(
					this.getMyID(), profile.getName(), 0, profile);
			// will update stats in DB
			this.DB.execute(report);
		}
	}

	// coordinate reconfiguration intent
	private boolean initiateReconfiguration(String name,
			ReconfigurationRecord<NodeIDType> record,
			Set<NodeIDType> newActives, InetSocketAddress sender,
			InetSocketAddress receiver, InetSocketAddress forwarder,
			String initialState, Map<String, String> nameStates,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes, ReconfigureUponActivesChange policy) {
		if (newActives == null)
			return false;
		// request to persistently log the intent to reconfigure
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.getMyID(), formStartEpoch(name, record, newActives,
						sender, receiver, forwarder, initialState, nameStates,
						newlyAddedNodes, policy), RequestTypes.RECONFIGURATION_INTENT);

		// coordinate intent with replicas
		if (this.isReadyForReconfiguration(rcRecReq, record)) {
			return this.DB.handleIncoming(rcRecReq, null);
		} else
			return false;
	}

	/* We check for ongoing reconfigurations to avoid multiple paxos
	 * coordinations by different nodes each trying to initiate a
	 * reconfiguration. Although only one will succeed at the end, it is still
	 * useful to limit needless paxos coordinated requests. Nevertheless, one
	 * problem with the check in this method is that multiple nodes can still
	 * try to initiate a reconfiguration as it only checks based on the DB
	 * state. Ideally, some randomization should make the likelihood of
	 * redundant concurrent reconfigurations low.
	 * 
	 * It is not important for this method to be atomic. Even if an RC group or
	 * a service name reconfiguration is initiated concurrently with the ready
	 * checks, paxos ensures that no more requests can be committed after the
	 * group has been stopped. If the group becomes non-ready immediately after
	 * this method returns true, the request for which this method is being
	 * called will either not get committed or be rendered a no-op. */
	private boolean isReadyForReconfiguration(
			BasicReconfigurationPacket<NodeIDType> rcPacket,
			ReconfigurationRecord<NodeIDType> recordServiceName) {
		ReconfigurationRecord<NodeIDType> recordGroupName = this.DB
				.getReconfigurationRecord(this.DB.getRCGroupName(rcPacket
						.getServiceName()));
		/* We need to check both if the RC group record is ready and the service
		 * name record is either also ready or null (possible during name
		 * creation). */
		boolean ready = recordGroupName != null && recordGroupName.isReady()
				&& (recordServiceName == null || recordServiceName.isReady());
		if (!ready)
			ReconfigurationConfig.log.log(Level.FINE,
					"{0} not ready to reconfigure {1}; record={2} and rcGroupRecord({3})={4}",
					new Object[] {
							this,
							rcPacket.getServiceName(),
							recordServiceName != null ? recordServiceName
									.getSummary() : "[null]",
									this.DB.getRCGroupName(rcPacket.getServiceName()),
							recordGroupName != null ? recordGroupName
									.getSummary() : "[null]" });
		return ready;
	}

	private NodeIDType getMyID() {
		return this.messenger.getMyID();
	}

	Stringifiable<NodeIDType> getUnstringer() {
		return this.consistentNodeConfig;
	}
	
	private ReconfigureUponActivesChange getReconfigureUponActivesChangePolicy(
			String name) {
		return this.DB.isRCGroupName(name)
				|| AbstractReconfiguratorDB.RecordNames.RC_NODES.toString()
						.equals(name)
				|| AbstractReconfiguratorDB.RecordNames.AR_NODES.toString()
						.equals(name) ? ReconfigurationConfig.ReconfigureUponActivesChange.DEFAULT
				: ReconfigurationConfig
						.getDefaultReconfigureUponActivesChangePolicy();
	}

	private StartEpoch<NodeIDType> formStartEpoch(String name,
			ReconfigurationRecord<NodeIDType> record,
			Set<NodeIDType> newActives, InetSocketAddress sender,
			InetSocketAddress receiver, InetSocketAddress forwarder,
			String initialState, Map<String, String> nameStates,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes) {
		return formStartEpoch(name, record, newActives, sender, receiver, forwarder,
				initialState, nameStates, newlyAddedNodes,
				getReconfigureUponActivesChangePolicy(name));
	}
	private StartEpoch<NodeIDType> formStartEpoch(String name,
			ReconfigurationRecord<NodeIDType> record,
			Set<NodeIDType> newActives, InetSocketAddress sender,
			InetSocketAddress receiver, InetSocketAddress forwarder,
			String initialState, Map<String, String> nameStates,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes, ReconfigureUponActivesChange policy) {
		StartEpoch<NodeIDType> startEpoch = (record != null) ?
		// typical reconfiguration
		new StartEpoch<NodeIDType>(getMyID(), name, record.getEpoch() + 1,
				newActives, record.getActiveReplicas(record.getName(),
						record.getEpoch()), sender, receiver, forwarder,
				initialState, nameStates, newlyAddedNodes, policy)
		// creation reconfiguration
				: new StartEpoch<NodeIDType>(getMyID(), name, 0, newActives,
						null, sender, receiver, forwarder, initialState,
						nameStates, newlyAddedNodes, policy);
		return startEpoch;
	}

	/************ Start of key construction utility methods *************/

	private String getTaskKey(Class<?> C, BasicReconfigurationPacket<?> rcPacket) {
		return getTaskKey(C, rcPacket, getMyID().toString());
	}

	/**
	 * @param C
	 * @param rcPacket
	 * @param myID
	 * @return The task key.
	 */
	public static final String getTaskKey(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket, String myID) {
		return getTaskKey(C, myID, rcPacket.getServiceName(),
				rcPacket.getEpochNumber());
	}

	private static final String getTaskKey(Class<?> C, String myID,
			String name, int epoch) {
		return C.getSimpleName() + myID + ":" + name + ":" + epoch;
	}

	private String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket) {
		return getTaskKeyPrev(C, rcPacket, getMyID().toString());
	}

	private String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket, int prev) {
		return getTaskKeyPrev(C, rcPacket, getMyID().toString(), prev);
	}

	protected static String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket, String myID) {
		return getTaskKeyPrev(C, rcPacket, myID, 1);
	}

	private static final String getTaskKeyPrev(Class<?> C,
			BasicReconfigurationPacket<?> rcPacket, String myID, int prev) {
		return getTaskKey(C, myID, rcPacket.getServiceName(),
				rcPacket.getEpochNumber() - prev);
	}

	/************ End of key construction utility methods *************/

	private void garbageCollectPendingTasks(RCRecordRequest<NodeIDType> rcRecReq) {
		this.garbageCollectStopAndStartTasks(rcRecReq);
		/* Remove secondary task, primary will take care of itself.
		 * 
		 * Invariant: The secondary task always terminates when a
		 * reconfiguration completes. */
		this.protocolExecutor.remove(getTaskKey(WaitPrimaryExecution.class,
				rcRecReq));

		/* We don't need to garbage collect the just completed reconfiguration's
		 * WaitAckDropEpoch as it should clean up after itself when if and when
		 * it finishes, but we should garbage collect any WaitAckDropEpoch from
		 * the immediately preceding reconfiguration completion. So we remove
		 * WaitAckDropEpoch[myID]:name:n-2 here, where 'n' is the epoch number
		 * to which we just completed reconfiguring.
		 * 
		 * Invariant: There is at most one WaitAckDropEpoch task running for a
		 * given name at any reconfigurator, the one for the most recently
		 * completed reconfiguration. */
		this.protocolExecutor.remove(getTaskKeyPrev(WaitAckDropEpoch.class,
				rcRecReq, 2));
	}

	// just before coordinating reconfiguration complete/merge
	private void garbageCollectStopAndStartTasks(
			RCRecordRequest<NodeIDType> rcRecReq) {
		// stop task obviated just before reconfiguration complete proposed
		this.protocolExecutor.remove(this.getTaskKeyPrev(
				WaitAckStopEpoch.class, rcRecReq));

		// FIXME: need to also remove split stop tasks here

		// start task obviated just before reconfiguration complete proposed
		this.protocolExecutor.remove(this.getTaskKey(WaitAckStartEpoch.class,
				rcRecReq));

		// remove previous epoch's start task in case it exists here
		this.protocolExecutor.remove(this.getTaskKeyPrev(
				WaitAckStartEpoch.class, rcRecReq));
	}

	private void initFinishPendingReconfigurations() throws IOException {
		/* Invoked just once upon recovery, but we could also invoke this
		 * periodically. */
		this.finishPendingReconfigurations();

		/* Periodic task to remove old file system based checkpoints after a
		 * safe timeout of MAX_FINAL_STATE_AGE. The choice of the period below
		 * of a tenth of that is somewhat arbitrary. */
		this.protocolExecutor.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				DB.garbageCollectOldFileSystemBasedCheckpoints();
			}
		}, 0, ReconfigurationConfig.getMaxFinalStateAge() / 10,
				TimeUnit.MILLISECONDS);

		/* Periodic task to finish pending deletions after a safe timeout of
		 * MAX_FINAL_STATE_AGE. The choice of the period below of a tenth of
		 * that is somewhat arbitrary. */
		this.protocolExecutor.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				DB.delayedDeleteComplete();
			}
		}, 0, ReconfigurationConfig.getMaxFinalStateAge() / 10,
				TimeUnit.MILLISECONDS);

		// for instrumentation, unrelated to pending reconfigurations
		this.protocolExecutor.scheduleWithFixedDelay(new Instrumenter(), 0, 60,
				TimeUnit.SECONDS);
	}

	private static final boolean SKIP_ACTIVE_DELETIONS_UPON_RECOVERY = true;

	/* Called initially upon recovery to finish pending reconfigurations. We
	 * assume that the set of pending reconfigurations is not too big as this is
	 * the set of reconfigurations that were ongoing at the time of the crash. */
	private void finishPendingReconfigurations() throws IOException {
		String[] pending = this.DB.getPendingReconfigurations();
		for (String name : pending) {
			ReconfigurationRecord<NodeIDType> record = this.DB
					.getReconfigurationRecord(name);

			if (record == null
					||
					// ignore failed creations
					record.getActiveReplicas() == null
					|| record.getActiveReplicas().isEmpty() ||
					// probably crashed before setPending(false)
					record.isReady()) {
				this.DB.removePending(name);
				continue;
			}

			/* Skip active node deletions upon recovery. Either others completed
			 * it or it did not get completed from the issuing client's
			 * perspective in which case it will be reissued if needed. */
			if (record.getName().equals(
					AbstractReconfiguratorDB.RecordNames.AR_NODES.toString())) {
				if (!SKIP_ACTIVE_DELETIONS_UPON_RECOVERY)
					this.executeActiveNodeConfigChange(new RCRecordRequest<NodeIDType>(
							this.getMyID(), this.formStartEpoch(name, record,
									record.getNewActives(), null, null, null,
									null, null, null),
							RCRecordRequest.RequestTypes.RECONFIGURATION_INTENT));
				this.DB.removePending(record.getName());
				continue;
			}

			/* Note; The fact that the RC record request is an intent is
			 * immaterial. It is really only used to construct the corresponding
			 * WaitAckStopEpoch task, i.e., the intent itself will not be
			 * committed again (and indeed can not be by design). */
			ReconfigurationConfig.log.log(Level.FINE,
					"{0} initiating pending reconfiguration for {1}",
					new Object[] { this, name });
			RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
					this.getMyID(), this.formStartEpoch(name, record,
							record.getNewActives(), null, null, null, null,
							null, null),
					RCRecordRequest.RequestTypes.RECONFIGURATION_INTENT);
			/* We spawn primary even though that may be unnecessary because we
			 * don't know if or when any other reconfigurator might finish this
			 * pending reconfiguration. Having multiple reconfigurators push a
			 * reconfiguration is okay as stop, start, and drop are all
			 * idempotent operations. */
			this.spawnPrimaryReconfiguratorTask(rcRecReq);
		}
	}

	private boolean forwardClientReconfigurationPacket(
			ClientReconfigurationPacket request) {
		try {
			Set<NodeIDType> responsibleRCs = this.DB
					.removeDead(new HashSet<NodeIDType>(
							this.consistentNodeConfig
									.getReplicatedReconfigurators(request
											.getServiceName())));
			if (responsibleRCs.isEmpty())
				return false;

			@SuppressWarnings("unchecked")
			NodeIDType randomResponsibleRC = (NodeIDType) (responsibleRCs
					.toArray()[(int) (Math.random() * responsibleRCs.size())]);

			request = request.setForwader(this.consistentNodeConfig
					.getBindSocketAddress(getMyID()));
			ReconfigurationConfig.log.log(Level.INFO,
					"{0} forwarding client request {1} to reconfigurator {2}:{3}",
					new Object[] {
							this,
							request.getSummary(),
							randomResponsibleRC,
							this.consistentNodeConfig
									.getNodeSocketAddress(randomResponsibleRC) });

			this.messenger.sendToAddress(
					this.consistentNodeConfig
							.getNodeSocketAddress(randomResponsibleRC),
					new JSONMessenger.JSONObjectByteableWrapper(request
							.setForwader(this.consistentNodeConfig
									.getNodeSocketAddress(getMyID()))
					// .toJSONObject()
					));
		} catch (IOException /* | JSONException */e) {
			ReconfigurationConfig.log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	private boolean sendClientReconfigurationPacket(
			ClientReconfigurationPacket response) {
		try {
			InetSocketAddress querier = this.getQuerier(response);
			if (querier.equals(response.getCreator())) {
				// only response can go back to client
				ReconfigurationConfig.log.log(Level.INFO,
						"{0} sending client RESPONSE {1}:{2} back to client",
						new Object[] { this, response.getSummary(),
								response.getResponseMessage(), querier });
				(this.getClientMessenger(response.getMyReceiver()))
						.sendToAddress(querier,
								new JSONMessenger.JSONObjectByteableWrapper(
										response
								// .toJSONObject()
								));
			} else {
				// may be a request or response
				ReconfigurationConfig.log.log(Level.INFO,
						"{0} sending {1} {2} to reconfigurator {3}",
						new Object[] { this,
								response.isRequest() ? "request" : "RESPONSE",
								response.getSummary(), querier });
				assert (this.messenger.sendToAddress(querier,
						new JSONMessenger.JSONObjectByteableWrapper(response
						// .toJSONObject()
						)) > 0);
			}
		} catch (IOException /* | JSONException */e) {
			ReconfigurationConfig.log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	/* If it is not my node config socket address, it must be one of the two
	 * client messengers. */
	private AddressMessenger<JSONObject> getMessenger(InetSocketAddress receiver) {
		if (receiver.equals(this.consistentNodeConfig.getBindSocketAddress(this
				.getMyID()))) {
			ReconfigurationConfig.log.log(Level.FINE,
					"{0} using messenger for {1}; bindAddress is {2}",
					new Object[] {
							this,
							receiver,
							this.consistentNodeConfig.getBindSocketAddress(this
									.getMyID()) });
			return this.messenger;
		} else {
			ReconfigurationConfig.log.log(Level.FINE,
					"{0} using clientMessenger for {1}; bindAddress is {2}",
					new Object[] {
							this,
							receiver,
							this.consistentNodeConfig.getBindSocketAddress(this
									.getMyID()) });
			return this.getClientMessenger(receiver);
		}
	}

	/* Confirmation means necessarily a positive response. This method is
	 * invoked from the creation execution callback. If the record already
	 * exists or is in the process of being created, we return an error as
	 * opposed to sending a confirmation via this method.
	 * 
	 * Note: this behavior is different from deletions where we return success
	 * if the record is pending deletion (but do return failure if it has been
	 * completely deleted). */
	private void sendCreateConfirmationToClient(
			RCRecordRequest<NodeIDType> rcRecReq, String headName) {
		if (rcRecReq.startEpoch.creator == null
				|| !rcRecReq.getInitiator().equals(getMyID())
				|| headName == null) {
			return;
		}

		DelayProfiler.updateDelay(ProfilerKeys.create.toString(),
				rcRecReq.startEpoch.getInitTime());
		try {
			InetSocketAddress querier = this.getQuerier(rcRecReq);
			CreateServiceName response = (CreateServiceName) (new CreateServiceName(
					rcRecReq.startEpoch.creator, headName,
					rcRecReq.getEpochNumber(), null,
					rcRecReq.startEpoch.getMyReceiver())).setForwader(
					rcRecReq.startEpoch.getForwarder()).makeResponse();

			Callback<Request,ReconfiguratorRequest> callback = this.callbacksCRP
					.remove(getCRPKey(response));
			if (callback != null)
				callback.processResponse(response);

			// need to use different messengers for client and forwarder
			else if (querier.equals(rcRecReq.startEpoch.creator)) {
				ReconfigurationConfig.log.log(Level.INFO,
						"{0} sending creation confirmation {1} back to client",
						new Object[] { this, response.getSummary(), querier });
				// this.getClientMessenger()
				(this.getMessenger(rcRecReq.startEpoch.getMyReceiver()))
						.sendToAddress(querier,
								new JSONMessenger.JSONObjectByteableWrapper(
										response
								// .toJSONObject()
								));
			} else {
				ReconfigurationConfig.log.log(Level.INFO,
						"{0} sending creation confirmation {1} to forwarding reconfigurator {2}",
						new Object[] { this, response.getSummary(), querier });
				this.messenger.sendToAddress(
						querier,
						new JSONMessenger.JSONObjectByteableWrapper(response
								.setForwardee(this.consistentNodeConfig
										.getNodeSocketAddress(getMyID()))
						// .toJSONObject()
						));
			}
		} catch (IOException /* | JSONException */e) {
			ReconfigurationConfig.log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	/* Confirmation means necessarily a positive response. This method is
	 * invoked either via the delete execution callback or immediately if the
	 * record is already pending deletion. If the record is completely deleted,
	 * we return an error as opposed to sending a confirmation via this method.
	 * 
	 * Note: Returning success for pending deletions is different from the
	 * behavior for creations where we return success only after the record's
	 * creation is complete, i.e., pending creations return a creation error but
	 * pending deletions return a deletion success. This difference is because
	 * once a record is marked as pending deletion (WAIT_DELETE), it is as good
	 * as deleted and is only waiting final garbage collection. */
	private void sendDeleteConfirmationToClient(
			RCRecordRequest<NodeIDType> rcRecReq) {
		if (rcRecReq.startEpoch.creator == null
				|| !rcRecReq.getInitiator().equals(getMyID()))
			return;
		try {
			InetSocketAddress querier = this.getQuerier(rcRecReq);
			// copy forwarder from startEpoch and mark as response
			DeleteServiceName response = (DeleteServiceName) new DeleteServiceName(
					rcRecReq.startEpoch.creator, rcRecReq.getServiceName(),
					rcRecReq.getEpochNumber() - 1,
					rcRecReq.startEpoch.getMyReceiver()).setForwader(
					rcRecReq.startEpoch.getForwarder()).makeResponse();

			Callback<Request,ReconfiguratorRequest> callback = this.callbacksCRP
					.remove(getCRPKey(response));
			if (callback != null)
				callback.processResponse(response);

			if (querier.equals(rcRecReq.startEpoch.creator)) {
				ReconfigurationConfig.log.log(Level.FINE,
						"{0} sending deletion confirmation {1} back to client",
						new Object[] { this, response.getSummary(), querier });
				// this.getClientMessenger()
				(this.getMessenger(rcRecReq.startEpoch.getMyReceiver()))
						.sendToAddress(this.getQuerier(rcRecReq),
								new JSONMessenger.JSONObjectByteableWrapper(
										response
								// .toJSONObject()
								));
			} else {
				ReconfigurationConfig.log.log(Level.FINE,
						"{0} sending deletion confirmation {1} to forwarding reconfigurator {2}",
						new Object[] { this, response.getSummary(), querier });
				this.messenger.sendToAddress(querier,
						new JSONMessenger.JSONObjectByteableWrapper(response
						// .toJSONObject()
						));
			}
		} catch (IOException /* | JSONException */e) {
			ReconfigurationConfig.log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	private InetSocketAddress getQuerier(RCRecordRequest<NodeIDType> rcRecReq) {
		InetSocketAddress forwarder = rcRecReq.startEpoch.getForwarder();
		InetSocketAddress me = this.consistentNodeConfig
				.getBindSocketAddress(getMyID());
		// if there is a forwarder that is not me, relay back
		if (forwarder != null && !forwarder.equals(me))
			return forwarder;
		else
			// return directly to creator
			return rcRecReq.startEpoch.creator;
	}

	private InetSocketAddress getQuerier(ClientReconfigurationPacket response) {
		InetSocketAddress forwarder = response.getForwader();
		InetSocketAddress me = this.consistentNodeConfig
				.getBindSocketAddress(getMyID());
		// if there is a forwarder that is not me, relay back
		if (forwarder != null && !forwarder.equals(me)) {
			return forwarder;
		} else {
			// return directly to creator
			return response.getCreator();
		}
	}

	private static final String separator = "-------------------------------------------------------------------------";

	private void sendReconfigureRCNodeConfigConfirmationToInitiator(
			RCRecordRequest<NodeIDType> rcRecReq) {
		try {
			ReconfigureRCNodeConfig<NodeIDType> response = new ReconfigureRCNodeConfig<NodeIDType>(
					this.DB.getMyID(), rcRecReq.startEpoch.newlyAddedNodes,
					this.diff(rcRecReq.startEpoch.prevEpochGroup,
							rcRecReq.startEpoch.curEpochGroup));
			ReconfigurationConfig.log.log(Level.INFO,
					"\n\n{0}\n{1} sending {2} confirmation to {3}: {4}\n{5}",
					new Object[] {
							separator,
							this,
							ReconfigurationPacket.PacketType.RECONFIGURE_RC_NODE_CONFIG,
							rcRecReq.startEpoch.creator, response.getSummary(),
							separator });
			(this.messenger).sendToAddress(rcRecReq.startEpoch.creator,
					new JSONMessenger.JSONObjectByteableWrapper(response
					// .toJSONObject()
					));
		} catch (IOException /* | JSONException */e) {
			ReconfigurationConfig.log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	private void sendReconfigureActiveNodeConfigConfirmationToInitiator(
			RCRecordRequest<NodeIDType> rcRecReq) {
		try {
			ReconfigureActiveNodeConfig<NodeIDType> response = new ReconfigureActiveNodeConfig<NodeIDType>(
					this.DB.getMyID(), rcRecReq.startEpoch.newlyAddedNodes,
					this.diff(rcRecReq.startEpoch.prevEpochGroup,
							rcRecReq.startEpoch.curEpochGroup));
			ReconfigurationConfig.log.log(Level.INFO,
					"{0} has nodeConfig = {1} after processing {2}",
					new Object[] { this, this.consistentNodeConfig,
							response.getSummary() });

			ReconfigurationConfig.log.log(Level.INFO,
					"\n\n{0}\n{1} finished required reconfigurations to change active replica(s) {2}; sending response to {3}\n{4}\n",
					new Object[] { separator, this, response.getSummary(),
							rcRecReq.startEpoch.creator, separator });
			(this.messenger).sendToAddress(rcRecReq.startEpoch.creator,
					new JSONMessenger.JSONObjectByteableWrapper(response
					// .toJSONObject()
					));
		} catch (IOException /* | JSONException */e) {
			ReconfigurationConfig.log.severe(this + " incurred " + e.getClass().getSimpleName()
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	/*************** Reconfigurator reconfiguration related methods ***************/
	/**
	 * 
	 * @param s1
	 * @param s2
	 * @return s1 - s2
	 */
	private Set<NodeIDType> diff(Set<NodeIDType> s1, Set<NodeIDType> s2) {
		Set<NodeIDType> diff = new HashSet<NodeIDType>();
		for (NodeIDType node : s1)
			if (!s2.contains(node))
				diff.add(node);
		return diff;
	}

	// all nodes are primaries for NC change.
	private boolean reconfigureNodeConfigRecord(
			RCRecordRequest<NodeIDType> rcRecReq) {
		if (rcRecReq.getInitiator().equals(getMyID()))
			this.spawnPrimaryReconfiguratorTask(rcRecReq);
		else
			this.spawnSecondaryReconfiguratorTask(rcRecReq);
		return true;
	}

	/**
	 * @param echo
	 * @param ptasks
	 * @return null
	 */
	public GenericMessagingTask<NodeIDType, ?>[] handleEchoRequest(
			EchoRequest echo,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		ReconfigurationConfig.log.log(Level.FINE, "{0} received echo request {1}", new Object[] {
				this, echo.getSummary() });
		if (echo.isRequest()) {
			// ignore echo requests
		} else if (echo.hasClosest()) {
			RTTEstimator.closest(echo.getSender(), echo.getClosest());
			ReconfigurationConfig.log.log(Level.INFO,
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
		
		NodeIDType nodeID = hello.getInitiator(); 
		if (nodeID == null) {
			ReconfigurationConfig.log.log(Level.FINE, "{0} has no initiator in it.", new Object[] {
					 hello.getSummary() });
			return null;
		}
		
		InetSocketAddress address = headerMap.get(nodeID.toString()).sndr;
		if (address != null) {
			synchronized(consistentNodeConfig) {
				consistentNodeConfig.removeActiveReplica(nodeID);
				consistentNodeConfig.addActiveReplica(nodeID, address);
			}
		}
		ReconfigurationConfig.log.log(Level.FINE, "NodeConfig gets updated to {0}", new Object[] {
				consistentNodeConfig });
		return null;
	}
	
	/* This method conducts the actual reconfiguration assuming that the
	 * "intent" has already been committed in the NC record. It (1) spawns each
	 * constituent reconfiguration for its new reconfigurator groups and (2)
	 * reconfigures the NC record itself. Spawning each constituent
	 * reconfiguration means executing the corresponding reconfiguration intent,
	 * then spawning WaitAckStop, etc. It is not important to worry about
	 * "completing" the NC change intent under failures as paxos will ensure
	 * safety. We do need a trigger to indicate the completion of all
	 * constituent reconfigurations so that the NC record change can be
	 * considered and marked as complete. For this, upon every NC
	 * reconfiguration complete commit, we could simply check if any of the new
	 * RC groups are still pending and if not, consider the NC change as
	 * incomplete until all constituent RC groups are ready. That is what we do
	 * in AbstractReconfiguratorDB. */
	private boolean executeNodeConfigChange(RCRecordRequest<NodeIDType> rcRecReq) {
		boolean allDone = true;

		// change soft copy of node config
		boolean ncChanged = changeSoftNodeConfig(rcRecReq.startEpoch);
		// change persistent copy of node config
		ncChanged = ncChanged
				&& this.DB.changeDBNodeConfig(rcRecReq.startEpoch
						.getEpochNumber());
		if (!ncChanged)
			throw new RuntimeException(this.getMyID() + " unable to change node config: " + rcRecReq.getSummary());
		assert (!rcRecReq.startEpoch.getNewlyAddedNodes().isEmpty() || !diff(
				rcRecReq.startEpoch.prevEpochGroup,
				rcRecReq.startEpoch.curEpochGroup).isEmpty());

		// to track epoch numbers of RC groups correctly
		Set<NodeIDType> affectedNodes = this.DB.setRCEpochs(
				rcRecReq.startEpoch.getNewlyAddedNodes(),
				diff(rcRecReq.startEpoch.prevEpochGroup,
						rcRecReq.startEpoch.curEpochGroup));

		allDone = this.changeSplitMergeGroups(
				affectedNodes,
				rcRecReq.startEpoch.getNewlyAddedNodes(),
				diff(rcRecReq.startEpoch.prevEpochGroup,
						rcRecReq.startEpoch.curEpochGroup));

		this.reconfigureNodeConfigRecord(rcRecReq);

		// finally all done
		return allDone;
	}

	/* Starts in a separate thread as it is a blocking operation. It does not
	 * have to finish before returning even though it is technically a paxos
	 * request (or the request's callback to be precise) because it is an
	 * internal operation. The issuing client only gets a response when
	 * executeActiveNodeConfig and the WaitAckDropEpoch task that it spawns both
	 * eventually complete, which may take a while. */
	private void spawnExecuteActiveNodeConfigChange(
			final RCRecordRequest<NodeIDType> rcRecReq) {
		ReconfigurationConfig.log.log(Level.INFO,
				"{0} spawning active node config change task for {1}",
				new Object[] {
						this,
						new ReconfigureActiveNodeConfig<NodeIDType>(rcRecReq
								.getInitiator(),
								rcRecReq.startEpoch.newlyAddedNodes,
								rcRecReq.startEpoch.getDeletedNodes())
								.getSummary() });
		this.protocolExecutor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					Reconfigurator.this.executeActiveNodeConfigChange(rcRecReq);
				} catch (Exception | Error e) {
					e.printStackTrace();
				}
			}

		});
	}

	/* There is no actual reconfiguration to be done for the ACTIVE_NODE_CONFIG
	 * record itself, just the app records placed on deleted active nodes. The
	 * current and new actives in the ACTIVE_NODE_CONFIG do *not* mean the
	 * current and new locations where the record is replicated. The record
	 * itself is replicated at all reconfigurators and the current and new
	 * actives are the universe of active replicas as maintained by the
	 * reconfigurators. */
	private boolean executeActiveNodeConfigChange(
			RCRecordRequest<NodeIDType> rcRecReq) {

		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.AR_NODES
						.toString());

		// change soft copy
		if (rcRecReq.startEpoch.hasNewlyAddedNodes())
			for (NodeIDType node : rcRecReq.startEpoch.newlyAddedNodes.keySet())
				this.consistentNodeConfig.addActiveReplica(node,
						rcRecReq.startEpoch.newlyAddedNodes.get(node));

		assert (diff(record.getActiveReplicas(), record.getNewActives()).size() <= 1);

		// FIXME: why change again?
		for (NodeIDType active : this.diff(record.getNewActives(),
				record.getActiveReplicas()))
			this.consistentNodeConfig.addActiveReplica(active,
					rcRecReq.startEpoch.newlyAddedNodes.get(active));

		// change persistent copy of active node config
		boolean ancChanged = this.DB
				.changeActiveDBNodeConfig(rcRecReq.startEpoch.getEpochNumber());

		// handle deleted active replicas
		Set<NodeIDType> deletedNodes = this.diff(record.getActiveReplicas(),
				record.getNewActives());
		for (NodeIDType active : deletedNodes)
			this.deleteActiveReplica(active, rcRecReq.startEpoch.creator);

		try {
			this.DB.waitOutstanding(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}

		this.consistentNodeConfig.removeActivesSlatedForRemoval();
		for (NodeIDType active : this.diff(record.getActiveReplicas(),
				record.getNewActives())) {
			assert (!this.consistentNodeConfig.nodeExists(active));
		}
		
		// handle newly added active replicas
		if (rcRecReq.startEpoch.newlyAddedNodes != null) {
			Set<NodeIDType> addedNodes = rcRecReq.startEpoch.newlyAddedNodes
					.keySet();
			for (NodeIDType active : addedNodes)
				this.addActiveReplica(active, rcRecReq.startEpoch.creator);

			try {
				this.DB.waitOutstanding(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return false;
			}
		}

		// uncoordinated change locally
		boolean executed = this.DB.execute(new RCRecordRequest<NodeIDType>(
				rcRecReq.getInitiator(), rcRecReq.startEpoch,
				RCRecordRequest.RequestTypes.RECONFIGURATION_COMPLETE));

		this.DB.forceCheckpoint(AbstractReconfiguratorDB.RecordNames.AR_NODES
				.toString());

		// launch prev drop complete
		if (rcRecReq.getInitiator().equals(this.getMyID()))
			this.protocolExecutor
					.spawnIfNotRunning(new WaitAckDropEpoch<NodeIDType>(
							rcRecReq.startEpoch, this.consistentNodeConfig
									.getReconfigurators(), this.DB));

		this.updatePropertiesFile(rcRecReq, PaxosConfig.DEFAULT_SERVER_PREFIX);
		return ancChanged && executed;
	}

	private void updatePropertiesFile(RCRecordRequest<NodeIDType> rcRecReq,
			String prefix) {
		// active node config change complete
		if (PaxosConfig.getPropertiesFile() != null)
			try {
				for (NodeIDType node : rcRecReq.startEpoch.getNewlyAddedNodes())
					UtilServer.writeProperty(prefix + node, this.consistentNodeConfig
							.getNodeAddress(node).getHostAddress()
							+ ":"
							+ this.consistentNodeConfig.getNodePort(node),
							PaxosConfig.getPropertiesFile(), prefix);
				for (NodeIDType node : rcRecReq.startEpoch.getDeletedNodes())
					UtilServer.writeProperty(prefix + node, null,
							PaxosConfig.getPropertiesFile(), prefix);
			} catch (IOException ioe) {
				ReconfigurationConfig.log.severe(this
						+ " incurred exception while modifying properties file"
						+ PaxosConfig.getPropertiesFile() + ioe);
			}
		else
			ReconfigurationConfig.log.log(Level.INFO,
					"{0} not updating non-existent properties file upon adds={1}, deletes={2}",
					new Object[] { this,
							rcRecReq.startEpoch.getNewlyAddedNodes(),
							rcRecReq.startEpoch.getDeletedNodes() });
	}

	/* We need to checkpoint the NC record after every NC change. Unlike other
	 * records for RC groups where we can roll forward quickly by simply
	 * applying state changes specified in the logged decisions (instead of
	 * actually re-conducting the corresponding reconfigurations), NC group
	 * changes are more complex and have to be re-conducted at each node
	 * redundantly, however that may not even be possible as deleted nodes or
	 * even existing nodes may no longer have the final state corresponding to
	 * older epochs. Checkpointing after every NC change ensures that, upon
	 * recovery, each node has to try to re-conduct at most only the most recent
	 * NC change.
	 * 
	 * What if this forceCheckpoint operation fails? If the next NC change
	 * successfully completes at this node before the next crash, there is no
	 * problem. Else, upon recovery, this node will try to re-conduct the NC
	 * change corresponding to the failed forceCheckpoint and might be unable to
	 * do so. This is equivalent to this node having missed long past NC
	 * changes. At this point, this node must be deleted and re-added to NC. */
	private void postCompleteNodeConfigChange(
			RCRecordRequest<NodeIDType> rcRecReq) {
		ReconfigurationConfig.log.log(Level.INFO,
				"{0} completed node config change for epoch {1}; (forcing checkpoint..)",
				new Object[] { this, rcRecReq.getEpochNumber() });
		this.DB.forceCheckpoint(rcRecReq.getServiceName());

		// active node config change complete
		this.updatePropertiesFile(rcRecReq,
				ReconfigurationConfig.DEFAULT_RECONFIGURATOR_PREFIX);

		// stop needless failure monitoring
		for (NodeIDType node : diff(rcRecReq.startEpoch.prevEpochGroup,
				rcRecReq.startEpoch.curEpochGroup))
			this.DB.garbageCollectDeletedNode(node);

	}

	// change soft copy of node config
	private boolean changeSoftNodeConfig(StartEpoch<NodeIDType> startEpoch) {
		/* Do adds immediately. This means that if we ever need the old
		 * "world view" again, e.g., to know which group a name maps to, we have
		 * to reconstruct the consistent hash ring on demand based on the old
		 * set of nodes in the DB. We could optimize this slightly by just
		 * storing also an in-memory copy of the old consistent hash ring, but
		 * this is probably unnecessary given that nodeConfig changes are rare,
		 * slow operations anyway. */
		if (startEpoch.hasNewlyAddedNodes())
			for (Map.Entry<NodeIDType, InetSocketAddress> entry : startEpoch.newlyAddedNodes
					.entrySet()) {
				this.consistentNodeConfig.addReconfigurator(entry.getKey(),
						entry.getValue());
				ReconfigurationConfig.log.log(Level.FINE,
						"{0} added new reconfigurator {1}={2} to node config",
						new Object[] {
								this,
								entry.getKey(),
								this.consistentNodeConfig
										.getNodeSocketAddress(entry.getKey()) });
			}
		/* Deletes, not so fast. If we delete entries from nodeConfig right
		 * away, we don't have those nodes' socket addresses, so we can't
		 * communicate with them any more, but we need to be able to communicate
		 * with them in order to do the necessary reconfigurations to cleanly
		 * eliminate them from the consistent hash ring. */
		for (NodeIDType node : this.diff(startEpoch.prevEpochGroup,
				startEpoch.curEpochGroup)) {
			this.consistentNodeConfig.slateForRemovalReconfigurator(node);
		}
		return true;
	}

	private boolean isPermitted(ReconfigureRCNodeConfig<NodeIDType> changeRC) {

		// if node is pending deletion from previous incarnation
		if (changeRC.getAddedNodeIDs() != null)
			for (NodeIDType addNode : changeRC.getAddedNodeIDs()) {
				ReconfigurationRecord<NodeIDType> rcRecord = this.DB
						.getReconfigurationRecord(this.DB
								.getRCGroupName(addNode));
				{
					if (rcRecord != null && rcRecord.isDeletePending()) {
						changeRC.setResponseMessage("Can not add reconfigurator named "
								+ addNode
								+ " as it is pending deletion from a previous add.");
						return false;
					}
					// check if name conflicts with active replica name
					else if (this.consistentNodeConfig.nodeExists(addNode)) {
						changeRC.setResponseMessage("Can not add reconfigurator named "
								+ addNode
								+ " as another node with the same name already exists.");
						return false;
					}
				}
			}
		// if node is not in the current set of RC nodes
		if (changeRC.deletedNodes != null)
			for (NodeIDType deleteNode : changeRC.deletedNodes) {
				if (!this.consistentNodeConfig.getReconfigurators().contains(
						deleteNode)) {
					changeRC.setResponseMessage("Can not delete reconfigurator "
							+ deleteNode
							+ " as it is not part of the current set of reconfigurators");
					return false;
				}
			}

		int permittedSize = this.consistentNodeConfig
				.getReplicatedReconfigurators("0").size();
		// allow at most one less than the reconfigurator group size
		return changeRC.getDeletedNodeIDs().size() > permittedSize ? (changeRC
				.setResponseMessage("Deleting more than " + (permittedSize - 1)
						+ " reconfigurators simultaneously is not permitted") != null)
				: true;
	}

	private boolean amAffected(Set<NodeIDType> addNodes,
			Set<NodeIDType> deleteNodes) {
		boolean affected = false;
		for (NodeIDType node : addNodes)
			if (this.DB.amAffected(node))
				affected = true;
		for (NodeIDType node : deleteNodes)
			if (this.DB.amAffected(node))
				affected = true;
		return affected;
	}

	private boolean changeSplitMergeGroups(Set<NodeIDType> affectedNodes,
			Set<NodeIDType> addNodes, Set<NodeIDType> deleteNodes) {
		if (!amAffected(addNodes, deleteNodes))
			return false;

		// get list of current RC groups from DB.
		Map<String, Set<NodeIDType>> curRCGroups = this.DB.getOldRCGroups();
		// get list of new RC groups from NODE_CONFIG record in DB
		Map<String, Set<NodeIDType>> newRCGroups = this.DB.getNewRCGroups();
		// get NC record from DB
		ReconfigurationRecord<NodeIDType> ncRecord = this.DB
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());

		assert (!ncRecord.getActiveReplicas().equals(ncRecord.getNewActives())) : ncRecord;

		if (ncRecord == null)
			return false;

		// adjustCurWithNewRCGroups(curRCGroups, newRCGroups, ncRecord);
		String changedSplitMerged = this.changeExistingGroups(curRCGroups,
				newRCGroups, ncRecord, affectedNodes);
		if (!ReconfigurationConfig.isAggregatedMergeSplit()) {
			// the two methods below are unused with aggregated merge/split
			changedSplitMerged += this.splitExistingGroups(curRCGroups,
					newRCGroups, ncRecord);
			changedSplitMerged += this.mergeExistingGroups(curRCGroups,
					newRCGroups, ncRecord);
		}

		ReconfigurationConfig.log.log(Level.INFO, "\n                           "
				+ "{0} changed/split/merged = \n{1}", new Object[] { this,
				changedSplitMerged });
		return !(changedSplitMerged).isEmpty();
	}

	private boolean isRecovering() {
		return this.recovering;
	}

	private boolean isPresent(String rcGroupName, Set<NodeIDType> affectedNodes) {
		for (NodeIDType node : affectedNodes) {
			if (this.DB.getRCGroupName(node).equals(rcGroupName))
				return true;
		}
		return false;
	}

	// NC request restarts should be slow and mostly unnecessary
	private static final long NODE_CONFIG_RESTART_PERIOD = 8 * WaitAckStopEpoch.RESTART_PERIOD;

	private void repeatUntilObviated(RCRecordRequest<NodeIDType> rcRecReq) {
		if (this.DB.isNCRecord(rcRecReq.getServiceName()))
			this.commitWorker.enqueueForExecution(rcRecReq,
					NODE_CONFIG_RESTART_PERIOD);
		else {
			if (rcRecReq.isReconfigurationMerge())
				ReconfigurationConfig.log.log(Level.INFO, "{0} coordinating merge {1}", new Object[] {
						this, rcRecReq.getSummary() });
			this.commitWorker.enqueueForExecution(rcRecReq);
		}
	}

	/**
	 * This method reconfigures groups that exist locally both in the old and
	 * new rings, i.e., this node just has to do a standard reconfiguration
	 * operation because the membership of the paxos group is changing.
	 * 
	 * With {@link ReconfigurationConfig#isAggregatedMergeSplit()}, this method suffices for all
	 * necessary reconfigurations; otherwise, we have to also invoke
	 * splitExistingGroups and mergeExistingGroups.
	 */
	private String changeExistingGroups(
			Map<String, Set<NodeIDType>> curRCGroups,
			Map<String, Set<NodeIDType>> newRCGroups,
			ReconfigurationRecord<NodeIDType> ncRecord,
			Set<NodeIDType> affectedNodes) {
		String debug = ""; // just for prettier clustered printing
		Map<String, Set<String>> mergeLists = this.DB.app.getMergeLists();
		// for each new group, initiate group change if and as needed
		for (String newRCGroup : newRCGroups.keySet()) {
			if (!isPresent(newRCGroup, affectedNodes))
				continue; // don't trivial-reconfigure
			else
				ReconfigurationConfig.log.log(Level.FINE,
						"{0} finds {1} present in affected RC groups {2}",
						new Object[] { this, newRCGroup, affectedNodes });
			final Map<String, Set<String>> mergees = this.isMergerGroup(
					newRCGroup, curRCGroups, newRCGroups, ncRecord);
			Map<String, Set<NodeIDType>> splitParentGroup = this.isSplitGroup(
					newRCGroup, curRCGroups, newRCGroups, ncRecord);
			int ncEpoch = ncRecord.getRCEpoch(newRCGroup);

			boolean invokeAggregatedMergeSplit = ReconfigurationConfig.isAggregatedMergeSplit()
					&& (!mergees.isEmpty() || splitParentGroup != null);

			if (curRCGroups.keySet().contains(newRCGroup)
					&& !invokeAggregatedMergeSplit) {

				// change current group
				debug += (this + " changing local group {" + newRCGroup + ":"
						+ (ncEpoch - 1) + "=" + curRCGroups.get(newRCGroup)
						+ "} to {" + newRCGroup + ":" + (ncEpoch) + "=" + newRCGroups
							.get(newRCGroup)) + "}";

				if ((mergees).isEmpty() || !ReconfigurationConfig.isAggregatedMergeSplit()) {
					this.repeatUntilObviated(new RCRecordRequest<NodeIDType>(
							this.getMyID(), new StartEpoch<NodeIDType>(this
									.getMyID(), newRCGroup, ncEpoch,
									newRCGroups.get(newRCGroup), curRCGroups
											.get(newRCGroup), mergeLists
											.get(newRCGroup)), // mergees
							RequestTypes.RECONFIGURATION_INTENT));
				}
			} else if (invokeAggregatedMergeSplit) {
				// change current group
				debug += (this
						+ (curRCGroups.keySet().contains(newRCGroup) ? " changing local group "
								: " creating ")
						+ "{"
						+ newRCGroup
						+ ":"
						+ (curRCGroups.keySet().contains(newRCGroup) ? ncEpoch - 1
								: ncEpoch)
						+ "="
						+ this.DB.getOldGroup(newRCGroup).values().iterator()
								.next()
						+ "}"

						+ (mergees.isEmpty() ? "" : " merging groups {"
								+ mergees + "}")

						+ (splitParentGroup == null ? "" : " splitting from {"
								+ splitParentGroup) + "}");
				this.aggregatedSplitMerge(newRCGroup, curRCGroups, newRCGroups,
						ncRecord, mergees, splitParentGroup);

			} else
				debug += "(" + this
						+ " relying on others to create non-local group {"
						+ newRCGroup + ":" + ncEpoch + "="
						+ newRCGroups.get(newRCGroup) + "})";
			debug += "\n";
		}
		return debug;
	}

	private void aggregatedSplitMerge(String newRCGroup,
			Map<String, Set<NodeIDType>> curRCGroups,
			Map<String, Set<NodeIDType>> newRCGroups,
			ReconfigurationRecord<NodeIDType> ncRecord,
			Map<String, Set<String>> mergees,
			Map<String, Set<NodeIDType>> splitParentGroup) {
		// spawn a new task to avoid blocking here
		this.protocolExecutor.submit(new Runnable() {

			public void run() {
				/* Aggregate mergee states, create replica group locally, and
				 * spawn WaitAckStartEpoch that will complete when all replicas
				 * have done the same. */
				Object monitor = new Object();
				String mergedStateFilename = Reconfigurator.this
						.spawnMergeeStopAndFetchStateTasks(Reconfigurator.this
								.getStopTasks(newRCGroup, mergees.keySet(),
										splitParentGroup, monitor, curRCGroups,
										newRCGroups, ncRecord), monitor,
								newRCGroup, ncRecord.getRCEpoch(newRCGroup));

				boolean created = Reconfigurator.this.DB.createReplicaGroup(
						newRCGroup, ncRecord.getRCEpoch(newRCGroup),
						LargeCheckpointer
								.createCheckpointHandle(mergedStateFilename),
						newRCGroups.get(newRCGroup));
				assert (created);

				// will issue the complete upon majority
				Reconfigurator.this.protocolExecutor
						.spawnIfNotRunning(new WaitAckStartEpoch<NodeIDType>(
						// passive start epoch
								new StartEpoch<NodeIDType>(Reconfigurator.this
										.getMyID(), newRCGroup, ncRecord
										.getRCEpoch(newRCGroup), newRCGroups
										.get(newRCGroup), true),
								Reconfigurator.this.DB));
			}
		});
	}

	private Map<String, Set<NodeIDType>> isSplitGroup(String rcGroup,
			Map<String, Set<NodeIDType>> curRCGroups,
			Map<String, Set<NodeIDType>> newRCGroups,
			ReconfigurationRecord<NodeIDType> ncRecord) {
		if (newRCGroups.keySet().contains(rcGroup)
				&& this.DB.isBeingAdded(rcGroup))
			return this.DB.getOldGroup(rcGroup);
		return null;
	}

	/**
	 * @param rcGroup
	 * @param curRCGroups
	 * @param newRCGroups
	 * @param ncRecord
	 * @return True if {@code rcGroup} is a group into which other groups are
	 *         being merged. This is true when a group is being deleted and it
	 *         consistent-hashes on to {@code rcGroup} in the new ring.
	 */
	private Map<String, Set<String>> isMergerGroup(String rcGroup,
			Map<String, Set<NodeIDType>> curRCGroups,
			Map<String, Set<NodeIDType>> newRCGroups,
			ReconfigurationRecord<NodeIDType> ncRecord) {
		Map<String, Set<String>> mergees = new ConcurrentHashMap<String, Set<String>>();
		if (newRCGroups.keySet().contains(rcGroup)) {
			for (NodeIDType curRCNode : ncRecord.getActiveReplicas()) {
				String curRCGroup = this.DB.getRCGroupName(curRCNode);
				if (this.DB.isBeingDeleted(curRCGroup)) {
					Map<String, Set<NodeIDType>> mergeGroup = this.DB
							.getNewGroup(curRCGroup);
					String mergeGroupName = mergeGroup.keySet().iterator()
							.next();
					if (mergeGroupName.equals(rcGroup))
						mergees.put(curRCGroup, Util.setToStringSet(mergeGroup
								.get(mergeGroupName)));
				}
			}
		}
		if (!mergees.isEmpty())
			mergees.put(
					rcGroup,
					Util.setToStringSet(this.DB.getOldGroup(rcGroup).values()
							.iterator().next()));
		return mergees;
	}

	/**
	 * 
	 * @param newRCGroup
	 * @param mergees
	 * @param splitParentGroup
	 * @param monitor
	 * @param curRCGroups
	 * @param newRCGroups
	 * @param ncRecord
	 * @return All of the {@link WaitAckStopEpoch} tasks needed to stop groups
	 *         being merged or split in order to create {@code newRCGroup}
	 */
	private Set<WaitAckStopEpoch<NodeIDType>> getStopTasks(String newRCGroup,
			Set<String> mergees, Map<String, Set<NodeIDType>> splitParentGroup,
			Object monitor, Map<String, Set<NodeIDType>> curRCGroups,
			Map<String, Set<NodeIDType>> newRCGroups,
			ReconfigurationRecord<NodeIDType> ncRecord) {
		Set<WaitAckStopEpoch<NodeIDType>> stopTasks = new HashSet<WaitAckStopEpoch<NodeIDType>>();
		for (String mergee : mergees) {
			Set<NodeIDType> mergeeGroup = this.DB.getOldGroup(newRCGroup)
					.values().iterator().next();
			stopTasks.add(new WaitAckStopEpoch<NodeIDType>(
					new StartEpoch<NodeIDType>(this.getMyID(), newRCGroup,
							ncRecord.getRCEpoch(newRCGroup), newRCGroups
									.get(newRCGroup), mergeeGroup, mergee,
							true, newRCGroups.containsKey(mergee) ? ncRecord
									.getRCEpoch(mergee) - 1 : ncRecord
									.getRCEpoch(mergee)), this.DB, monitor));
		}
		if (splitParentGroup != null) {
			String splitParent = splitParentGroup.keySet().iterator().next();
			stopTasks.add(new WaitAckStopEpoch<NodeIDType>(
					new StartEpoch<NodeIDType>(this.getMyID(), newRCGroup,
							ncRecord.getRCEpoch(newRCGroup), newRCGroups
									.get(newRCGroup), splitParentGroup.values()
									.iterator().next(), splitParent, false,
							ncRecord.getRCEpoch(splitParent) - 1), this.DB,
					monitor));
		}
		return stopTasks;
	}

	private String spawnMergeeStopAndFetchStateTasks(
			Set<WaitAckStopEpoch<NodeIDType>> stopTasks, Object monitor,
			String mergerGroup, int mergerGroupEpoch) {

		Map<String, String> finalStates = new ConcurrentHashMap<String, String>();

		ReconfigurationConfig.log.log(Level.INFO, "{0} starting wait on stop task monitors {1}",
				new Object[] { this, stopTasks });

		// FIXME: should start tasks inside synchronized?
		for (WaitAckStopEpoch<NodeIDType> stopTask : stopTasks)
			assert (this.protocolExecutor.spawnIfNotRunning(stopTask));

		synchronized (monitor) {
			while (!stopTasks.isEmpty())
				try {
					monitor.wait();
					for (Iterator<WaitAckStopEpoch<NodeIDType>> iter = stopTasks
							.iterator(); iter.hasNext();) {
						WaitAckStopEpoch<NodeIDType> stopTask = iter.next();
						String finalState = stopTask.getFinalState();
						if (finalState != null) {
							iter.remove();
							finalStates.put(
									stopTask.startEpoch.getPrevGroupName()
											+ ":"
											+ stopTask.startEpoch
													.getPrevEpochNumber(),
									finalState);
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
					ReconfigurationConfig.log.log(Level.INFO,
							"{0} interrupted while waiting on tasks {1}",
							new Object[] { this, stopTasks });
					throw new RuntimeException(
							"Task to spawn merge/split fetch tasks interrupted");
				}
		}
		ReconfigurationConfig.log.log(Level.INFO,
				"{0} finished waiting on all stop task monitors {1}",
				new Object[] { this, stopTasks });
		// merge mergee checkpoints
		return this.DB.app.fetchAndAggregateMergeeStates(finalStates,
				mergerGroup, mergerGroupEpoch);
		// can create the mergeGroup now with all the fetched states
	}

	/**
	 * This method "reconfigures" groups that will exist locally in the new ring
	 * but do not currently exist in the old ring. This "reconfiguration" is
	 * actually a group split operation, wherein an existing group is stopped
	 * and two new groups are created by splitting the final state of the
	 * stopped group, one with membership identical to the stopped group and the
	 * other corresponding to the new but currently non-existent group. A
	 * detailed example is described below.
	 * 
	 * Note: This method is unused with {@link ReconfigurationConfig#isAggregatedMergeSplit()}.
	 * 
	 * @param curRCGroups
	 * @param newRCGroups
	 * @param ncRecord
	 * @return A debug message for pretty-printing.
	 */
	@Deprecated
	private String splitExistingGroups(
			Map<String, Set<NodeIDType>> curRCGroups,
			Map<String, Set<NodeIDType>> newRCGroups,
			ReconfigurationRecord<NodeIDType> ncRecord) {
		String debug = ""; // just for prettier clustered printing
		// for each new group, initiate group change if and as needed
		for (String newRCGroup : newRCGroups.keySet()) {
			if (!curRCGroups.keySet().contains(newRCGroup)) {
				/* Create new group from scratch by splitting existing group.
				 * 
				 * Example: Suppose we have nodes Y, Z, A, C, D, E as
				 * consecutive RC nodes along the ring and we add B between A
				 * and C, and all groups are of size 3. Then, the group BCD is a
				 * new group getting added at nodes B, C, and D. This new group
				 * BCD must obtain state from the existing group CDE, i.e., the
				 * group CDE is getting split into two groups, BCD and CDE. One
				 * way to accomplish creation of the group BCD is to specify the
				 * previous group as CDE and just select the subset of state
				 * that gets remapped to BCD as the initial state. Below, we
				 * just acquire all of CDE's final state and simply choose what
				 * belongs to BCD while updating BCD's state at replica group
				 * creation time.
				 * 
				 * This operation will happen at C, and D, but not at B and E
				 * because E has no new group BCD that is not part of its
				 * existing groups, and B has nothing at all, not even a node
				 * config. */
				Map<String, Set<NodeIDType>> oldGroup = this.DB
						.getOldGroup(newRCGroup);
				assert (oldGroup != null && oldGroup.size() == 1);
				String oldGroupName = oldGroup.keySet().iterator().next();
				debug += this + " creating new group {" + newRCGroup + ":"
						+ ncRecord.getRCEpoch(newRCGroup) + "="
						+ newRCGroups.get(newRCGroup) + "} by splitting {"
						+ oldGroupName + ":"
						+ (ncRecord.getRCEpoch(oldGroupName) - 1) + "="
						+ oldGroup.get(oldGroupName) + "}\n";
				if (newRCGroup.equals(oldGroupName))
					continue; // no trivial splits

				// uncoordinated execute
				this.DB.execute(new RCRecordRequest<NodeIDType>(this.getMyID(),
						new StartEpoch<NodeIDType>(this.getMyID(), newRCGroup,
								ncRecord.getRCEpoch(newRCGroup), newRCGroups
										.get(newRCGroup), oldGroup
										.get(oldGroupName), oldGroupName,
								false, ncRecord.getRCEpoch(oldGroupName) - 1),
						RequestTypes.RECONFIGURATION_INTENT));
			}
		}
		return debug;
	}

	/**
	 * 
	 * This method "reconfigures" groups that will not exist locally in the new
	 * ring but do currently exist locally in the old ring. This
	 * "reconfiguration" is actually a group merge operation, wherein the old
	 * "mergee" group is stopped, the group which with the old group is supposed
	 * to merge (and will continue to exist locally in the new ring) is stopped,
	 * and the mergee group's final state is merged into the latter group simply
	 * through a paxos update operation. A detailed example and a discussion of
	 * relevant concerns is described below.
	 * 
	 * Note: This method is unused with {@link ReconfigurationConfig#isAggregatedMergeSplit()}.
	 * 
	 * @param curRCGroups
	 * @param newRCGroups
	 * @param ncRecord
	 * @return A debug message for pretty-printing.
	 */
	@Deprecated
	private String mergeExistingGroups(
			Map<String, Set<NodeIDType>> curRCGroups,
			Map<String, Set<NodeIDType>> newRCGroups,
			ReconfigurationRecord<NodeIDType> ncRecord) {
		/* Delete groups that no longer should exist at this node.
		 * 
		 * Example: Suppose we have nodes Y, Z, A, B, C, D, E as consecutive RC
		 * nodes along the ring and we are removing B between A and C, and all
		 * groups are of size 3.
		 * 
		 * Basic idea: For each node being deleted, if I belong to the deleted
		 * node's group, I need to reconfigure the deleted node's group by
		 * merging it with the node in the new ring to which the deleted node
		 * hashes.
		 * 
		 * In the example above, we need to remove group B at C by changing BCD
		 * to CDE. Likewise, at nodes D and E, we need to change group BCD to
		 * CDE.
		 * 
		 * C: BCD -> CDE (merge)
		 * 
		 * A merge is implemented as a reconfiguration that starts with
		 * WaitAckStopEpoch for the old group, but instead of starting the new
		 * group, it simply calls updateState on the new group to merge the
		 * stopped mergee group's final state into the new group.
		 * 
		 * Furthermore, the group ZAC is a new group getting added at node C
		 * because of the removal of B. There is no current group at C that
		 * needs to be stopped, however, one does need to stop the old group ZAB
		 * in order to reconfigure it to ZAC. One issue is that C doesn't even
		 * know ZAB's epoch number as the group doesn't exist locally at C. So
		 * we just let one of Z or A, not C, reconfigure ZAB in this case.
		 * 
		 * What if we are deleting B1, B2, and B3 from Y, Z, A, B1, B2, B3, C,
		 * D, E? The group ZAC has to get created at C, which can still be done
		 * by Z or A. Similarly, AB1B2 can be moved to ACD by A. However, B1B2B3
		 * can not be moved to CDE at C because CDE has to merge B1B2B3, B2B3C,
		 * and B3CD. C can conduct the latter two merges but not the first. To
		 * merge B1B2B3, at least one of B1, B2, or B3 must be up. The only
		 * compelling reason to delete all three of B1,B2, and B3 together is
		 * that they are all down, but in that case we can not delete them
		 * anyway until at least one of them comes back up. So we can delete at
		 * most as many nodes as the size of the reconfigurator replica group.
		 * 
		 * Actually, the exact condition is weaker (something like we can delete
		 * at most as many consecutive nodes as the size of the reconfigurator
		 * replica group, but we need to formally prove the
		 * necessity/sufficiency of this constraint). For now, simple and safe
		 * is good enough. */

		String debug = "";
		for (String curRCGroup : curRCGroups.keySet()) {
			if (!newRCGroups.containsKey(curRCGroup)
					&& this.DB.isBeingDeleted(curRCGroup)) {
				Map<String, Set<NodeIDType>> mergeGroup = this.DB
						.getNewGroup(curRCGroup);
				assert (mergeGroup != null && mergeGroup.size() == 1);
				String mergeGroupName = mergeGroup.keySet().iterator().next();

				/* mergeGroupName must be in my new groups and curRCGroup must
				 * exist locally. The latter is needed in order to know the
				 * epoch number of the group being merged. In the running
				 * example above, E does not satisfy both conditions because the
				 * mergeGroupName CDE exists at E but the mergee group BCD
				 * doesn't exist at E, so it is not in a position to conduct the
				 * reconfiguration (as it doesn't know which BCD epoch to stop
				 * and merge into CDE), so just one of C or D will conduct the
				 * merge in this case. */
				if (!newRCGroups.containsKey(mergeGroupName)
						|| this.DB.getEpoch(curRCGroup) == null)
					continue;

				// delete current group and merge into a new "mergeGroup"
				debug += (this + " merging current group {" + curRCGroup + ":" + this.DB
						.getReplicaGroup(curRCGroup))
						+ "} with {"
						+ mergeGroupName
						+ ":"
						+ (ncRecord.getRCEpoch(mergeGroupName))
						+ "="
						+ mergeGroup.get(mergeGroupName) + "}\n";

				/* Register the mergee groups right here so that they can be
				 * available upon reconfiguration complete and can be executed
				 * sequentially in the new epoch. It is also easy to look at the
				 * RC record and determine if all the merges are done.
				 * 
				 * It is better to first start a task to stop all mergee groups
				 * (including the mergeGroup) and get a copy of their final
				 * state on the local host and concatenate them into an
				 * initialState meant for the mergeGroup. This will obviate the
				 * current design of having to coordinate merges individually in
				 * the mergeGroup with a global state handle; each such
				 * coordinated merge operation can fail at a subset of replicas
				 * prompting them to lauch WaitAckStopEpoch tasks to try to
				 * re-coordinate the merge that would be redundant on the nodes
				 * on which the merge already succeeded. The current design is
				 * also more complex to debug and, most importantly, violates
				 * state machine semantics. Thus, it is possible that changes to
				 * a merged RC record don't result in the same state at all
				 * replicas because some replicas may not yet have completed the
				 * merge. Waiting until all replicas have completed all
				 * constituent merges is both more complicated to implement and
				 * is bad for liveness. The new design of assembling all merged
				 * state into an initialState before starting the paxos instance
				 * also has the benefit of cleanly dealing with crash recovery
				 * using paxos checkpoint transfers as usual.
				 * 
				 * Invariant: A state machine maintains state machine semantics,
				 * i.e., replicas start from the same initial state and any
				 * sequence of executed requests results in the same state at
				 * the end of the execution sequence at all replicas.
				 * 
				 * Note that the invariant above implicitly disallows "failed"
				 * request executions at a subset of replicas, i.e., a request R
				 * and a state S completely determine the resulting next state
				 * at any replica. Allowing R to fail at a strict subset of
				 * replicas while succeeding at others means that different
				 * replicas may go from the same state S to different possible
				 * next states, causing divergence.
				 * 
				 * The above design has now been implemented using the cleaner
				 * isAggregatedMergeSplit design. */
				this.protocolExecutor
						.spawnIfNotRunning(new WaitAckStopEpoch<NodeIDType>(
								new StartEpoch<NodeIDType>(this.getMyID(),
										mergeGroupName, ncRecord
												.getRCEpoch(mergeGroupName),
										mergeGroup.get(mergeGroupName),
										curRCGroups.get(curRCGroup),
										curRCGroup, true, ncRecord
												.getRCEpoch(curRCGroup)),
								this.DB));
			} else if (!newRCGroups.containsKey(curRCGroup)
					&& !this.DB.isBeingDeleted(curRCGroup)) {
				// delete current group and merge into a new "mergeGroup"
				debug += (this + " expecting others to delete current group {"
						+ curRCGroup + ":"
						+ (ncRecord.getRCEpoch(curRCGroup) - 1) + "=" + this.DB
							.getReplicaGroup(curRCGroup)) + "}\n";
			}
		}
		return debug;
	}

	private static final int MAX_OUTSTANDING_RECONFIGURATIONS = 100;

	/**
	 * This method issues reconfigurations for records replicated on active in a
	 * manner that limits the number of outstanding reconfigurations using the
	 * {@link #outstandingReconfigurations} queue.
	 */
	@SuppressWarnings({ "unchecked" })
	private boolean deleteActiveReplica(NodeIDType active,
			InetSocketAddress creator) {
		boolean initiated = this.DB.app.initiateReadActiveRecords(active);
		if (!initiated) {
			ReconfigurationConfig.log.log(Level.WARNING,
					"{0} deleteActiveReplica {1} unable to initiate read active records",
					new Object[] { this, active });
			return false;
		}
		int rcCount = 0;
		// this.setOutstanding(active);
		this.consistentNodeConfig.slateForRemovalActive(active);
		ReconfigurationRecord<NodeIDType> record = null;
		while ((record = this.DB.app.readNextActiveRecord(false)) != null) {
			ReconfigurationConfig.log.log(Level.FINEST,
					"{0} reconfiguring {1} in order to delete active {2}",
					new Object[] { this, record.getName(), active });
			try {
				this.DB.waitOutstanding(MAX_OUTSTANDING_RECONFIGURATIONS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return false;
			}
			// reconfigure name so as to exclude active
			Set<NodeIDType> newActives = new HashSet<NodeIDType>(
					record.getActiveReplicas());
			if(!newActives.contains(active)) continue;
			// else
			NodeIDType newActive = (NodeIDType) Util.getRandomOtherThan(
					this.consistentNodeConfig.getActiveReplicas(), newActives);
			if (newActive != null)
				newActives.add(newActive);
			newActives.remove(active);
			// Note: any REPLICATE_ALL record will be reconfigured
			if (this.initiateReconfiguration(record.getName(), record,
					newActives, creator, null, null, null, null, null, record.getReconfigureUponActivesChangePolicy())) {
				rcCount++;
				this.DB.addToOutstanding(record.getName());
				record = this.DB.getReconfigurationRecord(record.getName());
				if (record != null && record.getActiveReplicas() != null
						&& !record.getActiveReplicas().contains(active))
					// inelegant redundant check to handle concurrency
					this.DB.notifyOutstanding(record.getName());
			}
		}
		ReconfigurationConfig.log.log(Level.INFO,
				"{0} closing read active records cursor after initiating "
						+ "{1} reconfigurations in order to delete active {2}",
				new Object[] { this, rcCount, active });
		boolean closed = this.DB.app.closeReadActiveRecords();
		// this.setNoOutstanding();
		return initiated && closed;
	}

	private boolean addActiveReplica(NodeIDType active,
			InetSocketAddress creator) {
		ReconfigurationRecord<NodeIDType> record = null;

		boolean initiatedCursor = this.DB.app.initiateReadActiveRecords(active);
		if (!initiatedCursor) {
			ReconfigurationConfig.log.log(Level.WARNING,
					"{0} addActiveReplica {1} unable to initiate read active records",
					new Object[] { this, active });
			return false;
		}
		int rcCount = 0;
		Set<NodeIDType> newActives = null;
				
		// reconfigure AR_AR_NODES first 
		{
			record = this.DB
					.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES
							.toString());
			if (record != null
					&& (newActives = this.consistentNodeConfig
							.getActiveReplicas()) != null
							&& record.getActiveReplicas()!=null
							&& !record.getActiveReplicas().contains(active)
							&& !record.getNewActives().contains(active)
					&& (this.initiateReconfiguration(record.getName(), record,
							newActives, creator, null,
							null,
							// include initial state for AR_AR_NODES
							this.consistentNodeConfig
									.getActiveReplicasReadOnly().toString(),
							null, null, record
									.getReconfigureUponActivesChangePolicy()))) {
				try {
					ReconfigurationConfig.log
							.log(Level.INFO,
									"{0} initiated reconfiguration of {1}",
									new Object[] {
											this,
											AbstractReconfiguratorDB.RecordNames.AR_AR_NODES });
					this.DB.addToOutstanding(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES
							.toString());
					record = this.DB
							.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES
									.toString());
					if (record != null && record.getActiveReplicas() != null
							&& record.getActiveReplicas().contains(active))
						// inelegant redundant check to handle concurrency
						this.DB.notifyOutstanding(record.getName());
					
					this.DB.waitOutstanding(1);
					ReconfigurationConfig.log
					.log(Level.INFO,
							"{0} completed reconfiguration of {1}",
							new Object[] {
									this,
									AbstractReconfiguratorDB.RecordNames.AR_AR_NODES });

				} catch (InterruptedException e) {
					e.printStackTrace();
					ReconfigurationConfig.log
							.log(Level.WARNING,
									"{0} interrupted while waiting for {1} reconfiguration to complete",
									new Object[] { this, record.getName() });
					return false;
				}
			}
			else {
				ReconfigurationConfig.log
						.log(Level.INFO,
								"{0} unable to initiate reconfiguration for {1} record: [{2}]",
								new Object[] {
										this,
										AbstractReconfiguratorDB.RecordNames.AR_AR_NODES,
										record });
			}
		}
		// AR_AR_NODES reconfigured if here (or !initiatedReconfiguration)
		
		while ((record = this.DB.app.readNextActiveRecord(true)) != null) {
			// already addresses this as a special case above
			if(record.getName().equals(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString()))
				continue;

			ReconfigurationConfig.log.log(Level.FINEST,
					"{0} reconfiguring {1} in order to add active {2}",
					new Object[] { this, record.getName(), active });
			try {
				this.DB.waitOutstanding(MAX_OUTSTANDING_RECONFIGURATIONS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return false;
			}

			if(record.getReconfigureUponActivesChangePolicy()== ReconfigurationConfig.ReconfigureUponActivesChange.REPLICATE_ALL) {
				newActives = this.consistentNodeConfig.getActiveReplicas();
				// means reconfiguration already complete or underway
				if (record.getActiveReplicas().contains(active)
						|| (record.getNewActives().contains(active)))
					continue;
			}
			else if(record.getReconfigureUponActivesChangePolicy()== ReconfigurationConfig.ReconfigureUponActivesChange.CUSTOM) {
				newActives = this.shouldReconfigure(record.getName());
			}

			if (newActives != null
					&& this.initiateReconfiguration(record.getName(), record,
							newActives, creator, null, null, null, null, null,
							record.getReconfigureUponActivesChangePolicy())) {
				ReconfigurationConfig.log
				.log(Level.INFO,
						"{0} initiated reconfiguration of {1}",
						new Object[] {
								this,
								record.getName()});
				rcCount++;
				this.DB.addToOutstanding(record.getName());
				record = this.DB.getReconfigurationRecord(record.getName());
				if (record != null && record.getActiveReplicas() != null
						&& record.getActiveReplicas().contains(active))
					// inelegant redundant check to handle concurrency
					this.DB.notifyOutstanding(record.getName());
			}
		}
		
		ReconfigurationConfig.log.log(Level.INFO,
				"{0} closing read active records cursor after initiating "
						+ "{1} reconfigurations in order to add active {2}",
				new Object[] { this, rcCount, active });
		boolean closed = this.DB.app.closeReadActiveRecords();

		return initiatedCursor && closed;
	}
	
	/**
	 * This method reconfigures (in place) the AR_RC_NODES record at actives
	 * that contains the map of current reconfigurators. The reconfiguration
	 * is not strictly necessary but is the easiest way to update state at
	 * actives without introducing new packet types and code.
	 * 
	 * Note: The change to the comparable AR_AR_NODES is simply done along
	 * with all REPLICATE_ALL records when an active replica is added or
	 * deleted.
	 * 
	 * @param rcRecReq
	 * @return True if successfully changed reconfigurator map at actives.
	 */
	private boolean changeRCNodeConfigAtActives(
			RCRecordRequest<NodeIDType> rcRecReq) {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES
						.toString());
		if (record == null || !this.consistentNodeConfig.getReplicatedReconfigurators(record.getName()).contains(this.getMyID()))
			return true;
		ReconfigurationConfig.log.log(Level.INFO,
				"{0} reconfiguring RC map at actives upon adds/deletes {1}/{2}", new Object[] {
						this, rcRecReq.startEpoch.getNewlyAddedNodes(), rcRecReq.startEpoch.getDeletedNodes() });
		// else
		this.DB.addToOutstanding(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES
				.toString());
		boolean initiated = this.initiateReconfiguration(
				AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString(),
				record, record.getNewActives(), null, null, null,
				this.consistentNodeConfig.getReconfiguratorsReadOnly()
						.toString(), null, null,
				ReconfigurationConfig.ReconfigureUponActivesChange.REPLICATE_ALL);
		if (!initiated)
			ReconfigurationConfig.log.log(Level.SEVERE,
					"{0} unable to initiate reconfiguration for {1}",
					new Object[] { this,
							AbstractReconfiguratorDB.RecordNames.AR_RC_NODES });
		try {
			this.DB.waitOutstanding(1);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public ReconfiguratorRequest sendRequest(ReconfiguratorRequest request) {
		RequestCallbackFuture<ReconfiguratorRequest> callbackFuture;
		BasicReconfigurationPacket<?> packet = request instanceof ClientReconfigurationPacket ? (ClientReconfigurationPacket) request
				: request instanceof ServerReconfigurationPacket ? (ServerReconfigurationPacket<?>) request
						: null;
		if (packet == null)
			throw new RuntimeException("The "
					+ ReconfiguratorRequest.class.getSimpleName()
					+ " argument must either be a "
					+ ServerReconfigurationPacket.class.getSimpleName()
					+ " or a "
					+ ClientReconfigurationPacket.class.getSimpleName());

		this.protocolTask
				.handleEvent(
						packet,
						null,
						callbackFuture = new RequestCallbackFuture<ReconfiguratorRequest>(
								packet, null));
		try {
			return (ReconfiguratorRequest) callbackFuture.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public RequestCallbackFuture<ReconfiguratorRequest> sendRequest(
			ReconfiguratorRequest request,
			Callback<Request, ReconfiguratorRequest> callback) {
		RequestCallbackFuture<ReconfiguratorRequest> callbackFuture;
		BasicReconfigurationPacket<?> packet = request instanceof ClientReconfigurationPacket ? (ClientReconfigurationPacket) request
				: request instanceof ServerReconfigurationPacket ? (ServerReconfigurationPacket<?>) request
						: null;
		if (packet == null)
			throw new RuntimeException("The "
					+ ReconfiguratorRequest.class.getSimpleName()
					+ " argument must either be a "
					+ ServerReconfigurationPacket.class.getSimpleName()
					+ " or a "
					+ ClientReconfigurationPacket.class.getSimpleName());
		this.protocolTask
				.handleEvent(
						packet,
						null,
						callbackFuture = new RequestCallbackFuture<ReconfiguratorRequest>(
								packet, callback));
		return callbackFuture;

	}

	/**
	 * If only a subset of reconfigurators get a node config change intent, they
	 * could end up never executing the intent and therefore never doing the
	 * split/change/merge sequence. If only a single node is in this situation,
	 * there shouldn't be a problem as reconfiguration of reconfigurator groups
	 * is done redundantly by all relevant reconfigurators. However, if multiple
	 * nodes miss the NC change intent, say, only a single node executes it in
	 * the worst case, it will go ahead and create the next NC epoch but the
	 * other reconfigurators will not do anything for groups other than those at
	 * that reconfigurator. To address this problem, there must be enough
	 * information in the NC change complete for reconfigurators that missed the
	 * corresponding intent to go ahead and initiate the change/split/merge
	 * sequence anyway. There is of course enough information as all that is
	 * really needed is the old and new set of reconfigurators.
	 * 
	 * This fix has now been implemented using the
	 * {@link #executed(Request, boolean)} callback in AbstractReconfiguratorDB
	 * that gets it via {@link RepliconfigurableReconfiguratorDB} that in turn
	 * gets it from {@link AbstractReplicaCoordinator}.
	 * 
	 */ 
}
