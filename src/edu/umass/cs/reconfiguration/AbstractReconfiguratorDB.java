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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorCallback;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorDB;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.interfaces.Repliconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentHashing;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.DiskMap;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This class is the reconfigurator "application", essentially a
 *            database. It is {@link Replicable} and {@link Reconfigurable} and
 *            {@link ReconfiguratorDB}. {@link SQLReconfiguratorDB} is an
 *            example of a class concretizing this class.
 * 
 *            It is actually not important for this database to be persistent
 *            for safety reasons when paxos provides fault recovery, but we use
 *            the disk because all records may not fit in memory. In fact,
 *            {@link SQLReconfiguratorDB} currently uses {@link DiskMap} as the
 *            default option that asynchronously swaps to disk in the
 *            background.
 * 
 *            This class relies on reflection to automatically map incoming
 *            reconfiguration packets to their respective handlers, but it only
 *            expects to process two types of packets {@link RCRecordRequest}
 *            and {@link StopEpoch}.
 */

public abstract class AbstractReconfiguratorDB<NodeIDType> implements
		Repliconfigurable, ReconfiguratorDB<NodeIDType> {

	/**
	 * Constant RC record name keys. Currently there is only one, for the set of
	 * all reconfigurators.
	 */
	public static enum RecordNames {
		/**
		 * The record key for the RC record holding the set of all
		 * reconfigurators. This name is replicated at all reconfigurators and
		 * is reconfigured when reconfigurators are added or removed.
		 */
		RC_NODES,

		/**
		 * The record key for the RC record holding the set of all active
		 * replicas. This name is replicated at all reconfigurators and is
		 * updated (but not reconfigured) when active replicas are added or
		 * removed, and is reconfigured when reconfigurators are added or
		 * removed.
		 */
		AR_NODES, 
		
		/**
		 * The record key for a special name replicated at all current active replicas
		 * that simply contains a map of all current active replicas. This name is
		 * reconfigured when active replicas are added or removed. It is analogous to
		 * AR_NODES replicated at all reconfigurators in that its reconfiguration record
		 * is replicated at all reconfigurators, but differs in that the name itself
		 * is replicated at active replicas.
		 */
		AR_AR_NODES, 
		
		/**
		 * Similar in spirit to {@link #AR_AR_NODES} but for reconfigurators.
		 */
		AR_RC_NODES
	};

	protected final NodeIDType myID;
	protected final ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig;
	protected boolean recovering = true;

	private static final Logger log = (ReconfigurationConfig.getLogger());

	/**
	 * @param myID
	 * @param nc
	 */
	public AbstractReconfiguratorDB(NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> nc) {
		this.myID = myID;
		this.consistentNodeConfig = nc;
	}

	/**
	 * @param name
	 * @param epoch
	 * @return ReconfigurationRecord for {@code name:epoch}.
	 */
	public ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name, int epoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		return record != null && record.getEpoch() == epoch ? record : null;
	}


	/***************** Paxos related methods below ***********/
	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) {
		log.log(Level.FINE, "{0} executing {1}", new Object[] { this, request.getSummary() });
		if (request.getServiceName().equals(Request.NO_OP)
				&& request.toString().equals(Request.NO_OP))
			return true;
		assert (request instanceof BasicReconfigurationPacket<?>) : request;
		boolean handled = false;
		// cast checked by assert above
		@SuppressWarnings("unchecked")
		BasicReconfigurationPacket<NodeIDType> rcPacket = (BasicReconfigurationPacket<NodeIDType>) request;
		if (this.uglyRecoveryHack(rcPacket, this.recovering))
			handled = true;
		else
			handled = (Boolean) AbstractReconfiguratorDB.autoInvokeMethod(this,
					rcPacket, doNotReplyToClient, this.consistentNodeConfig);
		return handled;
	}

	protected static final Object autoInvokeMethod(Object target,
			BasicReconfigurationPacket<?> rcPacket, boolean recovery,
			Stringifiable<?> unstringer) {
		try {
			return target
					.getClass()
					.getMethod(
							ReconfigurationPacket.HANDLER_METHOD_PREFIX
									+ ReconfigurationPacket.getPacketTypeClassName(rcPacket
											.getType()),
							ReconfigurationPacket.getPacketTypeClass(rcPacket
									.getType()), boolean.class)
					.invoke(target, rcPacket, recovery);
		} catch (NoSuchMethodException nsme) {
			nsme.printStackTrace();
		} catch (InvocationTargetException ite) {
			ite.printStackTrace();
		} catch (IllegalAccessException iae) {
			iae.printStackTrace();
		}
		return null;
	}

	/* We want NC complete RCRecordRequest to be non-blocking during recovery.
	 * Otherwise, we may not even get started with finishPendingReconfigurations
	 * that in turn may be required for unblocking the NC complete. */
	private boolean uglyRecoveryHack(
			final BasicReconfigurationPacket<NodeIDType> rcPacket,
			boolean recovering) {
		if (recovering
				&& rcPacket.getServiceName().equals(
						RecordNames.RC_NODES.toString())) {
			(new Thread(new Runnable() {
				public void run() {
					autoInvokeMethod(AbstractReconfiguratorDB.this, rcPacket,
							recovering, consistentNodeConfig);
				}
			})).start();
			return true;
		}
		return false;
	}
	
	/**
	 * @param report
	 * @return True if demand report is handled successfully. False means that
	 *         it may not have been processed.
	 */
	public boolean handleDemandReport(DemandReport<NodeIDType> report, boolean recovery) {
		return this.updateDemandStats(report);
	}
	
	private ReconfiguratorCallback callback = null;
	
	public void setCallback(ReconfiguratorCallback callback) {
		this.callback = callback;
	}
	
	/**
	 * If a reconfiguration intent is being registered, a protocol task must be
	 * started that ensures that the reconfiguration completes successfully.
	 * 
	 * @param rcRecReq
	 * @param recovery
	 * @return True if the record was handled successfully.
	 */
	public boolean handleRCRecordRequest(RCRecordRequest<NodeIDType> rcRecReq,
			boolean recovery) {

		// create RC record upon a name creation request
		if (rcRecReq.startEpoch.isInitEpoch()
		// don't create if delete is being re-executed
				&& !rcRecReq.isDeleteIntentOrPrevDropComplete()
				// record==null ensures it is not waiting delete
				&& this.getReconfigurationRecord(rcRecReq.getServiceName()) == null)
			if (!rcRecReq.startEpoch.isBatchedCreate())
				this.createReconfigurationRecord(new ReconfigurationRecord<NodeIDType>(
						rcRecReq.getServiceName(), rcRecReq.startEpoch
								.getEpochNumber() - 1,
						rcRecReq.startEpoch.curEpochGroup, rcRecReq.startEpoch.getReconfigureUponActivesChangePolicy()));
			else if (!this.createReconfigurationRecords(
					rcRecReq.startEpoch.getNameStates(),
					rcRecReq.startEpoch.getCurEpochGroup(), rcRecReq.startEpoch.getReconfigureUponActivesChangePolicy()))
				return false;

		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(rcRecReq.getServiceName());

		assert (record != null || rcRecReq.isReconfigurationPrevDropComplete() || (rcRecReq
				.isReconfigurationComplete() && rcRecReq.startEpoch
				.isDeleteRequest())) : this.getMyID() + " : " + rcRecReq;
		if (record == null)
			if ((rcRecReq.isReconfigurationComplete() && rcRecReq.startEpoch
					.isDeleteRequest())) {
				log.log(Level.INFO, "{0} found no record for request",
						new Object[] { this, rcRecReq.getSummary() });
				return true;
			} else
				return false;

		log.log(Level.FINE,
				"{0} received RCRecordRequest {1} while rcRecord = {2}",
				new Object[] { this, rcRecReq.getSummary(), record.getSummary() });

		// verify legitimate transition and legitimate node config change
		if (!this.isLegitTransition(rcRecReq, record)
				|| !this.isLegitimateNodeConfigChange(rcRecReq, record)) {
			log.log(Level.FINE,
					"{0} received illegitimate RCRecordRequest {1} while rcRecord = {2}",
					new Object[] { this, rcRecReq.getSummary(),
							record.getSummary() });
			return false;
		}

		// wait till node config change is complete
		if (rcRecReq.isNodeConfigChange()
				&& rcRecReq.isReconfigurationComplete()) {
			// should not be here at node config creation time
			assert (!rcRecReq.startEpoch.getPrevEpochGroup().isEmpty());

			if (record.getEpoch() - rcRecReq.getEpochNumber() < 0
					&& record.isReady() && this.callback != null)
				this.callback
						.executed(
								new RCRecordRequest<NodeIDType>(
										rcRecReq.getInitiator(),
										rcRecReq.startEpoch,
										RCRecordRequest.RequestTypes.RECONFIGURATION_INTENT),
								true);
			// wait for all local RC groups to be up to date
			if (this.selfWait(rcRecReq)) {
				log.log(Level.INFO,
						"{0} blocked upon {1}  when record = {2}",
						new Object[] { this, rcRecReq.getSummary(),
								record.getSummary() });
				return false;
			}
			// delete lower node config versions from node config table
			this.garbageCollectOldReconfigurators(rcRecReq.getEpochNumber() - 1);
			// garbage collect soft socket address mappings for deleted RC nodes
			this.consistentNodeConfig.removeReconfiguratorsSlatedForRemoval();

			String print = AbstractReconfiguratorDB.RecordNames.RC_NODES
					.toString()
					+ ":"
					+ rcRecReq.getEpochNumber()
					+ "="
					+ this.consistentNodeConfig.getReconfigurators() + "  ";
			log.log(Level.INFO, "{0} {1}", new Object[] { this, print });
			if (rcRecReq.getInitiator().equals(getMyID()))
				System.out.print(print);
		}

		boolean handled = false;
		if (rcRecReq.isReconfigurationIntent()) {
			// READY -> WAIT_ACK_STOP
			log.log(Level.FINE,
					"{0} received {1}; changing state {2} {3} {4} -> {5} {6} {7}",
					new Object[] { this, rcRecReq.getSummary(),
							rcRecReq.getServiceName(), record.getEpoch(),
							record.getState(), rcRecReq.getEpochNumber() - 1,
							ReconfigurationRecord.RCStates.WAIT_ACK_STOP,
							rcRecReq.startEpoch.getCurEpochGroup() });

			handled = rcRecReq.startEpoch.isBatchedCreate() ?
			// batched create
			this.setStateInitReconfiguration(
					rcRecReq.startEpoch.getNameStates(),
					rcRecReq.getEpochNumber() - 1,
					ReconfigurationRecord.RCStates.WAIT_ACK_STOP,
					rcRecReq.startEpoch.getCurEpochGroup()) :
			// typical unbatched create
					this.setStateInitReconfiguration(rcRecReq.getServiceName(),
							rcRecReq.getEpochNumber() - 1,
							ReconfigurationRecord.RCStates.WAIT_ACK_STOP,
							rcRecReq.startEpoch.getCurEpochGroup());
		} else if (rcRecReq.isReconfigurationComplete()) {
			// WAIT_ACK_START -> READY
			log.log(Level.FINE,
					"{0} received {1}; changing state {2} {3} {4} -> {5} {6}",
					new Object[] { this, rcRecReq.getSummary(),
							rcRecReq.getServiceName(), record.getEpoch(),
							record.getState(), rcRecReq.getEpochNumber(),
							ReconfigurationRecord.RCStates.READY });

			handled = rcRecReq.startEpoch.isBatchedCreate() ? this
					.setStateMerge(rcRecReq.startEpoch.getNameStates(),
							rcRecReq.getEpochNumber(),
							ReconfigurationRecord.RCStates.READY_READY,
							rcRecReq.startEpoch.getCurEpochGroup())
					: this.setStateMerge(
							rcRecReq.getServiceName(),
							rcRecReq.getEpochNumber(),
							rcRecReq.startEpoch.noCurEpochGroup() ? ReconfigurationRecord.RCStates.WAIT_DELETE
									: rcRecReq.startEpoch.noPrevEpochGroup() ? ReconfigurationRecord.RCStates.READY_READY
											: ReconfigurationRecord.RCStates.READY,
							rcRecReq.startEpoch.getCurEpochGroup(),
							rcRecReq.startEpoch.getMergees());
			// merge ops should be specified at new epoch creation time
		} else if (rcRecReq.isDeleteIntent()) {
			// WAIT_ACK_STOP -> WAIT_DELETE
			log.log(Level.FINE,
					"{0} received {1}; changing state {2} {3} {4} -> DELETE",
					new Object[] { this, rcRecReq.getSummary(),
							rcRecReq.getServiceName(), record.getEpoch(),
							record.getState() });

			handled = this
					.markDeleteReconfigurationRecord(rcRecReq.getServiceName(),
							rcRecReq.getEpochNumber() /*- 1*/);
		} else if (rcRecReq.isReconfigurationPrevDropComplete()) {
			// READY -> READY_READY or WAIT_DELETE -> DELETE
			log.log(Level.FINE,
					"{0} received {1}; changing state {2} {3} {4} -> READY_READY/DELETE",
					new Object[] { this, rcRecReq.getSummary(),
							rcRecReq.getServiceName(), record.getEpoch(),
							record.getState(), });

			handled =
			// typical reconfiguration READY -> READY_READY
			rcRecReq.startEpoch.hasCurEpochGroup() ? this.setState(
					rcRecReq.getServiceName(), rcRecReq.getEpochNumber(),
					RCStates.READY_READY)
			// isDeleteable WAIT_DELETE -> DELETE
					: rcRecReq.startEpoch.noCurEpochGroup()
							&& record.isDeletable() ? this
							.deleteReconfigurationRecord(
									rcRecReq.getServiceName(),
									rcRecReq.getEpochNumber())
					// else return value doesn't really matter
							: true;
		} else if (rcRecReq.isReconfigurationMerge()) {
			// MERGE
			log.log(Level.FINE,
					"{0} received {1}; merging state {2} {3} into {4} {5}{6}",
					new Object[] { this, rcRecReq.getSummary(),
							rcRecReq.startEpoch.getPrevGroupName(),
							rcRecReq.startEpoch.getPrevEpochNumber(),
							rcRecReq.getServiceName(), record.getEpoch(),
							record.getState() });
			try {
				handled = this.mergeState(rcRecReq.getServiceName(),
						rcRecReq.getEpochNumber(),
						rcRecReq.startEpoch.getPrevGroupName(),
						rcRecReq.startEpoch.getPrevEpochNumber(),
						rcRecReq.startEpoch.initialState);
			} catch (Exception e) {
				log.severe(this + " incurred exception in mergeState "
						+ e.getMessage());
				handled = false;
			}
		} else
			throw new RuntimeException("Received unexpected RCRecordRequest");

		log.log(Level.INFO, "{0} {1} {2}; record = {3} ", new Object[] { this,
				handled ? "successfully handled" : "turned into a noop",
				rcRecReq.getSummary(), record.getSummary() });

		if (handled
				&& (rcRecReq.isReconfigurationComplete() || rcRecReq
						.isReconfigurationMerge())
				&& this.isRCGroupName(record.getName()))
			// notify to wake up node config completion wait
			selfNotify();
		else if (handled && rcRecReq.isReconfigurationComplete()
				&& rcRecReq.isNodeConfigChange())
			assertMergesAllDone();

		return handled;
	}

	private void assertMergesAllDone() {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());
		for (NodeIDType rcNode : ncRecord.getNewActives()) {
			ReconfigurationRecord<NodeIDType> record = this
					.getReconfigurationRecord(this.getRCGroupName(rcNode),
							ncRecord.getRCEpoch(this.getRCGroupName(rcNode)));
			assert (record == null || record.areMergesAllDone());
		}
	}

	/* Checks that oldGroup is current group and newGroup differs from old by
	 * exactly one node. */
	private boolean isLegitimateNodeConfigChange(
			RCRecordRequest<NodeIDType> rcRecReq,
			ReconfigurationRecord<NodeIDType> record) {
		if (!rcRecReq.getServiceName().equals(RecordNames.RC_NODES.toString()))
			return true;
		boolean consistent = rcRecReq.startEpoch.getPrevEpochGroup().equals(
				record.getActiveReplicas());
		Set<NodeIDType> oldGroup = rcRecReq.startEpoch.getPrevEpochGroup();
		Set<NodeIDType> newGroup = rcRecReq.startEpoch.getCurEpochGroup();
		consistent = consistent && differByOne(oldGroup, newGroup);
		return consistent;
	}

	private boolean differByOne(Set<NodeIDType> s1, Set<NodeIDType> s2) {
		return (s1.containsAll(s2) && (s1.size() == (s2.size() + 1)))
				|| (s2.containsAll(s1) && (s2.size() == (s1.size() + 1)));
	}

	public String toString() {
		return "RCDB" + myID;
	}

	/* doNotReplyToClient for this "app" is a no-op as it never replies to some
	 * "client". All messaging is done by a single reconfigurator node. The DB
	 * only reflects state changes. */
	@Override
	public boolean execute(Request request) {
		return this.execute(request, false);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Request getRequest(String stringified) throws RequestParseException {
		if (stringified.equals(Request.NO_OP))
			return getNoopRequest(stringified);
		BasicReconfigurationPacket<NodeIDType> rcPacket = null;
		try {
			rcPacket = (BasicReconfigurationPacket<NodeIDType>) ReconfigurationPacket
					.getReconfigurationPacket(new JSONObject(stringified),
							this.consistentNodeConfig);
		} catch (JSONException e) {
			log.severe(this + " encountered JSONException trying to decode ["
					+ stringified + "]");
			e.printStackTrace();
		}
		return rcPacket;
	}

	protected static final Request getNoopRequest(String stringified) {
		if (stringified.equals(Request.NO_OP)) {
			return new Request() {
				@Override
				public IntegerPacketType getRequestType() {
					return new IntegerPacketType() {
						@Override
						public int getInt() {
							return Integer.MAX_VALUE;
						}
					};
				}

				@Override
				public String getServiceName() {
					return Request.NO_OP;
				}

				@Override
				public String toString() {
					return Request.NO_OP;
				}
			};
		}
		return null;
	}

	/* Some methods below that throw a runtime exception saying that they should
	 * have never been called are so because, with the current design, these
	 * methods are subsumed by Reconfigurator and never directly called. The
	 * current call chain is PacketDemultiplexer -> Reconfigurator ->
	 * RepliconfigurableReconfigurator.handleIncoming(.) ->
	 * this.handleRequest(.). The getRequest and getRequestTypes methods are
	 * only used for demultiplexing and the set of packet types of this class
	 * are a subset of those of Reconfigurator. */

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>(); // empty
	}

	// Reconfigurable methods below
	@Override
	public ReconfigurableRequest getStopRequest(String name, int epoch) {
		StopEpoch<NodeIDType> stop = new StopEpoch<NodeIDType>(this.getMyID(),
				name, epoch);
		assert (stop instanceof ReplicableRequest);
		return stop;
	}

	/**
	 * @param name
	 * @return Current epoch
	 */
	@Override
	public synchronized Integer getEpoch(String name) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		return (record != null ? record.getEpoch() : null);
	}

	@Override
	public String getFinalState(String name, int epoch) {
		throw new RuntimeException(
				"Method not yet implemented and should never have been called"
						+ "as AbstractReconfiguratorDB uses PaxosReplicaCoordinator");
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		throw new RuntimeException(
				"Method not yet implemented and should never have been called"
						+ "as AbstractReconfiguratorDB uses PaxosReplicaCoordinator");
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		throw new RuntimeException(
				"Method not yet implemented and should never have been called"
						+ "as AbstractReconfiguratorDB uses PaxosReplicaCoordinator");
	}

	protected static final boolean TWO_PAXOS_RC = Config
			.getGlobalBoolean(RC.TWO_PAXOS_RC);

	/* A transition using an RCRecordRequest is legitimate iff if takes a record
	 * in the same epoch from READY */
	private boolean isLegitTransition(RCRecordRequest<NodeIDType> rcRecReq,
			ReconfigurationRecord<NodeIDType> record) {
		assert (record != null) : rcRecReq;

		// always ignore lower epochs
		if (rcRecReq.getEpochNumber() - record.getEpoch() < 0)
			return false;
		/* We need to consider both ==1 and >1 for epoch numbers as this
		 * particular node may have missed a few epochs. The received RC record
		 * must either initiate a reconfiguration or announce its completion
		 * even when this replica is waiting on an ackStop for the preceding
		 * epoch (something that is rare during gracious execution but can
		 * happen if a secondary replica takes over and completes the
		 * reconfiguration while the primary is still waiting for the previous
		 * epoch to stop). */
		if (rcRecReq.getEpochNumber() - record.getEpoch() >= 1) {
			// initiating reconfiguration to next epoch
			return
			// ready to reconfigure
			(record.isReconfigurationReady() && rcRecReq
					.isReconfigurationIntent())
			// waitAckStop and reconfiguration/delete complete or delete intent
					|| ((record.getState().equals(RCStates.WAIT_ACK_STOP)
					/* It is possible that the reconfiguration intent or the
					 * uncoordinated createReconfigurationRecord step does not
					 * happen at a node but it gets the paxos-coordinated
					 * complete for the higher epoch, so we must allow a
					 * transition from n-1:READY (as opposed to
					 * n-1:WAIT_ACK_STOP) to n:READY. This can only happen when
					 * isAggregatedMergeSplit. */
					|| (ReconfigurationConfig.isAggregatedMergeSplit() && record
							.getState().equals(RCStates.READY))) && (rcRecReq
							.isReconfigurationComplete()
					/* Batch creation initializes records to -1:WAIT_ACK_STOP in
					 * order to avoid two update steps, so we need the intent to
					 * be considered legitimate still. */
					|| rcRecReq.startEpoch.isBatchedCreate()))
					// higher epoch possible only if legitimate
					|| !TWO_PAXOS_RC;
			/* If a reconfiguration intent is allowed only from READY, we have a
			 * problem during recovery when reconfiguration completion is not
			 * automatically rolled forward. So reconfiguration initiations will
			 * fail because the current state won't be READY. Every
			 * reconfiguration from after the most recent checkpoint will have
			 * to be explicitly replayed again. One option is to allow
			 * illegitimate transitions during recovery. */
		}
		/* In the same epoch, the only state change possible is by receiving an
		 * RC record announcing reconfiguration completion while waiting for a
		 * majority ackStarts. */
		if (rcRecReq.getEpochNumber() - record.getEpoch() == 0) {
			return
			// waiting on ackStart and reconfiguration complete
			(record.getState().equals(RCStates.WAIT_ACK_START) && rcRecReq
					.isReconfigurationComplete())
			// ready and reconfiguration merge
					|| (record.isReady() && (rcRecReq.isReconfigurationMerge() || rcRecReq
							.isReconfigurationPrevDropComplete()))
					// delete pending and delete complete
					|| (record.getState().equals(RCStates.WAIT_DELETE) && rcRecReq
							.isReconfigurationPrevDropComplete());
		}
		return false;
	}

	/* Checks if all new RC groups are ready. */
	private boolean areRCChangesComplete() {
		return this.areRCChangesCompleteDebug().isEmpty();
	}

	protected String areRCChangesCompleteDebug() {
		long t0 = System.currentTimeMillis();
		Map<String, Set<NodeIDType>> newRCGroups = this.getNewRCGroups();
		boolean rcComplete = true;
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());
		String debug = "";
		for (String newRCGroup : newRCGroups.keySet()) {
			ReconfigurationRecord<NodeIDType> record = this
					.getReconfigurationRecord(newRCGroup);
			rcComplete = rcComplete && record != null
					// epoch matches
					&& ((record.getEpoch() == ncRecord
							.getRCEpoch(this.consistentNodeConfig
									.valueOf(newRCGroup)))
							// ready and all merges done
							&& ((record.isReconfigurationReady()
									&& newRCGroups.get(newRCGroup).equals(
											record.getActiveReplicas()) && record
										.areMergesAllDone())
							// or post-ready ( => ready and all merges done)
							|| (record.getState()
									.equals(RCStates.WAIT_ACK_STOP)))
					// or moved on to strictly higher epochs (possible?)
					|| (record.getEpoch() - ncRecord.getRCEpoch(newRCGroup) > 0));
			if (!rcComplete) {
				debug += (record != null) ? record.getSummary()
						+ (record.isReady() ? "->"
								+ ncRecord.getRCEpoch(newRCGroup) + ":"
								+ RCStates.READY + ":"
								+ newRCGroups.get(newRCGroup) : "")
						: "record = null";
				break;
			}
		}
		if (!rcComplete)
			log.log(Level.INFO,
					"{0}:{1} has not completed all constituent RC group changes yet, e.g., {2}",
					new Object[] { this, ncRecord.getSummary(), debug, });
		DelayProfiler.updateDelay("isNodeConfigChangeComplete", t0);
		return debug;
	}

	protected Map<String, Set<NodeIDType>> getNewRCGroups() {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());
		return this.getRCGroups(this.getMyID(), ncRecord.getNewActives());
	}

	protected Map<String, Set<NodeIDType>> getOldRCGroups() {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());
		return this.getRCGroups(this.getMyID(), ncRecord.getActiveReplicas());
	}

	protected Set<String> getMergeList(String newRCGroupName) {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());
		Set<NodeIDType> deletedNodes = diff(ncRecord.getActiveReplicas(),
				ncRecord.getNewActives());

		Set<String> mergees = new HashSet<String>();
		for (NodeIDType deletedNode : deletedNodes) {
			String merger = this
					.getRCGroupName(this
							.getNewConsistentHashRing()
							.getReplicatedServersArray(
									this.getRCGroupName(deletedNode)).get(0));
			if (merger.equals(newRCGroupName))
				mergees.add(this.getRCGroupName(deletedNode));
		}
		if (!mergees.isEmpty())
			log.log(Level.INFO, "{0} merging list of mergees {1}",
					new Object[] { this, mergees });
		return mergees;
	}

	protected HashMap<String, Set<String>> getMergeLists() {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());
		Set<NodeIDType> deletedNodes = diff(ncRecord.getActiveReplicas(),
				ncRecord.getNewActives());

		HashMap<String, Set<String>> mergeLists = new HashMap<String, Set<String>>();
		for (NodeIDType deletedNode : deletedNodes) {
			String merger = this
					.getRCGroupName(this
							.getNewConsistentHashRing()
							.getReplicatedServersArray(
									this.getRCGroupName(deletedNode)).get(0));
			if (!mergeLists.containsKey(merger))
				mergeLists.put(merger, new HashSet<String>());
			Set<String> mergees = mergeLists.get(merger);
			mergees.add(this.getRCGroupName(deletedNode));
			mergeLists.put(merger, mergees);
		}
		return mergeLists;
	}

	// return s1 - s2
	private Set<NodeIDType> diff(Set<NodeIDType> s1, Set<NodeIDType> s2) {
		Set<NodeIDType> diff = new HashSet<NodeIDType>();
		for (NodeIDType node : s1)
			if (!s2.contains(node))
				diff.add(node);
		return diff;
	}

	/* This method currently reconstructs a new consistent hashing structure
	 * afresh each time it is called, which may be inefficient. But it is
	 * unclear where we can store it in a manner that is safe, so we just
	 * reconstruct it from the DB on demand. It is used only while reconfiguring
	 * reconfigurators, which is rare. */
	protected Map<String, Set<NodeIDType>> getRCGroups(NodeIDType rc,
			Set<NodeIDType> allRCs, boolean print) {
		assert (rc != null && allRCs != null);
		ConsistentHashing<NodeIDType> newRCCH = new ConsistentHashing<NodeIDType>(
				allRCs);
		HashMap<String, Set<NodeIDType>> groups = new HashMap<String, Set<NodeIDType>>();
		String s = "RC groups with " + allRCs + " at " + getMyID() + " = ";
		// compute RC groups as in createDefaultGroups
		for (NodeIDType node : allRCs) {
			Set<NodeIDType> group = newRCCH.getReplicatedServers(node
					.toString());
			if (group.contains(rc)) {
				s += " [" + (node + ":" + group) + "] ";
				groups.put(this.getRCGroupName(node), group);
			}
		}
		if (print)
			System.out.println(s + "\n");

		return groups;
	}

	protected Map<String, Set<NodeIDType>> getRCGroups(NodeIDType rc,
			Set<NodeIDType> allRCs) {
		return this.getRCGroups(rc, allRCs, false);
	}

	private NodeIDType getMyID() {
		return this.myID;
	}

	private RCRecordRequest<NodeIDType> blockingRequest = null;

	private synchronized boolean selfWait(RCRecordRequest<NodeIDType> rcRecReq) {
		if (!this.areRCChangesComplete()) {
			this.blockingRequest = rcRecReq;
			return true;
		} else
			this.blockingRequest = null;
		try {
			while (!this.areRCChangesComplete()) {
				this.wait(5000);
				;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	private synchronized void selfNotify() {
		if (this.blockingRequest != null) {
			this.handleRCRecordRequest(this.blockingRequest, false);
		}
		this.notifyAll();
	}

	/**
	 * @param stopEpoch
	 * @param recovery
	 * @return If this {@code stopEpoch} was handled successfully.
	 */
	public boolean handleStopEpoch(StopEpoch<NodeIDType> stopEpoch,
			boolean recovery) {
		log.log(Level.INFO, "{0} stop-executed {1}", new Object[] { this,
				stopEpoch.getSummary() });
		// for exactly once semantics for merges
		this.clearMerged(stopEpoch.getServiceName(), stopEpoch.getEpochNumber());
		return true;
	}
	
	/**
	 * @param requestActives
	 * @param recovery
	 * @return True
	 */
	protected boolean handleRequestActiveReplicas(RequestActiveReplicas requestActives,
			boolean recovery) {
		// no-op
		return true;
	}

	protected String getRCGroupName(NodeIDType node) {
		return node.toString();
	}

	protected String getRCGroupName(String name) {
		if (name.equals(AbstractReconfiguratorDB.RecordNames.RC_NODES
				.toString()))
			return name;
		else if (name.equals(AbstractReconfiguratorDB.RecordNames.AR_NODES
				.toString()))
			return AbstractReconfiguratorDB.RecordNames.RC_NODES.toString();

		else if (this.isRCGroupName(name))
			return name;
		else
			return this.getRCGroupName(this.consistentNodeConfig
					.getFirstReconfigurator(name));
	}

	protected boolean isRCGroupName(String name) {
		for (NodeIDType rc : this.consistentNodeConfig.getReconfigurators())
			if (this.getRCGroupName(rc).equals(name))
				return true;
		return false;
	}

	/* Insert next nodeConfig version into DB. We have the necessary nodeID info
	 * from the NODE_CONFIG reconfiguration record, but we do need
	 * consistentNodeConfig for the corresponding InetSocketAddresses.
	 * 
	 * FIXME: This should probably be done atomically, not one record at a time. */
	protected boolean updateDBNodeConfig(int version) {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());
		boolean added = true;
		for (NodeIDType rc : ncRecord.getNewActives()) {
			assert (this.consistentNodeConfig.getNodeSocketAddress(rc) != null) : getMyID()
					+ " had no socket addres for " + rc;
			added = added
					&& this.addReconfigurator(rc,
							this.consistentNodeConfig.getNodeSocketAddress(rc),
							version);
		}
		return added;
	}

	protected boolean updateActiveDBNodeConfig(int version) {
		ReconfigurationRecord<NodeIDType> ancRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.AR_NODES
						.toString());
		boolean added = true;
		for (NodeIDType active : ancRecord.getNewActives()) {
			assert (this.consistentNodeConfig.getNodeSocketAddress(active) != null) : getMyID()
					+ " found no socket address for " + active + "; actives=" + this.consistentNodeConfig.getActiveReplicas();
			added = added
					&& this.addActiveReplica(active, this.consistentNodeConfig
							.getNodeSocketAddress(active), version);
		}
		return added;

	}

	protected Set<NodeIDType> setRCEpochs(Set<NodeIDType> addNodes,
			Set<NodeIDType> deleteNodes) {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());
		assert (!ncRecord.getActiveReplicas().equals(ncRecord.getNewActives())) : this
				+ " : " + ncRecord;
		Set<NodeIDType> affectedNodes = new HashSet<NodeIDType>();
		// affected by adds
		for (NodeIDType addNode : addNodes) {
			affectedNodes.add(addNode);
			for (NodeIDType oldNode : ncRecord.getActiveReplicas())
				if (this.isAffected(oldNode, addNode))
					affectedNodes.add(oldNode);
		}

		// affected by deletes
		for (NodeIDType deleteNode : deleteNodes)
			for (NodeIDType oldNode : ncRecord.getActiveReplicas())
				if (this.isAffected(oldNode, deleteNode))
					affectedNodes.add(oldNode);

		ncRecord.setRCEpochs(affectedNodes, addNodes, deleteNodes);
		this.setRCEpochs(ncRecord);
		return affectedNodes;
	}

	/* Determines if rcNode's group needs to be reconfigured because of the
	 * addition or deletion of addOrDelNode. We need this to correctly track the
	 * epoch numbers of all RC groups. */
	protected boolean isAffected(NodeIDType rcNode, NodeIDType addOrDelNode) {
		if (addOrDelNode == null)
			return false;
		boolean affected = false;
		ConsistentHashing<NodeIDType> oldRing = this.getOldConsistentHashRing();
		NodeIDType hashNode = oldRing.getReplicatedServersArray(
				this.getRCGroupName(addOrDelNode)).get(0);

		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
						.toString());
		for (NodeIDType oldNode : ncRecord.getActiveReplicas()) {
			if (oldRing.getReplicatedServers(this.getRCGroupName(oldNode))
					.contains(hashNode)) {
				affected = true;
			}
		}
		return affected;
	}

	protected ConsistentHashing<NodeIDType> getOldConsistentHashRing() {
		return new ConsistentHashing<NodeIDType>(this.getReconfigurationRecord(
				AbstractReconfiguratorDB.RecordNames.RC_NODES.toString())
				.getActiveReplicas());
	}

	protected ConsistentHashing<NodeIDType> getNewConsistentHashRing() {
		return new ConsistentHashing<NodeIDType>(this.getReconfigurationRecord(
				AbstractReconfiguratorDB.RecordNames.RC_NODES.toString())
				.getNewActives());
	}
}
