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
package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckDropEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DropEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
/* This protocol task is initiated at a reconfigurator to await a majority of
 * acknowledgments from active replicas for StopEpoch messages. */
public class WaitAckDropEpoch<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	/* We use different restart times for service names and RC group names as RC
	 * group names may have much more variance in checkpoint transfer times.
	 * Setting these values too low or too high will not impact safety. However,
	 * too low values may result in inefficient, remote checkpoint transfers
	 * while too high values may result in a large number of pending tasks
	 * consuming memory. */
	private static final long RESTART_PERIOD_SERVICE_NAMES = 4 * WaitAckStopEpoch.RESTART_PERIOD;
	private static final long RESTART_PERIOD_RC_GROUP_NAMES = 16 * WaitAckStopEpoch.RESTART_PERIOD;
	private static final long RESTART_PERIOD_NC_CHANGE = WaitAckStopEpoch.RESTART_PERIOD;

	private static final int MAX_RESTARTS = 5;

	private final NodeIDType myID;
	private final DropEpochFinalState<NodeIDType> dropEpoch;
	private final StartEpoch<NodeIDType> startEpoch;
	private final Set<NodeIDType> prevGroup;
	private final RepliconfigurableReconfiguratorDB<NodeIDType> DB;
	private int numRestarts = 0;
	private final long creationTime = System.currentTimeMillis();
	private final Set<NodeIDType> ackers = new HashSet<NodeIDType>();

	private final String key;

	private static final Logger log = ReconfigurationConfig.getLogger();

	/**
	 * @param startEpoch
	 * @param prevGroup
	 * @param DB
	 *            The getAllActive() call is a hack to be able to finish deletes
	 *            quicker. This has safety violation risks upon name re-creation
	 *            and is meant only for instrumentation.
	 *            <p>
	 * 
	 */
	public WaitAckDropEpoch(StartEpoch<NodeIDType> startEpoch,
			Set<NodeIDType> prevGroup,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		/* For node config changes, we require all *new* epoch replicas to
		 * acknowledge "deleting" the previous epoch's final state. The final
		 * state deletion is unnecessary, but we special case node config
		 * changes so that only reconfigurators that have fully completed the
		 * transition to the next epoch will respond positively to "dropping"
		 * the previous epoch state. This allows the initiating node to know if
		 * all current reconfigurators are current. */
		super(startEpoch.isRCNodeConfigChange() ? startEpoch.getCommonMembers()
				: prevGroup != null ? prevGroup : startEpoch
						.getPrevEpochGroup());
		assert (prevGroup == null || !prevGroup.isEmpty());
		assert (!DB.isRCGroupName(startEpoch.getServiceName()));
		// use prevGroup if one specified
		this.prevGroup = prevGroup != null ? new HashSet<NodeIDType>(prevGroup)
				: new HashSet<NodeIDType>();
		this.DB = DB;
		// dropEpoch service name should be previous group name for merge
		this.dropEpoch = new DropEpochFinalState<NodeIDType>(DB.getMyID(),
				(startEpoch.isMerge() ? startEpoch.getPrevGroupName()
						: startEpoch.getServiceName()),
				startEpoch.isMerge() ? startEpoch.getPrevEpochNumber()
						: startEpoch.getEpochNumber() - 1, false);

		this.startEpoch = startEpoch;

		this.myID = DB.getMyID();
		this.key = this.refreshKey();
		this.setPeriod(DB.isRCGroupName(startEpoch.getServiceName()) ? RESTART_PERIOD_RC_GROUP_NAMES
				: startEpoch.isNodeConfigChange() ? RESTART_PERIOD_NC_CHANGE
						: RESTART_PERIOD_SERVICE_NAMES);
	}

	/**
	 * @param startEpoch
	 * @param DB
	 */
	public WaitAckDropEpoch(StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		this(startEpoch, startEpoch.getPrevEpochGroup(), DB);
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		this.DB.filterDeletedActives(this.prevGroup);
		// send DropEpoch to all new actives and await acks from all?
		log.log(Level.INFO,
				"{0} (re-)sending [{1} of {2}] {3} to {4}; (#ackers={5}; super_ackers={6})",
				new Object[] {
						this.refreshKey(),
						this.numRestarts,
						MAX_RESTARTS,
						this.dropEpoch.getSummary(),
						startEpoch.isRCNodeConfigChange() ? startEpoch
								.getCommonMembers() : this.prevGroup,
						this.ackers, this.waitfor.getResponded() });

		if ((// have something to drop in the first place
				this.startEpoch.hasPrevEpochGroup()
						&& !this.prevGroup.isEmpty()
						// count limit on restarts for good garbage collection
						&& this.numRestarts++ < MAX_RESTARTS
				// time limit on restarts to prevent ghost final state deletion
				&& System.currentTimeMillis() - this.creationTime < ReconfigurationConfig
						.getMaxFinalStateAge())
				/* Or is node config change coz these drop epoch tasks are meant
				 * to ensure that all reconfigurators have completed the node
				 * config change, so they must go on for ever in order to be
				 * able to respond bacj with success to the client. */
				|| startEpoch.isNodeConfigChange()) {
			return (new GenericMessagingTask<NodeIDType, DropEpochFinalState<NodeIDType>>(
					startEpoch.isRCNodeConfigChange() ? this.startEpoch
							.getCommonMembers().toArray()
							: this.prevGroup.toArray(), this.dropEpoch))
					.toArray();
		}
		// else
		log.log(Level.FINE, "{0} protocol task canceling itself",
				new Object[] { this.getKey() });
		ProtocolExecutor.cancel(this);
		return null;
	}

	/**
	 * We skip the first period before sending out drop epochs. It is good to
	 * delay it a bit as some replicas may still not be done acquiring the
	 * previous epoch final state and it is good for them to have the
	 * opportunity to do so from a previous epoch replica rather than a paxos
	 * peer replica in the new epoch. This delaying is not needed for safety as
	 * a majority of replicas must have already started the new epoch by the
	 * time a drop epoch task is spawned, so this (minority) replica can always
	 * get the current state through a checkpoint transfer from one of the
	 * majority peer replica in the new epoch in case all previous epoch final
	 * state has been garbage collected before it could start the new epoch.
	 */
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		if (!this.startEpoch.hasCurEpochGroup())
			return this.restart();
		else
			return null;
	}

	/**
	 * @return The refreshed key.
	 */
	public String refreshKey() {
		return Reconfigurator.getTaskKey(getClass(), dropEpoch,
				this.myID.toString());
	}

	/**
	 * Packet types handled.
	 */
	public static final ReconfigurationPacket.PacketType[] types = { ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE };

	@Override
	public Set<PacketType> getEventTypes() {
		return new HashSet<ReconfigurationPacket.PacketType>(
				Arrays.asList(types));
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean handleEvent(ProtocolEvent<PacketType, String> event) {
		assert (event.getType()
				.equals(ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE));
		log.log(Level.INFO, "{0} received {1}",
				new Object[] { this.refreshKey(),
						((AckDropEpochFinalState<String>) event).getSummary() });
		this.ackers.add(((AckDropEpochFinalState<NodeIDType>) event)
				.getSender());
		return true;
	}

	/**
	 * We should delete the record currently pending deletion.
	 */
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(startEpoch.isNodeConfigChange() ? Level.INFO : Level.FINE,
				"{0} received all ACKs for {1}{2}",
				new Object[] {
						this.refreshKey(),
						dropEpoch.getSummary(),
						(this.startEpoch.noCurEpochGroup()
								|| this.startEpoch.isMerge() ? "; initiating DELETE_COMPLETE"
								: "") });
		// not safe to complete delete before MAX_FINAL_STATE_AGE
		if (this.startEpoch.noCurEpochGroup()
				&& !ReconfigurationConfig.aggressiveDeletionsAllowed())
			return null;
		// delete RC record if no current group
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.myID, this.startEpoch,
				RCRecordRequest.RequestTypes.RECONFIGURATION_PREV_DROPPED);
		return (new GenericMessagingTask<NodeIDType, Object>(this.myID,
				rcRecReq)).toArray();
	}
}
