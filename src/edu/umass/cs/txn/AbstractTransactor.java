package edu.umass.cs.txn;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.txn.interfaces.TXInterface;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TxOpRequest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.GCConcurrentHashMap;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public abstract class AbstractTransactor<NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType> {
	private final AbstractReplicaCoordinator<NodeIDType> coordinator;

	protected AbstractTransactor(
			AbstractReplicaCoordinator<NodeIDType> coordinator) {
		super(coordinator);
		this.coordinator = coordinator;
	}

	private static final IntegerPacketType[] txTypes = {
			TXPacket.PacketType.ABORT_REQUEST,
			TXPacket.PacketType.COMMIT_REQUEST,
			TXPacket.PacketType.LOCK_REQUEST,
			TXPacket.PacketType.TX_STATE_REQUEST,
			TXPacket.PacketType.UNLOCK_REQUEST };

	private static final boolean ENABLE_TRANSACTIONS = Config
			.getGlobalBoolean(RC.ENABLE_TRANSACTIONS);

	/* FIXME: how is a transaction's timeout decided? Any limit imposes a limit
	 * on the type of operations that can be done within a transaction.
	 * Databases don't impose such a timeout as they allow transactional
	 * operations to take arbitrarily long. So should we. */
	private static final long DEFAULT_TX_TIMEOUT = Long.MAX_VALUE;
	private static final long MAX_QUEUED_REQUESTS = 8000;
	private GCConcurrentHashMap<Request, ExecutedCallback> callbacks = new GCConcurrentHashMap<Request, ExecutedCallback>(
			DEFAULT_TX_TIMEOUT);
	/* ********* Start of coordinator-related methods *************** */
	private Set<IntegerPacketType> cachedRequestTypes = null;

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		if (cachedRequestTypes != null)
			return cachedRequestTypes;
		Set<IntegerPacketType> types = new HashSet<IntegerPacketType>();
		// underlying coordinator request (includes app types)
		types.addAll(this.coordinator.getRequestTypes());
		// tx types
		if (ENABLE_TRANSACTIONS)
			types.addAll(new HashSet<IntegerPacketType>(Arrays.asList(txTypes)));
		return cachedRequestTypes = types;
	}

	// simply enqueue if room and call parent
	@Override
	public boolean coordinateRequest(Request request, ExecutedCallback callback)
			throws IOException, RequestParseException {
		if (!ENABLE_TRANSACTIONS || this.callbacks.size() < MAX_QUEUED_REQUESTS
				&& this.callbacks.putIfAbsent(request, callback) == null)
			return this.coordinator.coordinateRequest(request, callback);
		// else
		ReconfigurationConfig.getLogger().log(Level.WARNING,
				"{0} dropping request {1} because queue size limit reached",
				new Object[] { this, request.getSummary() });
		return false;
	}

	@Override
	public boolean execute(Request request, boolean noReplyToClient) {
		if (!ENABLE_TRANSACTIONS || !isLocked(request.getServiceName()) || 
				(request instanceof TxOpRequest && this.isOngoing(((TxOpRequest)request).getTxID())))
			return this.coordinator.execute(request, noReplyToClient,
					this.callbacks.remove(request.getServiceName()));
		enqueue(request, noReplyToClient);
		/* Need to return true here no matter what, otherwise paxos will be
		 * stuck. */
		return true;
	}
	/**
	 * @param txID
	 * @return True if txID is an ongoing transaction.
	 */
	private boolean isOngoing(String txID) {
		// TODO Auto-generated method stub
		return false;
	}

	private TXInterface getTransaction(String serviceName) {
		// TODO Auto-generated method stub
		return null;
	}

	protected abstract void enqueue(Request request, boolean noReplyToClient);

	protected abstract boolean isLocked(String serviceName);

	@Override
	public boolean execute(Request request) {
		return this.execute(request, false);
	}

	@Override
	public boolean createReplicaGroup(String serviceName, int epoch,
			String state, Set<NodeIDType> nodes) {
		return this.coordinator.createReplicaGroup(serviceName, epoch, state,
				nodes);
	}

	@Override
	public boolean deleteReplicaGroup(String serviceName, int epoch) {
		return this.coordinator.deleteReplicaGroup(serviceName, epoch);
	}

	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		return this.coordinator.getReplicaGroup(serviceName);
	}

	/* ********* End of coordinator methods *************** */

	/* ********* Start of Repliconfigurable methods *************** */

	@Override
	public ReconfigurableRequest getStopRequest(String name, int epoch) {
		return this.coordinator.getStopRequest(name, epoch);
	}

	@Override
	public String getFinalState(String name, int epoch) {
		return this.coordinator.getFinalState(name, epoch);
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		this.coordinator.putInitialState(name, epoch, state);
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		return this.coordinator.deleteFinalState(name, epoch);
	}

	@Override
	public Integer getEpoch(String name) {
		return this.coordinator.getEpoch(name);
	}

	@Override
	public String checkpoint(String name) {
		return this.coordinator.checkpoint(name);
	}

	@Override
	public boolean restore(String name, String state) {
		TXInterface request = this.decodeTX(state);
		if (request == null)
			return this.coordinator.restore(name, state);
		else
			return this.processTX((TXInterface) request);
	}

	private boolean processTX(TXInterface request) {
		Set<NodeIDType> group = this.getReplicaGroup(request.getTXID());
		SortedSet<String> sorted = new TreeSet<String>();
		for (NodeIDType node : group)
			sorted.add(node.toString());
		if (this.getMyID().toString().equals(sorted.iterator().next())) {
			// if first in list, actually do stuff
		}
		// else secondary
		return spawnWaitPrimaryTask(request);
	}

	private boolean spawnWaitPrimaryTask(TXInterface request) {
		// TODO Auto-generated method stub
		return false;
	}

	private TXInterface decodeTX(String state) {
		// TODO Auto-generated method stub
		return null;
	}

	/* ********* End of Repliconfigurable methods *************** */

}
