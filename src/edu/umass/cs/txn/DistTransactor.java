package edu.umass.cs.txn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.Application;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.GigaPaxosClient;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.txn.exceptions.ResponseCode;
import edu.umass.cs.txn.exceptions.TXException;
import edu.umass.cs.txn.interfaces.TXLocker;
import edu.umass.cs.txn.interfaces.TxOp;
import edu.umass.cs.txn.txpackets.AbortRequest;
import edu.umass.cs.txn.txpackets.CommitRequest;
import edu.umass.cs.txn.txpackets.LockRequest;
import edu.umass.cs.txn.txpackets.UnlockRequest;
import edu.umass.cs.utils.Config;

/**
 * @author arun
 * 
 *         This class is used at a node pushing forward the transactions steps,
 *         which is normally the primary designate in the transaction group. If
 *         the primary crashes, a secondary might use this class for the same
 *         purpose.
 * @param <NodeIDType>
 */
public class DistTransactor<NodeIDType> extends AbstractTransactor<NodeIDType> implements TXLocker {

	/**
	 * A distributed transaction processor needs a client to submit transaction
	 * operations as well as to acquire and release locks.
	 */
	private final GigaPaxosClient<Request> gpClient;
	
	private final TXLocker txLocker;

	/**
	 * @param coordinator
	 * @throws IOException
	 */
	public DistTransactor(AbstractReplicaCoordinator<NodeIDType> coordinator)
			throws IOException {
		super(coordinator);
		this.gpClient = TXUtils.getGPClient(coordinator);
		this.txLocker = new TXLockerMap();
	}
	
	private Application getApp() {
//		if(this.app instanceof TrivialRepliconfigurable ? ((TrivialRepliconfigurable)this.app).)
		return null;
	}

	/**
	 * A blocking call that returns upon successfully locking {@code lockID} or
	 * throws a {@link TXException}. Locking a group involves synchronously 
	 * checkpointing its state and maintaining in memory its locked status.
	 * 
	 * @param lockID
	 * @throws TXException
	 */
	public void lock(String lockID) throws TXException {
		this.txLocker.lock(lockID);
	}

	/**
	 * Acquires the locks in the order specified by {@code lockIDs}.
	 * 
	 * @param lockIDs
	 * @throws TXException
	 */
	public void lock(String[] lockIDs) throws TXException {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * A blocking call that returns upon successfully release {@code lockID} or
	 * throws a {@link TXException} .
	 * 
	 * @param lockID
	 * @throws TXException
	 */
	public void unlock(String lockID) throws TXException {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * Releases the locks in the order specified by {@code lockIDs}.
	 * 
	 * @param lockIDs
	 * @throws TXException
	 */
	public void unlock(String[] lockIDs) throws TXException {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * This method is the top-level method initiating a transaction and consists
	 * of the following sequence of steps: (1) create transaction group; (2)
	 * acquire participant group locks; (3) execute transaction operations in
	 * order; (4) issue commit to transaction group; (5) release participant
	 * group locks; (6) delete transaction group. Each step in this sequence is
	 * blocking and all steps must succeed for this method to complete
	 * successfully, otherwise it will throw a {@link TXException}. The
	 * transaction group acts as the "monitor" group that makes it easy to
	 * reason about the safety property that all participant groups agree on
	 * whether a transaction is committed or aborted and that either decision is
	 * final.
	 * 
	 * Message complexity: Below, P1 refers to the message complexity of a paxos
	 * operation in the transaction group, P2 to that of a paxos operation in
	 * the largest participant group, and P3 to that in a reconfigurator group;
	 * N1 is the number of transaction steps involving participant groups, N2 is
	 * the number of name create operations, and N3 the number of name delete
	 * operations; M is the distinct number of participant groups (or names)
	 * involved;
	 * 
	 * (1) 2*P3 + 2/3*P1
	 * 
	 * (2) N1*P2
	 * 
	 * (3) (N1+N3)*P2 + 2*(N2+N3)*P3 + 2/3*N2*P2
	 * 
	 * (4) P1
	 * 
	 * (5) (N1 + N2 + N3)*P2
	 * 
	 * (6) P1 + 2*P3
	 * 
	 * In comparison, simply executing the transaction's steps sequentially
	 * without ensuring any transactional semantics has a message complexity of
	 * (N1+N3)*P2 + 2*(N2+N3)*P3 + 2/3*N2*P2, i.e., just step 3 above. Thus,
	 * transactions roughly increase the message complexity by a factor of 3
	 * (for (lock, execute, unlock) compared to just execute for each operation)
	 * plus a fixed number, a total of ~7, of additional paxos operations across
	 * the transaction or various reconfigurator groups involved.
	 * 
	 * Optimizations: The overhead of the transaction group can be reduced to
	 * just a single paxos operation with a corresponding liveness cost if we
	 * reuse transaction groups across different transactions. (1) Choosing the
	 * transaction group as a fixed set of active replicas makes transactional
	 * liveness limited by majority availability in that set; (2) Choosing the
	 * transaction group as the set of all active replicas can incur a
	 * prohibitively high overhead for even a single paxos operation as the
	 * total number of active replicas may be much higher than the size of
	 * typical participant groups.
	 * 
	 * 
	 * @param tx
	 * @throws TXException
	 */
	public void transact(Transaction tx) throws TXException {
		boolean locked = false, executed = false, committed = false;
		try {
			if (this.createTxGroup(tx) && (locked = getLocks(tx))
					&& (executed = executeTxOps(tx))
					&& (committed = commit(tx)) && releaseLocks(tx))
				// all is good
				return;

		} catch (IOException e) {
			throw new TXException(ResponseCode.IOEXCEPTION, e);
		} finally {
			// abort
			if (!committed)
				abort(tx, locked, executed);
		}
	}

	private void abort(Transaction tx, boolean locked, boolean executed)
			throws TXException {
		assert (!executed || locked);
		Request response = null;
		if (locked && !executed) {
			do {
				try {
					// try to get an abort committed in the transaction group
					response = this.gpClient.sendRequest(new AbortRequest(tx
							.getTxGroupName()));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} while (((AbortRequest) response).isFailed()
					&& !((AbortRequest) response).isCommitted());
		}

		if (((AbortRequest) response).isCommitted())
			this.releaseLocks(tx);

		// else rollback participants
		this.rollbackParticipantGroups(tx);
	}

	private void rollbackParticipantGroups(Transaction tx) {
		ArrayList<Request> rollbacks = new ArrayList<Request>();
		// abort participant groups until successful
		for (String participantGroup : tx.getLockList())
			rollbacks.add(new AbortRequest(participantGroup, tx
					.getTxGroupName()));
		/* best effort here is okay because one or more participant groups may
		 * be unavailable, so the onus is on them to complete the rollback when
		 * they are available again. */
		TXUtils.tryFinishAsyncTasks(this.gpClient, rollbacks);
	}

	private boolean commit(Transaction tx) throws TXException, IOException {
		if (((CommitRequest) this.gpClient.sendRequest(new CommitRequest(tx)))
				.isFailed())
			throw new TXException(ResponseCode.COMMIT_FAILURE,
					"Failed to commie transaction " + tx);
		;
		return true;
	}

	private boolean getLocks(Transaction tx) throws TXException, IOException {
		for (String lockID : tx.getLockList())
			if (((LockRequest) gpClient.sendRequest(new LockRequest(lockID,
			/* The client ID is used as the ID of the initiator. */
			tx))).isFailed())
				throw new TXException(ResponseCode.LOCK_FAILURE,
						"Failed to acquire lock " + lockID);
		;
		return true;
	}

	private boolean executeTxOps(Transaction tx) throws TXException,
			IOException {
		Request response;
		for (TxOp op : tx.getTxOps())
			if (!op.handleResponse(response = gpClient.sendRequest(op)))
				throw new TXException(ResponseCode.TXOP_FAILURE,
						"Failed to execute transaction operation "
								+ op.getSummary() + " : "
								+ response.getSummary());

		return true;
	}

	private boolean releaseLocks(Transaction tx) throws TXException {
		ArrayList<Request> unlocks = new ArrayList<Request>();
		for (String lockID : tx.getLockList())
			unlocks.add(new UnlockRequest(lockID,
			/* The client ID is used as the ID of the initiator. */
			gpClient.toString()));
		Request[] responses = TXUtils.tryFinishAsyncTasks(gpClient, unlocks);
		for (Request response : responses)
			if (((UnlockRequest) response).isFailed())
				return false;
		return true;
	}

	private boolean fixedTXGroupCreated = false;
	private static final boolean FIXED_TX_GROUP = true;

	/**
	 * This is the first step in a transaction. There is no point proceeding
	 * with a transaction if the transaction group does not exist. A transaction
	 * can be requested by an end-client but has to be initiated by an active
	 * replica, i.e., this createTxGroup has to be made from an active replica.
	 * There are two reasons for this: (1) there is a safety issue with allowing
	 * end-clients to lock consensus groups without any checks; (2) an active
	 * replica can be part of the transaction group saving the need to issue and
	 * maintain triggers in order to respond back to the end-client.
	 * 
	 * @return True if the group got created or if the group already exists;
	 *         false otherwise.
	 * @throws IOException
	 */
	private boolean createTxGroup(Transaction tx) throws IOException {
		if (FIXED_TX_GROUP && this.fixedTXGroupCreated)
			return true;
		// else
		CreateServiceName response = (CreateServiceName) (this.gpClient
				.sendRequest(new CreateServiceName(FIXED_TX_GROUP ? Config
						.getGlobalString(RC.TX_GROUP_NAME) : tx
						.getTxGroupName(), tx.getTxInitState(), getTxGroup(tx
						.getTXID()))));
		return response != null
				&& (!response.isFailed() || response.getResponseCode() == ClientReconfigurationPacket.ResponseCodes.DUPLICATE_ERROR)
				&& (this.fixedTXGroupCreated = true);
	}

	private static final int MAX_TX_GROUP_SIZE = 11;

	/* The default policy is to use a deterministic set of active replicas for
	 * each transaction of size MAX_TX_GROUP_SIZE of the total number of active
	 * replica, whichever is lower. */
	protected Set<InetSocketAddress> getTxGroup(String txid) throws IOException {
		InetSocketAddress[] addresses = this.getAllActiveReplicas().toArray(
				new InetSocketAddress[0]);
		Set<InetSocketAddress> group = new HashSet<InetSocketAddress>();
		/* Picking start index randomly introduces some load balancing in the
		 * transaction group when the total number of active replicas is much
		 * higher than MAX_TX_GROUP_SIZE */
		int startIndex = txid.hashCode() % addresses.length;
		for (int i = startIndex; group.size() < MAX_TX_GROUP_SIZE; i = (i + 1)
				% addresses.length)
			group.add(addresses[i]);
		return group;
	}

	/**
	 * There isn't an easy way to get the correct list of all active replicas at
	 * an active replica without consulting reconfigurators. Reading it from the
	 * config file will in general be incorrect if active replicas are added or
	 * deleted over time.
	 * 
	 * @return The set of active replica socket addresses.
	 * @throws IOException
	 */
	private Set<InetSocketAddress> getAllActiveReplicas() throws IOException {
		return ((RequestActiveReplicas) this.gpClient
				.sendRequest(new RequestActiveReplicas(Config
						.getGlobalString(RC.BROADCAST_NAME)))).getActives();
	}

	protected boolean isLocked(String name) {
		throw new RuntimeException("Unimplemented");
	}

	protected void enqueue(Request request, boolean noReplyToClient) {
		throw new RuntimeException("Unimplemented");
	}
}
