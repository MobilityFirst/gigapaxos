package edu.umass.cs.txn;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.interfaces.GigaPaxosClient;
import edu.umass.cs.txn.txpackets.AbortRequest;
import edu.umass.cs.txn.txpackets.LockRequest;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TxStateRequest;
import edu.umass.cs.txn.txpackets.UnlockRequest;

/**
 * @author arun
 * 
 *         Each active replica maintains a map of transaction state that are in
 *         progress at that replica.
 */
public class TxCallback implements ExecutedCallback {

	/**
	 * This state is maintained for each participant group for each transaction.
	 * The value of this state is not really used or updated because the only
	 * purpose of the map storing this state is to store the set of outstanding
	 * transactions, so the map is really a set of its keys with the values not
	 * being used for now.
	 */
	static class TxState {
		// overall transaction state
		TxStateRequest.State state = TxStateRequest.State.EXECUTING;
	}

	/**
	 * The String key is the key composed of the transaction group name and the
	 * participant group name as in the getKey method.
	 */
	private final ConcurrentMap<String, TxState> txMap = new ConcurrentHashMap<String, TxState>();

	private final GigaPaxosClient<Request> gpClient;

	private static String getKey(String txGroupName, String participant) {
		return txGroupName + ":" + participant;
	}

	protected TxCallback(GigaPaxosClient<Request> gpClient) {
		this.gpClient = gpClient;
	}

	/**
	 * This method is invoked at a participant group to check on the status of
	 * the transaction and complete the local abort or commit steps if
	 * necessary. This is a one-time check, i.e., IO or other failures will
	 * defer to future re-attempts.
	 * 
	 * @param tx
	 * @param participant
	 * @throws IOException
	 */
	public void txTryFinish(Transaction tx, String participant)
			throws IOException {
		TxStateRequest response = (TxStateRequest) this.gpClient
				.sendRequest(new TxStateRequest(tx.getTxGroupName()));
		assert (!response.isFailed());
		if (response.getState() == TxStateRequest.State.ABORTED)
			rollback(tx, participant);

		if (response.getState() == TxStateRequest.State.COMMITTED)
			unlock(tx, participant);
	}

	private void rollback(Transaction tx, String participantGroup)
			throws IOException {
		AbortRequest response = (AbortRequest) this.gpClient
				.sendRequest((new AbortRequest(participantGroup, tx
						.getTxGroupName())));
		if (!response.isFailed())
			cleanup(tx.getTxGroupName(), participantGroup);

	}

	private void unlock(Transaction tx, String participantGroup)
			throws IOException {
		UnlockRequest response = (UnlockRequest) this.gpClient
				.sendRequest((new UnlockRequest(participantGroup, tx
						.getTxGroupName())));
		if (!response.isFailed()) // all done
			cleanup(tx.getTxGroupName(), participantGroup);
	}

	/* Cleans up transaction state for each participant group. */
	private TxState cleanup(String txID, String participantGroup) {
		return this.txMap.remove(getKey(txID, participantGroup));
	}

	/**
	 * Default callback for transaction operations at an active replica.
	 */
	@Override
	public void executed(Request request, boolean handled) {
		assert (request instanceof TXPacket);

		TXPacket.PacketType type = ((TXPacket) request).getTXPacketType();
		switch (type) {
		case LOCK_REQUEST:
			// init tx state
			txMap.putIfAbsent(
					getKey(((LockRequest) request).getTXID(),
							request.getServiceName()), new TxState());
			break;
		case UNLOCK_REQUEST:
			// all done, cleanup may be redundant
			cleanup(getKey(((UnlockRequest) request).getTXID(),
					request.getServiceName()), request.getServiceName());
			break;
		default:
			// do nothing
		}
	}
}
