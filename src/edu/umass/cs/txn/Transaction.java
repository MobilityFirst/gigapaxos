package edu.umass.cs.txn;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.txn.interfaces.TxOp;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TxOpRequest;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *         A transaction is an indivisible sequence of operations that satisfy
 *         ACID (atomicity, consistency, isolation, and durability) properties.
 * 
 *         <p>
 * 
 *         The list of individual {@link TxOp} operations must either be a
 *         {@link ClientRequest} or {@link ClientReconfigurationPacket}. If any
 *         of the TxOp operations is a {@link DeleteServiceName} request, the
 *         deletion(s) need to be implemented as an in-memory pre-delete
 *         operation at both reconfigurators and active replicas so that they
 *         can be reverted if necessary.
 *
 */
public class Transaction {

	protected static enum Keys {
		TXOPS, TXID, DELETES,
	}

	// the sequence of operations in this transaction
	private ArrayList<TxOp> txops;

	// a transaction number chosen to be unique at each client
	private long txn;

	// the server issuing the transaction
	private InetSocketAddress entryServer;

	/**
	 * @param entryServer
	 * @param ops
	 */
	public Transaction(InetSocketAddress entryServer, ArrayList<Request> ops) {
		this(entryServer, ops.toArray(new Request[0]));
	}

	/**
	 * @param entryServer
	 * @param requests
	 */
	public Transaction(InetSocketAddress entryServer, Request... requests) {
		this.txn = getNewTxid(entryServer);
		this.txops = new ArrayList<TxOp>();
		for (Request request : requests)
			// if (!(request instanceof DeleteServiceName))
			this.txops
					.add(new TxOpRequest(Util.toString(entryServer), request));
		/* Name deletions should be issued only after the transaction has been
		 * committed. */
	}

	/**
	 * @return The set of lock identifiers needed for this transaction in
	 *         lexicographic order. The lock identifiers are in general a subset
	 *         of participant groups in the transaction as name creation and
	 *         deletion requests are excluded from this list.
	 */
	public TreeSet<String> getLockList() {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * @return The sequence of operations constituting this transaction.
	 *         Modifying this sequence will violate safety.
	 */
	public ArrayList<TxOp> getTxOps() {
		return this.txops;
	}

	/**
	 * @return An ID for this transaction created by concatenating the issuer's
	 *         address and the long transaction number; for safety, this ID must
	 *         be unique across all transactions in the system, so the same
	 *         issuer must not issue different transactions with the same
	 *         transaction number.
	 */
	public String getTXID() {
		return this.entryServer.getAddress().getHostAddress() + ":"
				+ this.entryServer.getPort() + ":" + this.txn;
	}

	/**
	 * The transaction group name must be globally unique, otherwise the
	 * transaction will fail in the very first tx_group creation step.
	 * 
	 * @return The name of this transaction, which also acts as the name of the
	 *         replica group conducting the transaction. The name is composed of
	 *         the sender ID and a long transaction number that is chosen
	 *         uniquely at each sender.
	 */
	public String getTxGroupName() {
		return this.getTXID();
	}

	protected String getTxInitState() {
		throw new RuntimeException("Unimplemented");
	}

	protected InetSocketAddress getEntryServer() {
		return this.entryServer;
	}

	private static final long DEFAULT_TIMEOUT = 3600 * 1000;
	private static GCConcurrentHashMap<Long, String> txids = new GCConcurrentHashMap<Long, String>(
			DEFAULT_TIMEOUT);

	private synchronized static long getNewTxid(InetSocketAddress initiator) {
		return getNewTxid(initiator.getAddress().getHostAddress() + ":"
				+ initiator.getPort());
	}

	private synchronized static long getNewTxid(String initiator) {
		long txid = (long) (Math.random() * Long.MAX_VALUE);
		while (txids.contains(txid))
			txid = (long) (Math.random() * Long.MAX_VALUE);
		txids.put(txid, initiator);
		return txid;
	}

	protected synchronized static String releaseTxid(long txid) {
		return txids.remove(txid);
	}
}
