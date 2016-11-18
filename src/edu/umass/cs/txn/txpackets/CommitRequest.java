package edu.umass.cs.txn.txpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.txn.Transaction;

/**
 * @author arun
 *
 *         This request commits a transaction. At a transaction group member,
 *         the corresponding callback will trigger unlocks to participant
 *         groups.
 */
public class CommitRequest extends TXPacket {
	public CommitRequest(Transaction tx) {
		this(null, tx);
	}

	public CommitRequest(String initiator, Transaction tx) {
		super(TXPacket.PacketType.ABORT_REQUEST, initiator);
		// TODO Auto-generated constructor stub
	}

	public CommitRequest(JSONObject json) throws JSONException {
		super(json);
		// TODO Auto-generated constructor stub
	}
}
