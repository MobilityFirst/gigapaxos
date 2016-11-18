package edu.umass.cs.txn.txpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.txn.Transaction;

/**
 * @author arun
 *
 */
public class LockRequest extends TXPacket {

	private static enum Keys {
		LOCKID, TXID
	};

	private final String lockID;

	/**
	 * @param lockID
	 */
	public LockRequest(String lockID, Transaction tx) {
		super(TXPacket.PacketType.LOCK_REQUEST, tx.getTXID());
		this.lockID = lockID;
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public LockRequest(JSONObject json) throws JSONException {
		super(json);
		this.lockID = json.getString(Keys.LOCKID.toString());
	}

	public JSONObject toJSONObjectImpl() {
		throw new RuntimeException("Unimplemented");
	}
}
