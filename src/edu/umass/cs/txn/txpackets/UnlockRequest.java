package edu.umass.cs.txn.txpackets;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 *
 */
public class UnlockRequest extends TXPacket {

	private static enum Keys {
		UNLOCKID, TXID
	};

	private final String lockID;
	private final String txID;

	public UnlockRequest(String lockID, String txID) {
		super(TXPacket.PacketType.UNLOCK_REQUEST, null);
		this.lockID = lockID;
		this.txID = txID;
	}

	public UnlockRequest(JSONObject json) throws JSONException {
		super(json);
		this.lockID = json.getString(Keys.UNLOCKID.toString());
		this.txID = json.getString(Keys.TXID.toString());
	}

	public JSONObject toJSONObjectImpl() {
		throw new RuntimeException("Unimplemented");
	}
	
	public String getTXID() {
		return this.txID;
	}
}
