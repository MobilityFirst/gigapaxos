package edu.umass.cs.txn.txpackets;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 * 
 *         This request aborts a transaction. At a transaction group member, the
 *         corresponding callback will trigger aborts to participant groups, and
 *         will also undo any constituent create name operations. At a
 *         participant group member, the callback will revert to the
 *         pre-transaction checkpoint.
 *
 */
public class AbortRequest extends TXPacket {
	public AbortRequest(String txGroupName) {
		this(null, txGroupName, txGroupName);
	}

	public AbortRequest(String participantGroup, String txGroupName) {
		this(null, participantGroup, txGroupName);
	}

	public AbortRequest(String initiator, String participantGroup,
			String txGroupName) {
		super(TXPacket.PacketType.ABORT_REQUEST, initiator);
	}

	public AbortRequest(JSONObject json) throws JSONException {
		super(json);
	}

	public boolean isCommitted() {
		throw new RuntimeException("Unimplemented");
	}

}
