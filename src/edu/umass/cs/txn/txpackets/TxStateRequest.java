package edu.umass.cs.txn.txpackets;

import org.json.JSONException;
import org.json.JSONObject;

public class TxStateRequest extends TXPacket {

	public static enum State {
		COMMITTED, 
		
		ABORTED, 
		
		EXECUTING,
	}

	private State state = State.EXECUTING;

	public TxStateRequest(String txGroupName) {
		super(TXPacket.PacketType.TX_STATE_REQUEST, null);
	}

	public TxStateRequest(JSONObject json) throws JSONException {
		super(json);
		// TODO Auto-generated constructor stub
	}

	public State getState() {
		return this.state;
	}
}
