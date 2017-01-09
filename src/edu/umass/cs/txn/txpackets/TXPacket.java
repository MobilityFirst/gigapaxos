package edu.umass.cs.txn.txpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.txn.exceptions.ResponseCode;
import edu.umass.cs.utils.IntegerPacketTypeMap;

/**
 * @author arun
 *
 *         All transaction processing related packets inherit from this class.
 */
public abstract class TXPacket extends JSONPacket implements ReplicableRequest,
		ClientRequest {

	/**
	 * Transaction packet types.
	 */
	public enum PacketType implements IntegerPacketType {

		/**
		 * 
		 */
		LOCK_REQUEST(251),

		/**
		 * 
		 */
		UNLOCK_REQUEST(252),

		/**
		 * 
		 */
		ABORT_REQUEST(253),

		/**
		 * 
		 */
		COMMIT_REQUEST(254),

		/**
		 * 
		 */
		TX_STATE_REQUEST(255), 
		
		/**
		 * 
		 */
		TX_OP_REQUEST (256),
		;

		private final int number;

		PacketType(int t) {
			this.number = t;
		}

		public int getInt() {
			return number;
		}

		/**
		 * 
		 */
		public static final IntegerPacketTypeMap<PacketType> intToType = new IntegerPacketTypeMap<PacketType>(
				PacketType.values());
	}

	/* The tuple <txid, initiator> is used to detect conflicting txids chosen by
	 * different initiating nodes. */
	protected final String txid;
	private boolean failed;
	private ResponseCode code = null;

	private static enum Keys {
		TXID, LOCKID, INITIATOR
	}

	/**
	 * @param t
	 * @param txid 
	 */
	public TXPacket(IntegerPacketType t, String txid) {
		super(t);
		this.txid = txid;
	}


	/**
	 * @param json
	 * @throws JSONException
	 */
	public TXPacket(JSONObject json) throws JSONException {
		super(json);
		this.txid = json.getString(Keys.TXID.toString());
	}

	@Override
	public IntegerPacketType getRequestType() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @return {@link TXPacket.PacketType}
	 */
	public TXPacket.PacketType getTXPacketType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getServiceName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getRequestID() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ClientRequest getResponse() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean needsCoordination() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected JSONObject toJSONObjectImpl() throws JSONException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @return True if this request failed; false otherwise.
	 */
	public boolean isFailed() {
		return this.failed;
	}

	protected TXPacket setFailed() {
		this.failed = true;
		return this;
	}

	/**
	 * @return Response code.
	 */
	public ResponseCode getResponseCode() {
		return this.code;
	}

	/**
	 * @param code
	 */
	public void setResponseCode(ResponseCode code) {
		this.code = code;
	}

	/**
	 * @return The ID of the transaction to which this packet corresponds. The
	 *         initiator is set only after a transaction request has been
	 *         received by the entry server, so until then, the ID only has the
	 *         transaction number.
	 */
	public String getTXID() {
		return this.txid;
	}
}
