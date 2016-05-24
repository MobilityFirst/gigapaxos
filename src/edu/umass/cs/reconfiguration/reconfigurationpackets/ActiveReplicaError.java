package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.Stringifiable;

/**
 * @author arun
 * 
 *         The primary purpose of this class is just to report to an app client
 *         that no active replica was found at the node that received the
 *         request using the response code
 *         {@link edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket.ResponseCodes#ACTIVE_REPLICA_EXCEPTION}
 *         .
 * 
 *         This class is also (ab-)used to report to the app client that the
 *         name itself does not exist; if so, the corresponding respose code is
 *         {@link edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket.ResponseCodes#NONEXISTENT_NAME_ERROR}
 *         .
 *
 */
public class ActiveReplicaError extends ClientReconfigurationPacket {

	private static enum Keys {
		REQUEST_ID, ERROR_CODE
	};

	private final long requestID;

	private final ResponseCodes code;

	/**
	 * @param initiator
	 * @param name
	 * @param requestID
	 */
	public ActiveReplicaError(InetSocketAddress initiator, String name,
			long requestID) {
		super(initiator, ReconfigurationPacket.PacketType.ACTIVE_REPLICA_ERROR,
				name, 0);
		this.requestID = requestID;
		this.makeResponse();
		this.code = ResponseCodes.ACTIVE_REPLICA_EXCEPTION;
	}

	/**
	 * @param name
	 * @param requestID
	 * @param crp
	 */
	public ActiveReplicaError(String name, long requestID,
			ClientReconfigurationPacket crp) {
		super(name, crp);
		this.type = ReconfigurationPacket.PacketType.ACTIVE_REPLICA_ERROR;
		this.requestID = requestID;
		makeResponse();
		this.code = ResponseCodes.NONEXISTENT_NAME_ERROR;
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public ActiveReplicaError(JSONObject json) throws JSONException {
		super(json);
		this.makeResponse();
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
		this.code = ResponseCodes.valueOf(json.getString(Keys.ERROR_CODE
				.toString()));
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public ActiveReplicaError(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, unstringer);
		this.makeResponse();
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
		this.code = ResponseCodes.valueOf(json.getString(Keys.ERROR_CODE
				.toString()));
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		json.put(Keys.ERROR_CODE.toString(), this.code);
		return json;
	}

	/**
	 * @return Request ID.
	 */
	public long getRequestID() {
		return this.requestID;
	}

	@Override
	public String getSummary() {
		return this.code + ":" + super.getSummary() + ":" + ActiveReplicaError.this.requestID;
	}
}