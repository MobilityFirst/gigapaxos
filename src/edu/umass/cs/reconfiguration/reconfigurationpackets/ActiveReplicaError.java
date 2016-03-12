package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.Stringifiable;

/**
 * @author arun
 * 
 *         The purpose of this class is just to report to an app client that no
 *         active replica was found at the node that received the request. 
 *
 */
public class ActiveReplicaError extends ClientReconfigurationPacket {

	private static enum Keys {REQUEST_ID};
	
	private final long requestID;
	
	/**
	 * @param initiator
	 * @param name
	 * @param requestID 
	 */
	public ActiveReplicaError(InetSocketAddress initiator, String name, long requestID) {
		super(initiator, ReconfigurationPacket.PacketType.ACTIVE_REPLICA_ERROR,
				name, 0);
		this.requestID = requestID;
		this.makeResponse();
	}
	
	/**
	 * @param name
	 * @param requestID
	 * @param crp
	 */
	public ActiveReplicaError(String name, long requestID, ClientReconfigurationPacket crp) {
		super(name, crp);
		this.type = ReconfigurationPacket.PacketType.ACTIVE_REPLICA_ERROR;
		this.requestID = requestID;
		makeResponse();
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public ActiveReplicaError(JSONObject json) throws JSONException {
		super(json);
		this.makeResponse();
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
	}

	/**
	 * @param json
	 * @param unstringer 
	 * @throws JSONException
	 */
	public ActiveReplicaError(JSONObject json, Stringifiable<?> unstringer) throws JSONException {
		super(json, unstringer);
		this.makeResponse();
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
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
		return super.getSummary() + ":" + ActiveReplicaError.this.requestID;
	}
}