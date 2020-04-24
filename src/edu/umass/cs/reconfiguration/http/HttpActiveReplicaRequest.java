package edu.umass.cs.reconfiguration.http;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;

/**
 * HttpRequest is the type of request used by {@link HttpActiveReplica}.
 * All the applications that need to use {@link HttpActiveReplica} must
 * follow the spec of this class (HttpRequest).
 *
 * HttpRequest requires the content of a HTTP POST request (to
 * {@link HttpActiveReplica} ) in JSON format.
 * It must contains the following keys: NAME, VALUE, and SYNC
 * 
 * The key PACKET_TYPE of {@link JSONPacket}.
 *
 * @author gaozy
 *
 */

public class HttpActiveReplicaRequest extends JSONPacket implements 
	ReplicableRequest, ReconfigurableRequest, ClientRequest {
	
	/**
	 * Keys of HttpRequest
	 *
	 */
	public static enum Keys{
		/**
		 * The service name that must present in every request.
		 */
		NAME,
		
		/**
		 * Request value
		 */
		QVAL,
		
		/**
		 * Request id
		 */
		QID,
		
		/**
		 * Response value
		 */
		RVAL,
		
		/**
		 * Coordinated or not
		 */
		COORD,
		
		/**
		 * 
		 */
		EPOCH,
		
		/**
		 * 
		 */
		STOP
	};
	
	private final String name;
	private final long id;
	private final String value;
	private final boolean coord;
	private final boolean stop;
	private int epoch;
	
	/**
	 * Response from underlying app
	 */
	public String response;
	
	/**
	 * HttpRequest is request type agnostic, the underlying application needs 
	 * to define their own types if necessary.
	 * 
	 * @param t
	 * @param name
	 * @param id 
	 * @param value 
	 * @param coord 
	 * @param stop 
	 * @param epoch 
	 */
	public HttpActiveReplicaRequest(IntegerPacketType t, String name, long id, String value, boolean coord, boolean stop, int epoch) {
		super(t);
		this.name = name;
		this.id = id;
		this.value = value;
		this.coord = coord;
		this.stop = stop;
		this.epoch = epoch;
	}
	
	
	/**
	 * By default, sync is false, i.e., HTTP response can be sent back before app executes the request
	 * 
	 * @param t
	 * @param name
	 * @param id
	 * @param value
	 * @param stop 
	 * @param epoch 
	 */
	public HttpActiveReplicaRequest(IntegerPacketType t, String name, long id, String value, boolean stop, int epoch){
		this(t, name, id, value, true, stop, epoch);
	}
	
	
	/**
	 * If no ID presents, we may generate an ID for the request.
	 * 
	 * @param t
	 * @param name
	 * @param value
	 * @param epoch 
	 */
	public HttpActiveReplicaRequest(IntegerPacketType t, String name, String value, int epoch){
		this(t, name, (int) (Math.random() * Integer.MAX_VALUE), value, true, epoch);
	}
	
	/**
	 * @param t
	 * @param name
	 * @param value
	 */
	public HttpActiveReplicaRequest(IntegerPacketType t, String name, String value) {
		this(t, name, (int) (Math.random() * Integer.MAX_VALUE), value, true, 0);
	}
	
	/**
	 * @param value
	 * @param req
	 */
	public HttpActiveReplicaRequest(String value, HttpActiveReplicaRequest req) {
		this(HttpActiveReplicaPacketType.getPacketType(req.type), req.name, req.id, value, req.coord, req.epoch);
	}
	
	/**
	 * @param request
	 * @throws JSONException
	 */
	public HttpActiveReplicaRequest(ReplicableClientRequest request) throws JSONException {
		this(request.toJSONObject());
	}
	
	/**
	 * @param json
	 * @throws JSONException
	 */
	public HttpActiveReplicaRequest(JSONObject json) throws JSONException{
		// get request type
		super(json);
		
		Object obj = getValueFromJSONCaseInsensitive(Keys.NAME.toString(), json);		
		this.name = obj==null? null : (String) obj;
		
		obj = getValueFromJSONCaseInsensitive(Keys.QVAL.toString(), json);
		this.value = obj==null? null : (String) obj;
		
		obj = getValueFromJSONCaseInsensitive(Keys.QID.toString(), json);
		this.id = obj==null? (int) (Math.random() * Integer.MAX_VALUE): Long.parseLong(String.valueOf(obj));
		
		obj = getValueFromJSONCaseInsensitive(Keys.COORD.toString(), json);
		this.coord = obj==null? false: (Boolean) obj;
		
		obj = getValueFromJSONCaseInsensitive(Keys.RVAL.toString(), json);
		this.response = obj==null? null: (String) obj;
		
		obj = getValueFromJSONCaseInsensitive(Keys.STOP.toString(), json);
		this.stop = obj==null? false: (Boolean) obj;
		
		obj = getValueFromJSONCaseInsensitive(Keys.EPOCH.toString(), json);
		this.epoch = obj==null? 0: (Integer) obj;
	}
	
	private Object getValueFromJSONCaseInsensitive(String key, JSONObject json) throws JSONException {
		if (json.has(key)) {
			return json.get(key);
		} else if (json.has(key.toLowerCase())) {
			return json.get(key.toLowerCase());
		}
		return null;
	}
	
	@Override
	protected JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.NAME.toString(), this.name);
		json.put(Keys.QVAL.toString(), this.value);
		json.put(Keys.COORD.toString(), this.coord);
		json.put(Keys.QID.toString(), this.id);
		json.put("STOP", this.stop);
		json.put("EPOCH", this.epoch);
		json.putOpt(Keys.RVAL.toString(), this.response);
		return json;
	}
	
	@Override
	public IntegerPacketType getRequestType() {
		return HttpActiveReplicaPacketType.getPacketType(this.type);
	}

	@Override
	public String getServiceName() {
		return this.name;
	}

	@Override
	public long getRequestID() {
		return this.id;
	}
	
	/**
	 * @return value
	 */
	public String getValue() {
		return this.value;
	}
	
	@Override
	public boolean needsCoordination() {
		// every HttpRequest needs coordination
		return this.coord;
	}

	/**
	 * Always not a stop request
	 */
	@Override
	public boolean isStop() {
		return this.stop;
	}

	/**
	 * @param response
	 */
	public void setResponse(String response) {
		this.response = response;
	}

	
	/**
	 * @param epoch
	 * @return HttpActiveReplicaRequest
	 */
	public HttpActiveReplicaRequest setEpoch(int epoch) {
		this.epoch = epoch;
		return this;
	}
	
	@Override
	public ClientRequest getResponse() {
		if (this.response != null)
			return new HttpActiveReplicaRequest(this.response, this);
		return null;
	}
	
	@Override
	public int getEpochNumber() {
		return this.epoch;
	}
	
	/**
	 * Test request serialization.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
