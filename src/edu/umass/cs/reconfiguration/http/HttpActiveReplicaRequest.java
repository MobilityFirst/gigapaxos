package edu.umass.cs.reconfiguration.http;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

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
	};
	
	private final String name;
	private final long id;
	private final String value;
	private final boolean coord;
	
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
	 */
	public HttpActiveReplicaRequest(IntegerPacketType t, String name, long id, String value, boolean coord) {
		super(t);
		this.name = name;
		this.id = id;
		this.value = value;
		this.coord = coord;
	}
	
	
	/**
	 * By default, sync is false, i.e., HTTP response can be sent back before app executes the request
	 * 
	 * @param t
	 * @param name
	 * @param id
	 * @param value
	 */
	public HttpActiveReplicaRequest(IntegerPacketType t, String name, long id, String value){
		this(t, name, id, value, true);
	}
	
	/**
	 * If no ID presents, we may generate an ID for the request.
	 * 
	 * @param t
	 * @param name
	 * @param value
	 */
	public HttpActiveReplicaRequest(IntegerPacketType t, String name, String value){
		this(t, name, (int) (Math.random() * Integer.MAX_VALUE), value, true);
	}
		
	/**
	 * @param value
	 * @param req
	 */
	public HttpActiveReplicaRequest(String value, HttpActiveReplicaRequest req) {
		this(HttpActiveReplicaPacketType.getPacketType(req.type), req.name, req.id, value, req.coord);
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
		// json.put("STOP", false);
		// json.put("EPOCH", 0);
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
	
	@Override
	public int getEpochNumber() {
		return 0;
	}

	/**
	 * Always not a stop request
	 */
	@Override
	public boolean isStop() {
		return false;
	}

	/**
	 * @param response
	 */
	public void setResponse(String response) {
		this.response = response;
	}

	@Override
	public ClientRequest getResponse() {
		if (this.response != null)
			return new HttpActiveReplicaRequest(this.response, this);
		return null;
	}
	
	
	/**
	 * Test request serialization.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
