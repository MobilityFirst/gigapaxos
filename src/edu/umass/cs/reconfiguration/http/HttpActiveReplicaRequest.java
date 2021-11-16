package edu.umass.cs.reconfiguration.http;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.utils.Util;

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
	ReplicableRequest, ReconfigurableRequest, ClientRequest, Byteable {
	
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
	
	protected static String CHARSET = "ISO-8859-1";
	
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
	 * We need this estimate to use it in {@link edu.umass.cs.gigapaxos.RequestBatcher#dequeueImpl()}.
	 * The value needs to be an upper bound on the sum total of all of the gunk
	 * in PValuePacket other than the requestValue itself, i.e., the size of a
	 * no-op decision.
	 */
	public static final int SIZE_ESTIMATE = estimateSize();

	private static int estimateSize() {
		// type (int) | name (String) | id (long) | value (String) | coord (bool) | stop (bool) | epoch (int)
		
		int length  = Integer.BYTES * 2 // type + epoch
				+ Long.BYTES // id
				+ 2 // coord + stop
				; 
		// 100% extra for other miscellaneous additions
		return (int) (length * 2);		
	}
	
	/**
	 * 
	 */
	private int lengthEstimate() {
		int len = this.value.length() + this.name.length() 
		+ (this.response == null? 0 : this.response.length()) + SIZE_ESTIMATE;
		return len;
	}
	
	// type (int) | name (String) | id (long) | value (String) | coord (bool) | stop (bool) | epoch (int) | response (String)
	
	@Override
	public byte[] toBytes() {
		int exactLength = 0;
		byte[] array = new byte[this.lengthEstimate()];
		ByteBuffer bbuf = ByteBuffer.wrap(array);
		assert (bbuf.position() == 0);
		
		try{
			bbuf.putInt(this.type);
			exactLength += Integer.BYTES;
			
			assert(this.name != null);
			
			byte[] nameBytes = this.name.getBytes(CHARSET);
			bbuf.putInt(nameBytes.length);
			bbuf.put(nameBytes);
			exactLength += (Integer.BYTES+nameBytes.length);
			
			bbuf.putLong(this.id);
			exactLength += Long.BYTES;
			
			byte[] valueBytes = this.value != null ? 
					this.value.getBytes(CHARSET) : new byte[0];
			bbuf.putInt(valueBytes.length);
			bbuf.put(valueBytes);
			exactLength += (Integer.BYTES+valueBytes.length);
			
			bbuf.put(this.coord ? (byte) 1 : (byte) 0);
			bbuf.put(this.stop ? (byte) 1 : (byte) 0);
			bbuf.putInt(this.epoch);
			exactLength += (Integer.BYTES + 2);
			
			byte[] responseBytes = this.response != null? 
					this.response.getBytes(CHARSET) : new byte[0];
			bbuf.putInt(responseBytes != null ? responseBytes.length : 0);
			bbuf.put(responseBytes);
			exactLength += (Integer.BYTES + responseBytes.length);
			
			byte[] exactBytes = new byte[exactLength];
			bbuf.flip();
			
			bbuf.get(exactBytes);
			
			return exactBytes;
			
		} catch (UnsupportedEncodingException e) {
			
		}
		
		return null;
	}
	
	/**
	 * @param bytes
	 * @throws UnsupportedEncodingException
	 * @throws UnknownHostException
	 */
	public HttpActiveReplicaRequest(byte[] bytes) throws UnsupportedEncodingException, 
	UnknownHostException{
		this(ByteBuffer.wrap(bytes));
	}

	/**
	 * @param bbuf
	 * @throws UnsupportedEncodingException
	 * @throws UnknownHostException
	 */
	public HttpActiveReplicaRequest(ByteBuffer bbuf) throws UnsupportedEncodingException,
	UnknownHostException {
		// the only request type used for HttpActiveReplicaRequest
		super(HttpActiveReplicaPacketType.EXECUTE);
		this.type = bbuf.getInt();
		
		int nameLen = bbuf.getInt();
		byte[] nameBytes = new byte[nameLen];
		bbuf.get(nameBytes);
		this.name = nameBytes.length > 0 ? new String(nameBytes,CHARSET) 
				: null;
		
		this.id = bbuf.getLong();
		
		int valLen = bbuf.getInt();
		byte[] valBytes = new byte[valLen];
		bbuf.get(valBytes);
		this.value = valBytes.length > 0 ? new String(valBytes,
				CHARSET) : null;
		
		this.coord = bbuf.get() == (byte) 1;
		this.stop = bbuf.get() == (byte) 1;
		
		this.epoch = bbuf.getInt();
		
		int respLen = bbuf.getInt();
		byte[] respBytes = new byte[respLen];
		bbuf.get(respBytes);
		this.response = respBytes.length > 0 ? new String(respBytes,
				CHARSET) : null;
	}
	
	@Override
	public boolean equals(Object obj) {
		HttpActiveReplicaRequest request = null;
		
		return (obj instanceof HttpActiveReplicaRequest)
				&& ((request = (HttpActiveReplicaRequest) obj) != null)
				&& (this.name == request.name)
				&& (this.id == request.id)
				&& (this.value.equals(request.value));
	}
	
	/**
	 * Test request serialization.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		
		String serviceName = "test";
		String value = "OK";
		
		HttpActiveReplicaRequest req1 = new HttpActiveReplicaRequest(HttpActiveReplicaPacketType.EXECUTE,
				serviceName, value);
		HttpActiveReplicaRequest req2 = new HttpActiveReplicaRequest(value, req1);
		HttpActiveReplicaRequest req3 = new HttpActiveReplicaRequest(value, req1);
		
		// Reflexive
		assert(req1.equals(req1));
		
		// Symmetry: if(x.equals(y)==true)
		assert(req1.equals(req2) && req2.equals(req1));
		
		// Transitive: if x.equals(y) and y.equals(z); then x.equals(z)
		assert(req2.equals(req3));
		
		// Consistent: if x.equals(y)==true and no value is modified, then it's always true for every call.
		
		// For any non-null object x, x.equals(null)==false.
		assert(req1.equals(null) == false);
	}

}
