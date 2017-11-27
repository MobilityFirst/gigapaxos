package edu.umass.cs.reconfiguration.examples.linwrites;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;

/**
 * @author arun
 *
 *         A class like this is needed only if the app wants to use request
 *         types other than {@link RequestPacket}, which is generally useful
 *         only if the app wants to coordinate only some request types using
 *         consensus but process other requests locally at replicas or
 *         coordinate them using custom replica coordination protocols. For
 *         using just gigapaxos to implement linearizability, i.e.,
 *         coordinating all requests via consensus-based RSM, this class is
 *         unnecessary as applications can simply encapsulate requests as
 *         {@link RequestPacket}.
 */
public class SimpleAppRequest extends JSONPacket implements
		ReplicableRequest, ClientRequest {

	/**
	 * Packet type class for example application requests.
	 */
	public enum PacketType implements IntegerPacketType {
		/**
		 * Default coordinated app request.
		 */
		COORDINATED_WRITE(401),
		/**
		 * Uncoordinated app request.
		 */
		LOCAL_READ(402), ;

		/******************************** BEGIN static ***********************/
		private static HashMap<Integer, PacketType> numbers = new HashMap<Integer, PacketType>();

		static {
			for (PacketType type : PacketType.values()) {
				if (!PacketType.numbers.containsKey(type.number)) {
					PacketType.numbers.put(type.number, type);
				} else {
					String error = "Duplicate or inconsistent enum type";
					assert (false) : error;
					throw new RuntimeException(error);
				}
			}
		}

		/**
		 * @param type
		 * @return PacketType from int type.
		 */
		public static PacketType getPacketType(int type) {
			return PacketType.numbers.get(type);
		}

		/********************************** END static ***********************/

		private final int number;

		PacketType(int t) {
			this.number = t;
		}

		@Override
		public int getInt() {
			return this.number;
		}
	}

	/**
	 *
	 */
	public enum Keys {
		SERVICE_NAME, EPOCH, REQUEST_ID, REQUEST_VALUE, STOP, ACK, RESPONSE_VALUE
	};

	// name of the replicated state machine
	private final String name;

	// epoch number (nonzero if reconfigured)
	private final int epoch;

	// request identifier
	private final long requestID;

	// request value
	private final String value;

	// Whether this request should stop the RSM, which can
	// be used to delete the RSM entirely.
	private final boolean stop;

	// used when getResponse is invoked
	private String response = null;


	/**
	 * @param name Name of RSM
	 * @param epoch Number of time RSM has been reconfigured
	 * @param id Request identifier
	 * @param value Request value
	 * @param type Request type
	 * @param stop Whether the RSM should be stopped entirely
	 */
	public SimpleAppRequest(String name, int epoch, long id, String value,
							IntegerPacketType type, boolean stop) {
		super(type);
		this.name = name;
		this.epoch = epoch;
		this.requestID = id;
		this.stop = stop;
		this.value = value;
	}

	/**
	 * @param name
	 * @param value
	 * @param type
	 */
	public SimpleAppRequest(String name, String value, IntegerPacketType type) {
		this(name,value, type,false);
	}

	/**
	 * @param name
	 * @param value
	 * @param type
	 * @param stop
	 */
	public SimpleAppRequest(String name, String value, IntegerPacketType type,
							boolean stop) {
		this(name, 0, (long) (Math.random() * Long.MAX_VALUE), value, type,
				stop);
	}

	/**
	 * @param name
	 * @param id
	 * @param value
	 * @param type
	 * @param stop
	 */
	public SimpleAppRequest(String name, long id, String value,
							IntegerPacketType type, boolean stop) {
		this(name, 0, id, value, type, stop);
	}


	/**
	 * @param json
	 * @throws JSONException
	 */
	public SimpleAppRequest(JSONObject json) throws JSONException {
		super(json);
		this.name = json.getString(Keys.SERVICE_NAME.toString());
		this.epoch = json.getInt(Keys.EPOCH.toString());
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
		this.stop = json.getBoolean(Keys.STOP.toString());
		this.value = json.getString(Keys.REQUEST_VALUE.toString());
	}

	@Override
	public IntegerPacketType getRequestType() {
		return PacketType.getPacketType(this.type);
	}

	@Override
	public String getServiceName() {
		return this.name;
	}

	/**
	 * @return Request value.
	 */
	public String getValue() {
		return this.value;
	}

	/**
	 * @return Unique request ID.
	 */
	public long getRequestID() {
		return this.requestID;
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.SERVICE_NAME.toString(), this.name);
		json.put(Keys.EPOCH.toString(), this.epoch);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		json.put(Keys.STOP.toString(), this.stop);
		json.put(Keys.REQUEST_VALUE.toString(), this.value);
		return json;
	}

	@Override
	public boolean needsCoordination() {
		return this.getRequestType().equals
				(PacketType.LOCAL_READ) ? false : true;
	}

	@Override
	public ClientRequest getResponse() {
		return new SimpleAppRequest(this.name, this.epoch, this.requestID,
				this.response==null ? Keys.ACK.toString() : this.response, PacketType
				.getPacketType(type), this.stop);
	}

	/**
	 * @param response
	 */
	public SimpleAppRequest setResponse(String response) {
		this.response = response;
		return this;
	}

	/**
	 * Simple check to ensure that stringification and back yields the
	 * original request.
	 * @param args
	 */
	public static void main(String[] args) {
		SimpleAppRequest request = new SimpleAppRequest("name1", 0, 0,
				"request1", SimpleAppRequest.PacketType.COORDINATED_WRITE,
				false);
		System.out.println(request);
		try {
			SimpleAppRequest request2 = (new SimpleAppRequest(
					request.toJSONObject()));
			assert (request.toString().equals(request2.toString()));
			System.out.println("SUCCESS");
		} catch (JSONException je) {
			je.printStackTrace();
		}
	}

}
