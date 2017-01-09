package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class EchoRequest extends BasicReconfigurationPacket<InetSocketAddress>
		implements ClientRequest {

	private static enum Keys {
		IS_REQUEST, CLOSEST, QID, SENT_TIME
	};

	/**
	 * True if this is a request, false otherwise. Requests beget responses;
	 * responses don't.
	 */
	private boolean isRequest;
	/**
	 * 
	 */
	public final long requestID;

	/**
	 * 
	 */
	public final long sentTime;
	/**
	 * 
	 */
	public final InetSocketAddress myReceiver;

	private final Map<InetAddress, Long> closest;

	/**
	 * @param initiator
	 * @param closest
	 */
	public EchoRequest(InetSocketAddress initiator,
			Map<InetAddress, Long> closest) {
		super(initiator, ReconfigurationPacket.PacketType.ECHO_REQUEST, Config
				.getGlobalString(RC.BROADCAST_NAME), 0);
		this.isRequest = closest != null ? false : true;
		this.closest = closest;
		this.requestID = (long) (Math.random() * Long.MAX_VALUE);
		this.myReceiver = null;
		this.sentTime = System.currentTimeMillis();
	}

	/**
	 * @param initiator
	 */
	public EchoRequest(InetSocketAddress initiator) {
		this(initiator, null);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 * @throws UnknownHostException
	 */
	public EchoRequest(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException, UnknownHostException {
		super(json, ClientReconfigurationPacket.unstringer);
		this.isRequest = json.has(Keys.IS_REQUEST.toString()) ? json
				.getBoolean(Keys.IS_REQUEST.toString()) : false;
		this.closest = json.has(Keys.CLOSEST.toString()) ? getClosest(json
				.getJSONArray(Keys.CLOSEST.toString())) : null;
		this.requestID = json.getLong(Keys.QID.toString());
		this.myReceiver = MessageNIOTransport.getReceiverAddress(json);
		this.setSender(MessageNIOTransport.getSenderAddress(json));
		this.sentTime = json.getLong(Keys.SENT_TIME.toString());
	}

	/**
	 * @param json
	 * @throws JSONException
	 * @throws UnknownHostException
	 */
	public EchoRequest(JSONObject json) throws JSONException,
			UnknownHostException {
		this(json, ClientReconfigurationPacket.unstringer);
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.IS_REQUEST.toString(), this.isRequest);
		if (this.closest != null && !this.closest.isEmpty()) {
			JSONArray jarray = new JSONArray();
			int i = 0;
			for (InetAddress addr : this.closest.keySet()) {
				JSONArray element = new JSONArray();
				element.put(0, addr.getHostAddress());
				element.put(1, this.closest.get(addr));
				jarray.put(i++, element);
			}
			json.put(Keys.CLOSEST.toString(), jarray);
		}
		json.put(Keys.QID.toString(), this.requestID);
		json.put(Keys.SENT_TIME.toString(), this.sentTime);
		return json;
	}

	private Map<InetAddress, Long> getClosest(JSONArray jarray)
			throws UnknownHostException, JSONException {
		if (jarray.length() > 0) {
			Map<InetAddress, Long> nearest = new LinkedHashMap<InetAddress, Long>();
			for (int i = 0; i < jarray.length(); i++) {
				nearest.put(Util.getInetAddressFromString(jarray
						.getJSONArray(i).getString(0)), jarray.getJSONArray(i)
						.getLong(1));
				// nearest.add(Util.getInetAddressFromString(jarray.getString(i)));
			}
			return nearest;
		}
		return null;
	}

	/**
	 * @return True if this is a request from an external client to an active
	 *         replica.
	 */
	public boolean isRequest() {
		return this.isRequest;
	}

	@Override
	public long getRequestID() {
		return this.requestID;
	}

	@Override
	public InetSocketAddress getClientAddress() {
		return null;
	}

	@Override
	public ClientRequest getResponse() {
		this.isRequest = false;
		return this;
	}

	/**
	 * @return True if it has nonempty closest map.
	 */
	public boolean hasClosest() {
		return this.closest != null && !this.closest.isEmpty();
	}

	/**
	 * @param address
	 * @return {@code this}.
	 */
	public EchoRequest makeResponse(InetSocketAddress address) {
		this.setSender(address);
		return (EchoRequest) this.getResponse();
	}

	public String getSummary() {
		return EchoRequest.this.serviceName + ":" + EchoRequest.this.requestID
				+ ":" + EchoRequest.this.getSender();
	}

	/**
	 * @return closest map.
	 */
	public Map<InetAddress, Long> getClosest() {
		return this.closest;
	}
}