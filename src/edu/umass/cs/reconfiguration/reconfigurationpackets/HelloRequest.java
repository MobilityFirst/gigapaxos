package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.utils.Config;

/**
 * @author gaozy
 * 
 * @param <NodeIDType> 
 *
 */
public class HelloRequest<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> 
	implements ClientRequest {

	private static enum Keys {
		QID, ID
	};
	
	/**
	 * 
	 */
	public final long requestID;
	
	/**
	 * 
	 */
	public final NodeIDType myID;
	/**
	 * 
	 */
	
	/**
	 * @param initiator
	 */
	public HelloRequest(NodeIDType initiator) {
		super(initiator, 
				ReconfigurationPacket.PacketType.HELLO_REQUEST, 
				Config.getGlobalString(RC.BROADCAST_NAME), 0);
		// this.addr = addr;
		this.requestID = (long) (Math.random() * Long.MAX_VALUE);
		this.myID = initiator;
		
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 * @throws UnknownHostException
	 */
	@SuppressWarnings("unchecked")
	public HelloRequest(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException, UnknownHostException {
		super(json, unstringer);
		
		this.requestID = json.getLong(Keys.QID.toString());
		this.myID = (NodeIDType) json.get(Keys.ID.toString());
		/*
		this.myReceiver = MessageNIOTransport.getReceiverAddress(json);
		this.setSender(MessageNIOTransport.getSenderAddress(json));
		this.sentTime = json.getLong(Keys.SENT_TIME.toString());
		*/
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.QID.toString(), this.requestID);
		json.put(Keys.ID.toString(), this.myID);
		
		return json;
	}
	
	@Override
	public long getRequestID() {
		return this.requestID;
	}

	@Override
	public ClientRequest getResponse() {
		return this;
	}
	
	@Override
	public InetSocketAddress getClientAddress() {
		return null;
	}
}
