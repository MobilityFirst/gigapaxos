package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
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
	
	public static void main(String[] args) {
		final int id = 15;
		HelloRequest<Integer> helloReq = new HelloRequest<Integer>(id);
		System.out.println(helloReq);		
		try {
			assert(helloReq.myID == id);
			HelloRequest<Integer> helloReq2 = new HelloRequest<Integer>(
					helloReq.toJSONObject(), new StringifiableDefault<Integer>(id));
			assert(helloReq2.toString().equals(helloReq.toString()));
			
			HelloRequest<Integer> helloReq3 = new HelloRequest<Integer>(
					new JSONObject(helloReq.toString()), new StringifiableDefault<Integer>(id));
			assert(helloReq3.toString().equals(helloReq.toString()));
		} catch (UnknownHostException | JSONException e) {
			e.printStackTrace();
		}
	}
}
