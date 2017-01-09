package edu.umass.cs.reconfiguration.testing;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.utils.Config;

/**
 * @author arun
 *
 *         A request packet for testing in a manner very similar to
 *         TESTPaxosClient and RequestPacket but using {@link TESTPaxosClient}.
 */
public class MetaRequestPacket extends RequestPacket {

	/**
	 * @param name
	 * @param val
	 */
	public MetaRequestPacket(String name, String val) {
		super((long) (Math.random() * Long.MAX_VALUE), val, false);
		this.putPaxosID(name, 0);
		this.type = AppRequest.PacketType.META_REQUEST_PACKET.getInt();
	}

	/**
	 * @param id
	 * @param val
	 * @param stop
	 */
	public MetaRequestPacket(long id, String val, boolean stop) {
		super(id, val, stop);
		this.type = AppRequest.PacketType.META_REQUEST_PACKET.getInt();
	}
	
	/**
	 * @param bytes
	 * @throws UnsupportedEncodingException
	 * @throws UnknownHostException
	 */
	public MetaRequestPacket(byte[] bytes) throws UnsupportedEncodingException, UnknownHostException {
		super(bytes);
		this.type = AppRequest.PacketType.META_REQUEST_PACKET.getInt();		
	}

	private static final boolean BYTEIFICATION = Config
			.getGlobalBoolean(PC.BYTEIFICATION);

	/**
	 * @param bytes
	 * @return MetaRequestPacket from bytes.
	 * @throws UnsupportedEncodingException
	 * @throws UnknownHostException
	 * @throws JSONException
	 */
	public static MetaRequestPacket getMetaRequestPacket(byte[] bytes)
			throws UnsupportedEncodingException, UnknownHostException,
			JSONException {
		MetaRequestPacket meta = null;
		if (BYTEIFICATION) {
			meta = new MetaRequestPacket(bytes);
		} else {
			meta = new MetaRequestPacket(new JSONObject(new String(bytes,
					NIOHeader.CHARSET)));
		}
		meta.type = AppRequest.PacketType.META_REQUEST_PACKET.getInt();
		return meta;
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public MetaRequestPacket(JSONObject json) throws JSONException {
		super(json);
		this.type = AppRequest.PacketType.META_REQUEST_PACKET.getInt();
	}

	/**
	 * @param req
	 */
	public MetaRequestPacket(RequestPacket req) {
		super(req);
		this.type = AppRequest.PacketType.META_REQUEST_PACKET.getInt();
	}

	public IntegerPacketType getRequestType() {
		return AppRequest.PacketType.META_REQUEST_PACKET;
	}

	private static final boolean USE_REQUEST_PACKET = false;
	public byte[] toBytes() {
		if (BYTEIFICATION)
			return ByteBuffer
					.wrap(super.toBytes())
					.putInt(USE_REQUEST_PACKET ? PaxosPacket.PaxosPacketType.PAXOS_PACKET.getInt() : 
						(AppRequest.PacketType.META_REQUEST_PACKET
							.getInt())).array();
		try {
			JSONObject json = super.toJSONObject();
			if(USE_REQUEST_PACKET) JSONPacket.putPacketType(json,
					AppRequest.PacketType.META_REQUEST_PACKET.getInt());
			return json.toString().getBytes(NIOHeader.CHARSET);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		JSONPacket.putPacketType(json,
				AppRequest.PacketType.META_REQUEST_PACKET.getInt());
		return json;
	}

	public String toString() {
		try {
			return this.toJSONObject().toString();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @return False. FIXME:
	 */
	public boolean needsCoordination() {
		return false;
	}

	public MetaRequestPacket getResponse() {
		return new MetaRequestPacket((RequestPacket) super.getResponse());
	}
}
