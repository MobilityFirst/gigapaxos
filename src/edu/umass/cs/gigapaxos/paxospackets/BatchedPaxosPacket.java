package edu.umass.cs.gigapaxos.paxospackets;

import java.util.ArrayList;
import java.util.Collection;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.PaxosPacketDemultiplexer;
import edu.umass.cs.gigapaxos.paxosutil.PaxosPacketDemultiplexerFast;

/**
 * @author arun
 *
 *         A general-purpose batched paxos packet. This is useful to extract
 *         batching benefits even with many groups just by batching together
 *         paxos packets destined to the same set of destinations. We might as
 *         well try to extract blood out of stone.
 * 
 */
public class BatchedPaxosPacket extends PaxosPacket {

	ArrayList<PaxosPacket> packets = new ArrayList<PaxosPacket>();

	/**
	 * @param pkt
	 */
	public BatchedPaxosPacket(PaxosPacket pkt) {
		super((PaxosPacket) null);
		assert(pkt!=null);
		this.packetType = PaxosPacketType.BATCHED_PAXOS_PACKET;
		packets.add(pkt);
	}
	/**
	 * @param pkts
	 */
	public BatchedPaxosPacket(PaxosPacket[] pkts) {
		super((PaxosPacket) null);
		assert(pkts!=null);
		this.packetType = PaxosPacketType.BATCHED_PAXOS_PACKET;
		for(PaxosPacket pkt : pkts)
			packets.add(pkt);
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public BatchedPaxosPacket(JSONObject json) throws JSONException {
		super(json);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		JSONArray jarray = json.getJSONArray(PaxosPacket.Keys.PP.toString());
		for (int i = 0; i < jarray.length(); i++)
			this.packets.add(PaxosPacketDemultiplexer.toPaxosPacket(
					jarray.getJSONObject(i), null));
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public BatchedPaxosPacket(net.minidev.json.JSONObject json)
			throws JSONException {
		super(json);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		net.minidev.json.JSONArray jarray = (net.minidev.json.JSONArray) json
				.get(PaxosPacket.Keys.PP.toString());
		for (int i = 0; i < jarray.size(); i++) {
			assert(jarray.get(i)!=null) : jarray + " in " + json;
			this.packets.add(PaxosPacketDemultiplexerFast.toPaxosPacket(
					(net.minidev.json.JSONObject) jarray.get(i), null));
		}
	}

	@Override
	protected JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		JSONArray jarray = new JSONArray();
		for (PaxosPacket pp : this.packets)
			jarray.put(pp.toJSONObject());
		json.put(PaxosPacket.Keys.PP.toString(), jarray);
		return json;
	}

	@Override
	protected net.minidev.json.JSONObject toJSONSmartImpl()
			throws JSONException {
		net.minidev.json.JSONObject json = new net.minidev.json.JSONObject();
		net.minidev.json.JSONArray jarray = new net.minidev.json.JSONArray();
		for (PaxosPacket pp : this.packets) 
			jarray.add(pp.toJSONSmart());
		json.put(PaxosPacket.Keys.PP.toString(), jarray);
		
		assert (!this.packets.isEmpty());
		assert (((net.minidev.json.JSONArray) json.get(PaxosPacket.Keys.PP
				.toString())).get(0) != null);
		
		return json;
	}

	@Override
	protected String getSummaryString() {
		return "" + this.packets.size();
	}

	/**
	 * @return Unbatched paxos packets.
	 */
	public Collection<PaxosPacket> getPaxosPackets() {
		return this.packets;
	}

	/**
	 * @param pp
	 * @return {@code this}
	 */
	public BatchedPaxosPacket append(PaxosPacket pp) {
		this.packets.add(pp);
		return this;
	}
	/**
	 * @param pps
	 * @return {@code this}
	 */
	public BatchedPaxosPacket append(PaxosPacket[] pps) {
		for(PaxosPacket pp : pps)
			this.packets.add(pp);
		return this;
	}

	/**
	 * @return Number of batched packets.
	 */
	public int size() {
		return this.packets.size();
	}

}
