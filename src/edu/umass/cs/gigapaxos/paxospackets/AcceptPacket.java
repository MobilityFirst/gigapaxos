/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxospackets;

import java.security.MessageDigest;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public final class AcceptPacket extends PValuePacket {
	/**
	 * Sender node ID. FIXME: should just be the same as the ballot coordinator.
	 */
	public final int sender;
	
	public AcceptPacket(int nodeID, PValuePacket pValue, int medianCPSlot) {
		super(pValue);
		this.packetType = PaxosPacketType.ACCEPT;
		this.sender = nodeID;
		this.setMedianCheckpointedSlot(medianCPSlot);
	}

	public AcceptPacket(JSONObject json) throws JSONException {
		super(json);
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.ACCEPT); 
		this.packetType = PaxosPacketType.ACCEPT;
		this.sender = json.getInt(PaxosPacket.NodeIDKeys.SNDR.toString());
		// TODO: why do we need to set paxosID here?
		this.paxosID = json.getString(PaxosPacket.Keys.ID.toString());
	}
	public AcceptPacket(net.minidev.json.JSONObject json) throws JSONException {
		super(json);
		assert(json.containsKey(RequestPacket.Keys.STRINGIFIED.toString()));
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.ACCEPT); 
		this.packetType = PaxosPacketType.ACCEPT;
		this.sender = (Integer)json.get(PaxosPacket.NodeIDKeys.SNDR.toString());
		// TODO: why do we need to set paxosID here?
		this.paxosID = (String)json.get(PaxosPacket.Keys.ID.toString());
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(PaxosPacket.NodeIDKeys.SNDR.toString(), sender);
		return json;
	}
	@Override
	public net.minidev.json.JSONObject toJSONSmartImpl() throws JSONException {
		net.minidev.json.JSONObject json = super.toJSONSmartImpl();
		json.put(PaxosPacket.NodeIDKeys.SNDR.toString(), sender);
		return json;
	}
	

	@Override
	protected String getSummaryString() {
		return super.getSummaryString();
	}
	
	public AcceptPacket digest(MessageDigest md) {
		assert(md!=null);
		AcceptPacket accept = new AcceptPacket(this.sender, new PValuePacket(
				this.ballot, new ProposalPacket(this.slot, new RequestPacket(
						this.requestID, null, this.stop, this.getFirstOnly()))),
				this.getMedianCheckpointedSlot());
		assert(accept.digest!=null && !accept.hasRequestValue());
		return accept;
	}
	
	public AcceptPacket undigest(RequestPacket req) {
		assert(this.requestID==req.requestID);
		return (AcceptPacket) new AcceptPacket(this.sender,
				new PValuePacket(this.ballot,
						new ProposalPacket(this.slot, req)),
				this.getMedianCheckpointedSlot());
	}
}
