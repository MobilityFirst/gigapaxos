/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun */
package edu.umass.cs.gigapaxos.paxospackets;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.paxosutil.PaxosMessenger;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

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

	static enum Fields implements GetType {
		sender(int.class);
		final Class<?> type;

		Fields(Class<?> type) {
			this.type = type;
		}

		public Class<?> getType() {
			return this.type;
		}
	}

	static {
		checkFields(AcceptPacket.class, Fields.values());
	}

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
		assert (json.containsKey(RequestPacket.Keys.STRINGIFIED.toString()));
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.ACCEPT);
		this.packetType = PaxosPacketType.ACCEPT;
		this.sender = (Integer) json
				.get(PaxosPacket.NodeIDKeys.SNDR.toString());
		// TODO: why do we need to set paxosID here?
		this.paxosID = (String) json.get(PaxosPacket.Keys.ID.toString());
	}

	public AcceptPacket(ByteBuffer bbuf) throws UnsupportedEncodingException,
			UnknownHostException {
		super(bbuf);
		this.sender = bbuf.getInt();
	}

	protected static final int SIZEOF_ACCEPT = 4;

	@Override
	public synchronized byte[] toBytes() {
		long t = System.nanoTime();
		if (!(PaxosPacket.BYTEIFICATION && IntegerMap.allInt()))
			return super.toBytes();

		if (this.getByteifiedSelf() != null)
			return this.getByteifiedSelf();

		// else construct
		byte[] buf = super.toBytes(false);
		byte[] bytes = new byte[buf.length
		// ProposalPacket.slot
				+ SIZEOF_PROPOSAL
				// PValuePacket:ballot, recovery, medianCheckpointedSlot,
				// noCoalesce
				+ SIZEOF_PVALUE
				// AcceptPacket.sender
				+ SIZEOF_ACCEPT];
		ByteBuffer bbuf = ByteBuffer.wrap(bytes);

		// request
		bbuf.put(buf);
		// proposal
		bbuf.putInt(this.slot)
				// pvalue
				.putInt(this.ballot.ballotNumber)
				.putInt(this.ballot.coordinatorID)
				.put(this.isRecovery() ? (byte) 1 : 0)
				.putInt(this.getMedianCheckpointedSlot()).put((byte) 0)
				// accept
				.putInt(this.sender);

		assert (bbuf.remaining() == 0); // exact alignment

		this.setByteifiedSelf(bytes);

		if (PaxosMessenger.INSTRUMENT_SERIALIZATION && Util.oneIn(100))
			DelayProfiler.updateDelayNano("accept->", t,
					this.batchSize() + 1);

		return bytes;
	}

	public AcceptPacket(byte[] bytes) throws UnsupportedEncodingException,
			UnknownHostException {
		this(ByteBuffer.wrap(bytes));
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
		assert (md != null);
		AcceptPacket accept = new AcceptPacket(this.sender,
				new PValuePacket(this.ballot, new ProposalPacket(this.slot,
						new RequestPacket(this.requestID, null, this.stop, this
								.getFirstOnly()))),
				this.getMedianCheckpointedSlot());
		assert (accept.digest != null && !accept.hasRequestValue());
		return accept;
	}

	public AcceptPacket undigest(RequestPacket req) {
		assert (this.requestID == req.requestID);
		return (AcceptPacket) new AcceptPacket(this.sender, new PValuePacket(
				this.ballot, new ProposalPacket(this.slot, req)),
				this.getMedianCheckpointedSlot());
	}
	
	public static void main(String[] args) {
	}
}
