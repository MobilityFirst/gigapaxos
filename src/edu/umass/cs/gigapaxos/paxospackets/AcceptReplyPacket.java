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

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;

/**
 * @author arun
 *
 */
public class AcceptReplyPacket extends PaxosPacket {
	/**
	 * Sender node ID.
	 */
	public final int acceptor;
	/**
	 * Ballot in the ACCEPT request being replied to.
	 */
	public final Ballot ballot;
	/**
	 * Slot number in the ACCEPT request being replied to.
	 */
	public final int slotNumber;
	/**
	 * Maximum slot up to which this node has checkpointed state, a value that
	 * is used for garbage collection of logs.
	 */
	public final int maxCheckpointedSlot;

	protected final long requestID; // used only for debugging

	// true => not an ack but a request for the undigested accept
	private boolean undigestRequest = false;

	/**
	 * @param nodeID
	 * @param ballot
	 * @param slotNumber
	 * @param maxCheckpointedSlot
	 * @param requestID
	 * @param ar
	 */
	public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber,
			int maxCheckpointedSlot, long requestID, AcceptReplyPacket ar) {
		super(ar);
		this.packetType = PaxosPacketType.ACCEPT_REPLY;
		this.acceptor = nodeID;
		this.ballot = ballot;
		this.slotNumber = slotNumber;
		this.maxCheckpointedSlot = maxCheckpointedSlot;
		// debugging only
		this.requestID = ar != null ? (ar instanceof BatchedAcceptReply ? ((BatchedAcceptReply) ar)
				.getRequestID(slotNumber) : ar.requestID)
				: requestID; 
	}

	/**
	 * @param nodeID
	 * @param ballot
	 * @param slotNumber
	 * @param maxCheckpointedSlot
	 */
	public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber,
			int maxCheckpointedSlot) {
		this(nodeID, ballot, slotNumber, maxCheckpointedSlot, 0, null);
	}

	// used only for instrumentation
	/**
	 * @param nodeID
	 * @param ballot
	 * @param slotNumber
	 * @param maxCheckpointedSlot
	 * @param requestID
	 */
	public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber,
			int maxCheckpointedSlot, long requestID) {
		this(nodeID, ballot, slotNumber, maxCheckpointedSlot, requestID, null);
	}

	/**
	 * @param jsonObject
	 * @throws JSONException
	 */
	public AcceptReplyPacket(JSONObject jsonObject) throws JSONException {
		super(jsonObject);
		this.packetType = PaxosPacketType.ACCEPT_REPLY;
		this.acceptor = jsonObject.getInt(PaxosPacket.NodeIDKeys.SNDR
				.toString());
		this.ballot = new Ballot(jsonObject.getString(PaxosPacket.NodeIDKeys.B
				.toString()));
		this.slotNumber = jsonObject.getInt(PaxosPacket.Keys.S.toString());
		this.maxCheckpointedSlot = jsonObject.getInt(PaxosPacket.Keys.CP_S
				.toString());
		this.requestID = jsonObject.getInt(RequestPacket.Keys.QID.toString());
		if (jsonObject.has(PaxosPacket.Keys.NACK.toString()))
			this.undigestRequest = jsonObject.getBoolean(PaxosPacket.Keys.NACK
					.toString());
	}

	protected static final int SIZEOF_ACCEPTREPLY = 4 // acceptor
			+ 8 // int,int ballot
			+ 4 // int slotNumber
			+ 4 // int maxCheckpointedSlot
			+ 8 // long requestID
			+ 1 // boolean undigestRequest
	;

	static enum Fields implements GetType {
		acceptor(int.class), ballot(Ballot.class), slotNumber(int.class), maxCheckpointedSlot(
				int.class), requestID(long.class), undigestRequest(
				boolean.class);
		final Class<?> type;

		Fields(Class<?> type) {
			this.type = type;
		}

		public Class<?> getType() {
			return this.type;
		};
	}

	static {
		checkFields(AcceptReplyPacket.class, Fields.values());
	}

	/**
	 * @param bbuf
	 * @throws UnsupportedEncodingException
	 * @throws UnknownHostException
	 */
	public AcceptReplyPacket(ByteBuffer bbuf)
			throws UnsupportedEncodingException, UnknownHostException {
		super(bbuf);
		int paxosIDLength = this.getPaxosID().getBytes(CHARSET).length;
		assert (bbuf.position() == SIZEOF_PAXOSPACKET_FIXED + paxosIDLength);
		this.acceptor = bbuf.getInt();
		this.ballot = new Ballot(bbuf.getInt(), bbuf.getInt());
		this.slotNumber = bbuf.getInt();
		this.maxCheckpointedSlot = bbuf.getInt();
		this.requestID = bbuf.getLong();
		if (bbuf.get() == (byte) 1)
			this.setDigestRequest();
		assert (bbuf.position() == SIZEOF_PAXOSPACKET_FIXED + paxosIDLength
				+ SIZEOF_ACCEPTREPLY) : bbuf.position() + " != ["
				+ SIZEOF_PAXOSPACKET_FIXED + " + " + paxosIDLength + " + "
				+ SIZEOF_ACCEPTREPLY + "]";
	}

	protected static final int SIZEOF_BATCHEDACCEPTREPLY = 4 + 8 + 4 + 4 + 8
			+ 1;

	public ByteBuffer toBytes(ByteBuffer bbuf)
			throws UnsupportedEncodingException {
		super.toBytes(bbuf);
		assert (bbuf.position() == SIZEOF_PAXOSPACKET_FIXED
				+ this.getPaxosID().getBytes(CHARSET).length);
		bbuf.putInt(this.acceptor).putInt(this.ballot.ballotNumber)
				.putInt(this.ballot.coordinatorID).putInt(this.slotNumber)
				.putInt(this.maxCheckpointedSlot).putLong(this.requestID)
				.put(this.isUndigestRequest() ? (byte) 1 : (byte) 0);
		return bbuf;
	}

	/**
	 * @return {@link #requestID} used only for debugging.
	 */
	@SuppressWarnings("javadoc")
	public long getRequestID() {
		return this.requestID;
	}

	@Override
	protected String getSummaryString() {
		return acceptor + "->" + ballot + ", " + slotNumber + "("
				+ maxCheckpointedSlot + ")";
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.SNDR.toString(), acceptor);
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.S.toString(), slotNumber);
		json.put(PaxosPacket.Keys.CP_S.toString(), this.maxCheckpointedSlot);
		json.put(RequestPacket.Keys.QID.toString(), this.requestID);
		if (this.undigestRequest)
			json.put(PaxosPacket.Keys.NACK.toString(), this.undigestRequest);
		return json;
	}

	@Override
	public net.minidev.json.JSONObject toJSONSmartImpl() throws JSONException {
		net.minidev.json.JSONObject json = new net.minidev.json.JSONObject();
		json.put(PaxosPacket.NodeIDKeys.SNDR.toString(), acceptor);
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.S.toString(), slotNumber);
		json.put(PaxosPacket.Keys.CP_S.toString(), this.maxCheckpointedSlot);
		json.put(RequestPacket.Keys.QID.toString(), this.requestID);
		if (this.undigestRequest)
			json.put(PaxosPacket.Keys.NACK.toString(), this.undigestRequest);
		return json;
	}

	/**
	 * @return Whether this packet is coalescable, which is true when its
	 *         undigestRequest flag is false.
	 */
	public boolean isCoalescable() {
		return !this.undigestRequest;
	}

	/**
	 * @return {@link #undigestRequest}
	 */
	@SuppressWarnings("javadoc")
	public boolean isUndigestRequest() {
		return this.undigestRequest;
	}

	/**
	 * @return Sets {@link #undigestRequest}.
	 */
	@SuppressWarnings("javadoc")
	public AcceptReplyPacket setDigestRequest() {
		this.undigestRequest = true;
		return this;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}
}
