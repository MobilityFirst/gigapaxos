package edu.umass.cs.gigapaxos.paxospackets;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxospackets.BatchedCommit.Fields;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.GetType;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class BatchedAcceptReply extends AcceptReplyPacket implements Byteable {

	/**
	 */
	public static final int MAX_BATCH_SIZE = 2048;

	private final TreeSet<Integer> slots = new TreeSet<Integer>();

	// exists only to check field sequence, not automate serialization
	static enum Fields implements GetType {
		slots(TreeSet.class);
		final Class<?> type;

		Fields(Class<?> type) {
			this.type = type;
		}

		public Class<?> getType() {
			return this.type;
		}
	};

	// to prevent unintentional field sequence modifications
	static {
		checkFields(BatchedAcceptReply.class, Fields.values());
	}

	/**
	 * @param ar
	 */
	public BatchedAcceptReply(AcceptReplyPacket ar) {
		super(ar.acceptor, ar.ballot, ar.slotNumber, ar.maxCheckpointedSlot);
		this.packetType = PaxosPacket.PaxosPacketType.BATCHED_ACCEPT_REPLY;
		this.putPaxosID(ar.getPaxosID(), ar.getVersion());
		this.slots.add(ar.slotNumber);
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public BatchedAcceptReply(JSONObject json) throws JSONException {
		super(json);
		this.packetType = PaxosPacket.getPaxosPacketType(json);

		JSONArray jsonArray = json.getJSONArray(PaxosPacket.Keys.SLOTS
				.toString());
		for (int i = 0; i < jsonArray.length(); i++)
			this.slots.add(jsonArray.getInt(i));
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(PaxosPacket.Keys.SLOTS.toString(), this.slots);
		return json;
	}

	@Override
	public net.minidev.json.JSONObject toJSONSmartImpl() throws JSONException {
		net.minidev.json.JSONObject json = super.toJSONSmartImpl();
		json.put(PaxosPacket.Keys.SLOTS.toString(), this.slots);
		return json;
	}

	/**
	 * @param bbuf
	 * @throws UnsupportedEncodingException
	 * @throws UnknownHostException
	 */
	public BatchedAcceptReply(ByteBuffer bbuf)
			throws UnsupportedEncodingException, UnknownHostException {
		super(bbuf);
		assert (bbuf.position() == SIZEOF_PAXOSPACKET_FIXED
				+ this.getPaxosID().getBytes(CHARSET).length
				+ SIZEOF_ACCEPTREPLY) : bbuf.position() + " != " + "["
				+ SIZEOF_PAXOSPACKET_FIXED + " + "
				+ this.getPaxosID().getBytes(CHARSET).length + " + "
				+ SIZEOF_ACCEPTREPLY + "]";

		int numSlots = bbuf.getInt();
		for (int i = 0; i < numSlots; i++)
			this.slots.add(bbuf.getInt());
		assert (!bbuf.hasRemaining()); // perfect alignment
	}

	@Override
	public byte[] toBytes() {
		long t = System.nanoTime();
		if (!(PaxosPacket.BYTEIFICATION && IntegerMap.allInt()))
			try {
				return this.toString().getBytes(CHARSET);
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
				return null;
			}
		ByteBuffer bbuf;
		int paxosIDLength;
		try {
			paxosIDLength = this.getPaxosID().getBytes(CHARSET).length;
			bbuf = ByteBuffer.wrap(new byte[
			// PaxosPacket
					SIZEOF_PAXOSPACKET_FIXED + paxosIDLength
							// self
							+ SIZEOF_BATCHEDACCEPTREPLY + 4
							* (this.slots.size() + 1)]);

			super.toBytes(bbuf); // AcceptReplyPacket and above
			assert (bbuf.position() == SIZEOF_PAXOSPACKET_FIXED + paxosIDLength
					+ SIZEOF_BATCHEDACCEPTREPLY);

			// self
			bbuf.putInt(this.slots.size());
			for (Integer i : slots)
				bbuf.putInt(i);
			assert (!bbuf.hasRemaining()) : bbuf.remaining();
			// all done with byteification

			// asserts that slots size is correctly set
			assert (((ByteBuffer) bbuf.position(SIZEOF_PAXOSPACKET_FIXED
					+ paxosIDLength + SIZEOF_BATCHEDACCEPTREPLY)).getInt() == this.slots
					.size()) : ByteBuffer.wrap(
					bbuf.array(),
					SIZEOF_PAXOSPACKET_FIXED + paxosIDLength
							+ SIZEOF_BATCHEDACCEPTREPLY, 4).getInt()
					+ " != "
					+ this.getClass().getSimpleName()
					+ ".slots.size()=" + this.slots.size();

			if (Util.oneIn(Integer.MAX_VALUE))
				DelayProfiler
						.updateDelayNano("batchedAcceptReplyByteification", t,
								this.slots.size());
			return bbuf.array();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param ar
	 * @return True if no entry already present for the slot.
	 */
	public boolean addAcceptReply(AcceptReplyPacket ar) {
		if (!ar.ballot.equals(this.ballot) || !this.paxosID.equals(ar.paxosID))
			throw new RuntimeException("Unable to combine " + ar.getSummary()
					+ " with " + this.getSummary());
		return this.slots.add(ar.slotNumber);
	}

	/**
	 * @return Accepted slots.
	 */
	public Integer[] getAcceptedSlots() {
		return this.slots.toArray(new Integer[0]);
	}

	/**
	 * @return Size.
	 */
	public int size() {
		return this.slots.size();
	}

	public static void main(String[] args) {
	}
}
