package edu.umass.cs.gigapaxos.paxospackets;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.paxosutil.PaxosMessenger;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class BatchedCommit extends PaxosPacket implements Byteable {

	/**
	 * Note: The order of the fields can not be changed. No new fields can be
	 * added or existing fields removed unless the bytebuffer based constructor
	 * and toBytes() methods are accordingly modified. Use the JSON mode for
	 * adding temporary debugging fields if needed.
	 */
	public final Ballot ballot;
	private int medianCheckpointedSlot;
	private final TreeSet<Integer> slots = new TreeSet<Integer>();
	private final Set<Integer> group;

	// exists only to check field sequence, not automate serialization
	static enum Fields implements GetType {
		ballot(Ballot.class), medianCheckpointedSlot(int.class), slots(
				TreeSet.class), group(Set.class);
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
		checkFields(BatchedCommit.class, Fields.values());
	}

	/**
	 * @param pvalue
	 * @param group
	 */
	public BatchedCommit(PValuePacket pvalue, Set<Integer> group) {
		super(pvalue);
		this.slots.add(pvalue.slot);
		this.ballot = pvalue.ballot;
		this.packetType = PaxosPacket.PaxosPacketType.BATCHED_COMMIT;
		this.group = group;
		this.medianCheckpointedSlot = pvalue.getMedianCheckpointedSlot();
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public BatchedCommit(JSONObject json) throws JSONException {
		super(json);
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.B
				.toString()));
		this.medianCheckpointedSlot = json.getInt(PaxosPacket.Keys.GC_S
				.toString());
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.group = new HashSet<Integer>();
		JSONArray jsonArray = json.getJSONArray(PaxosPacket.NodeIDKeys.GROUP
				.toString());
		for (int i = 0; i < jsonArray.length(); i++)
			this.group.add(jsonArray.getInt(i));

		jsonArray = json.getJSONArray(PaxosPacket.Keys.SLOTS.toString());
		for (int i = 0; i < jsonArray.length(); i++)
			this.slots.add(jsonArray.getInt(i));
	}

	/**
	 * @param bCommit
	 * @return True if no entry already present for the slot.
	 */
	public boolean addBatchedCommit(BatchedCommit bCommit) {
		if (!bCommit.ballot.equals(this.ballot)
				|| !bCommit.paxosID.equals(this.paxosID))
			throw new RuntimeException("Unable to combine "
					+ bCommit.getSummary() + " with " + this.getSummary());
		if (bCommit.getMedianCheckpointedSlot() - this.medianCheckpointedSlot > 0)
			this.medianCheckpointedSlot = bCommit.getMedianCheckpointedSlot();
		return this.slots.addAll(bCommit.slots);
	}

	/**
	 * @param commit
	 * @return True if no entry already present for the slot.
	 */
	public boolean addCommit(PValuePacket commit) {
		if (!commit.ballot.equals(this.ballot)
				|| !commit.paxosID.equals(this.paxosID))
			throw new RuntimeException("Unable to combine "
					+ commit.getSummary() + " with " + this.getSummary());
		if (commit.getMedianCheckpointedSlot() - this.medianCheckpointedSlot > 0)
			this.medianCheckpointedSlot = commit.getMedianCheckpointedSlot();
		return this.slots.add(commit.slot);
	}

	/**
	 * @return Committed slots.
	 */
	public Integer[] getCommittedSlots() {
		return this.slots.toArray(new Integer[0]);
	}

	@Override
	protected JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.GC_S.toString(), this.medianCheckpointedSlot);
		json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), this.group);
		json.put(PaxosPacket.Keys.SLOTS.toString(), this.slots);
		// values must be sorted in slot order
		return json;
	}

	@Override
	protected net.minidev.json.JSONObject toJSONSmartImpl()
			throws JSONException {
		net.minidev.json.JSONObject json = new net.minidev.json.JSONObject();
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.GC_S.toString(), this.medianCheckpointedSlot);
		json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), this.group);
		json.put(PaxosPacket.Keys.SLOTS.toString(), this.slots);
		// values must be sorted in slot order
		return json;
	}

	/**
	 * @param bbuf
	 * @throws UnsupportedEncodingException
	 * @throws UnknownHostException
	 */
	public BatchedCommit(ByteBuffer bbuf) throws UnsupportedEncodingException,
			UnknownHostException {
		super(bbuf);
		this.ballot = new Ballot(bbuf.getInt(), bbuf.getInt());
		this.medianCheckpointedSlot = bbuf.getInt();
		int numSlots = bbuf.getInt();
		for (int i = 0; i < numSlots; i++)
			this.slots.add(bbuf.getInt());
		this.group = new HashSet<Integer>();
		int groupSize = bbuf.getInt();
		for (int i = 0; i < groupSize; i++)
			this.group.add(bbuf.getInt());
	}

	/**
	 * Includes everything without {@link #slots}, {@link #group} and the two
	 * integers for their respective sizes. This is a bit inconsistent with
	 * {@link PaxosPacket#SIZEOF_PAXOSPACKET_FIXED} where the size integer is
	 * included in the fixed length. This is because we have two variable length
	 * sets in BatchedCommit, so it is more convenient to count both sizes as
	 * part of those variable length sets.
	 */
	protected static final int SIZEOF_BATCHEDCOMMIT_FIXED = 8 // int,int ballot
	+ 4 // int medianCheckpointedSlot
	;
	private byte[] byteifiedSelf = null;

	@Override
	public byte[] toBytes() {
		long t = System.nanoTime();
		if (this.byteifiedSelf != null)
			return this.byteifiedSelf;

		if (!(BYTEIFICATION && IntegerMap.allInt())) {
			try {
				return this.byteifiedSelf = this.toString().getBytes(CHARSET);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				return null;
			}
		}
		// else
		ByteBuffer bbuf;
		try {
			int paxosIDLength = this.getPaxosID().getBytes(CHARSET).length;
			bbuf = ByteBuffer.wrap(new byte[
			// PaxosPacket
					SIZEOF_PAXOSPACKET_FIXED + paxosIDLength
							// self
							+ SIZEOF_BATCHEDCOMMIT_FIXED + 4
							* (this.slots.size() + 1 + this.group.size() + 1)]);
			super.toBytes(bbuf);
			assert (bbuf.position() == SIZEOF_PAXOSPACKET_FIXED + paxosIDLength);
			bbuf.putInt(this.ballot.ballotNumber)
					.putInt(this.ballot.coordinatorID)
					.putInt(this.medianCheckpointedSlot)
					.putInt(this.slots.size());
			for (Integer i : slots)
				bbuf.putInt(i);
			bbuf.putInt(this.group.size());
			for (Integer i : this.group)
				bbuf.putInt(i);
			assert (!bbuf.hasRemaining()); // perfect alignment
			if (PaxosMessenger.INSTRUMENT_SERIALIZATION && Util.oneIn(100))
				DelayProfiler.updateDelayNano("commit->", t, this.slots.size());
			// all done with byteification here

			// ugly stuff below is for assertions only
			// assert that slots size is correctly set
			assert (((ByteBuffer) bbuf.position(SIZEOF_PAXOSPACKET_FIXED
					+ this.getPaxosID().getBytes(CHARSET).length
					+ SIZEOF_BATCHEDCOMMIT_FIXED)).getInt() == this.slots
					.size()) : ByteBuffer.wrap(
					bbuf.array(),
					SIZEOF_PAXOSPACKET_FIXED
							+ this.getPaxosID().getBytes(CHARSET).length
							+ SIZEOF_BATCHEDCOMMIT_FIXED, 4).getInt()
					+ " != "
					+ this.getClass().getSimpleName()
					+ ".slots.size()=" + this.slots.size();

			// assert that group size is correctly set
			assert (((ByteBuffer) bbuf.position(SIZEOF_PAXOSPACKET_FIXED
					+ this.getPaxosID().getBytes(CHARSET).length
					+ SIZEOF_BATCHEDCOMMIT_FIXED + 4 + this.slots.size() * 4))
					.getInt() == this.group.size()) : ByteBuffer.wrap(
					bbuf.array(),
					SIZEOF_PAXOSPACKET_FIXED
							+ this.getPaxosID().getBytes(CHARSET).length
							+ SIZEOF_BATCHEDCOMMIT_FIXED + 4
							+ this.slots.size() * 4, 4).getInt()
					+ " != "
					+ this.getClass().getSimpleName()
					+ ".group.size()=" + this.group.size();

			return bbuf.array();

		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @return Median checkpointed slot.
	 */
	public int getMedianCheckpointedSlot() {
		return this.medianCheckpointedSlot;
	}

	/**
	 * @param slot
	 */
	public void setMedianCheckpointedSlot(int slot) {
		this.medianCheckpointedSlot = slot;
	}

	@Override
	protected String getSummaryString() {
		return this.ballot + ":" + this.slots;
	}

	/**
	 * @return Group members as int[].
	 */
	public int[] getGroup() {
		return Util.setToIntArray(this.group);
	}

	/**
	 * @return Group members as a set.
	 */
	public Set<Integer> getGroupSet() {
		return this.group;
	}

	/**
	 * @return Size.
	 */
	public int size() {
		return this.slots.size();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}
}
